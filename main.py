"""Databricks SQL connector for RustGIS (session process plugin, ABI 1).

RustGIS starts this program once and keeps it running ("session": true). It talks over stdin/stdout, one JSON
object per line (RustGIS docs/plugins.md, "Session plugins"):

  in : {"session": {...}} first, then {"command": request, "id": n}, {"event": "view" | "layers" | ...},
       and replies to our host requests {"ok": value, "id": "c7"} / {"error": "text", "id": "c7"}
  out: {"ok": value, "id": n} answers, {"call": {"op", "params"}, "id": "c7"} host requests, {"progress": "…"}

Three things live here:
  * Tables / SQL: load Unity Catalog tables or any SELECT as layers (fixed parameters, a view or a drawn box).
  * Dynamic layers: a layer whose rows follow the map. After each pan/zoom (debounced) the view box goes into the
    query as ST_INTERSECTS(geom, box); results are rewritten to a local GeoJSON that RustGIS reloads in place,
    so styling is kept. Panning inside what's already loaded doesn't re-query.
  * Genie: a Genie space per selected table (created once, cached). Plain-English questions become SQL that
    Genie runs; the plugin maps the result (geometry, WKT, lon/lat or H3 cells), styles it, and shows Genie's
    answer, its SQL and follow-up ideas.

Sign-in goes through the Databricks SDK and a Databricks CLI profile; the plugin never stores a token.
"""
import datetime
import decimal
import itertools
import json
import math
import os
import queue
import re
import sys
import tempfile
import threading
import time
import uuid

MAX_ROWS = 200_000
DEFAULT_LIMIT = 10_000     # per view for dynamic layers: a screenful, fast to fetch
PAD = 0.25                 # dynamic layers fetch 25% beyond the view, so small pans don't re-query
DEBOUNCE_S = 0.25          # re-query this long after the map stops moving
CACHE_DIR = os.path.join(tempfile.gettempdir(), "rustgis-databricks")

# ---------------------------------------------------------------------------- session transport
_inbox = queue.Queue()
_pending = []              # commands / events that arrived while we waited for a host reply
_lock = threading.Lock()
_seq = [0]


_EOF = object()             # stdin closed: RustGIS is stopping us


def _reader():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if isinstance(msg, dict):
            _inbox.put(msg)
    _inbox.put(_EOF)


def send(obj):
    with _lock:
        sys.stdout.write(json.dumps(obj, allow_nan=False) + "\n")
        sys.stdout.flush()


def call(op, params=None, timeout=120):
    _seq[0] += 1
    cid = f"c{_seq[0]}"
    send({"call": {"op": op, "params": params or {}}, "id": cid})
    end = time.time() + timeout
    while True:
        left = end - time.time()
        if left <= 0:
            raise TimeoutError(f"RustGIS didn't answer {op} in {timeout} s")
        try:
            msg = _inbox.get(timeout=left)
        except queue.Empty:
            raise TimeoutError(f"RustGIS didn't answer {op} in {timeout} s")
        if msg is _EOF:
            raise SystemExit(0)
        if msg.get("id") == cid and ("ok" in msg or "error" in msg) and "command" not in msg:
            if "error" in msg:
                raise RuntimeError(msg["error"])
            return msg.get("ok")
        _pending.append(msg)


def progress(text):
    send({"progress": text})


_last_note = {"text": None, "at": 0.0}
_warned = set()


def warn_once(key, text):
    """An error note shown once per layer and error, not on every pan."""
    if key not in _warned:
        _warned.add(key)
        notify(text, error=True)


def notify(text, error=False):
    """A status-bar note (never a dialog). The same note within 30 s is dropped, so nothing nags."""
    if text == _last_note["text"] and time.time() - _last_note["at"] < 30:
        return
    _last_note.update(text=text, at=time.time())
    try:
        call("host.notify", {"text": text, "error": error})
    except Exception:
        pass


def settings():
    try:
        return call("host.settings") or {}
    except Exception:
        return {}


def save_settings(values):
    try:
        call("host.setSettings", {"values": values})
    except Exception:
        pass


# ---------------------------------------------------------------------------- Databricks
_cfg_cache = {}


def sdk_config(profile):
    from databricks.sdk.core import Config
    if profile not in _cfg_cache:
        _cfg_cache[profile] = Config(profile=profile)
    return _cfg_cache[profile]


CONN = {}                  # profile, warehouse_id: set on the main thread, read by the worker
_tls = threading.local()


def connect(profile, warehouse_id):
    """A new connection (Connect's test). Long-lived ones come from get_conn()."""
    from databricks import sql
    cfg = sdk_config(profile)
    host = cfg.host.replace("https://", "").rstrip("/")
    conn = sql.connect(server_hostname=host, http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                       credentials_provider=lambda: cfg.authenticate, _user_agent_entry="rustgis-databricks-plugin/0.4")
    return conn, host


def get_conn(reset=False):
    """This thread's warm connection to the current warehouse (opening a session costs ~1 s; reuse it)."""
    key = (CONN.get("profile"), CONN.get("warehouse_id"))
    if not all(key):
        raise RuntimeError("Not connected yet: in the Databricks panel, enter a CLI profile and a warehouse ID, then Connect.")
    c = getattr(_tls, "conn", None)
    if c is not None and getattr(_tls, "key", None) == key and not reset:
        return c, _tls.host
    if c is not None:
        try:
            c.close()
        except Exception:
            pass
        _tls.conn = None
    _tls.conn, _tls.host = connect(*key)
    _tls.key = key
    return _tls.conn, _tls.host


class _Pooled:
    """Looks like a connection, but close() keeps the warm session open."""
    def __init__(self, c):
        self._c = c

    def cursor(self):
        return self._c.cursor()

    def close(self):
        pass


def current():
    if not CONN.get("profile"):
        s = settings()
        if not s.get("profile") or not s.get("warehouse_id"):
            raise RuntimeError("Not connected yet: in the Databricks panel, enter a CLI profile and a warehouse ID, then Connect.")
        CONN.update(profile=s["profile"], warehouse_id=s["warehouse_id"])
    c, host = get_conn()
    return _Pooled(c), host


def workspace(s=None):
    from databricks.sdk import WorkspaceClient
    s = s or settings()
    return WorkspaceClient(config=sdk_config(s["profile"]))


def q(ident):
    return f"`{ident.replace('`', '``')}`"


def quote_table(name):
    parts = [p.strip("` ") for p in name.split(".")]
    if len(parts) != 3 or not all(parts):
        raise RuntimeError(f"Choose a table: a three-part name, catalog.schema.table (got '{name}').")
    return ".".join(q(p) for p in parts)


def is_geo(dtype):
    return str(dtype).upper().startswith(("GEOMETRY", "GEOGRAPHY"))


def is_num(dtype):
    return str(dtype).upper().split("(")[0] in ("INT", "BIGINT", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "DECIMAL", "LONG")


def map_extent():
    return (call("map.extent") or {}).get("extent")


def pad(b, f=PAD):
    w, s, e, n = b
    dx, dy = (e - w) * f, (n - s) * f
    return [max(w - dx, -180.0), max(s - dy, -89.9), min(e + dx, 180.0), min(n + dy, 89.9)]


def inside(inner, outer):
    return outer and inner[0] >= outer[0] and inner[1] >= outer[1] and inner[2] <= outer[2] and inner[3] <= outer[3]


def describe(cur, source_sql):
    cur.execute(f"DESCRIBE QUERY SELECT * FROM {source_sql} AS src")
    return [(r[0], str(r[1])) for r in cur.fetchall() if r[0] and not r[0].startswith("#")]


def geo_expr(cols, geom_col):
    """Pick the geometry column and the expression that gives it in WGS 84."""
    geos = [c for c, t in cols if is_geo(t)]
    if geom_col:
        if geom_col not in geos:
            raise RuntimeError(f"'{geom_col}' is not a GEOMETRY / GEOGRAPHY column here (found: {', '.join(geos) or 'none'}).")
    elif geos:
        geom_col = geos[0]
    else:
        raise RuntimeError("No GEOMETRY or GEOGRAPHY column. Build one in SQL, e.g. ST_POINT(lon, lat) AS geom.")
    dtype = dict(cols)[geom_col].upper()
    g = q(geom_col)
    m = re.search(r"\((\d+)\)", dtype)
    srid = int(m.group(1)) if m else 0
    if dtype.startswith("GEOGRAPHY") or srid == 4326:
        e = g
    elif srid == 0:
        e = f"ST_SETSRID({g}, 4326)"          # SRID 0: treat as lon/lat, so ST_INTERSECTS with a 4326 box is allowed
    else:
        e = f"ST_TRANSFORM({g}, 4326)"
    flt = f"ST_GEOMFROMWKB(ST_ASWKB({g}), 4326)" if dtype.startswith("GEOGRAPHY") else e
    return geom_col, e, flt


SCHEMA = {}                # (source, geom) -> cached plan: columns, geometry expressions, point fast path


def plan(cur, source_sql, geom_col):
    key = (source_sql, geom_col)
    if key in SCHEMA:
        return SCHEMA[key]
    cols = describe(cur, source_sql)
    gcol, e, flt = geo_expr(cols, geom_col)
    names = {c.lower(): c for c, t in cols if is_num(t)}
    lon = next((names[k] for k in ("lon", "lng", "longitude") if k in names), None)
    lat = next((names[k] for k in ("lat", "latitude") if k in names), None)
    fast = None
    if lon and lat:                       # point table with numeric lon/lat: plain range filters let Delta skip files
        try:
            cur.execute(f"SELECT ST_GEOMETRYTYPE({q(gcol)}) FROM {source_sql} AS src WHERE {q(gcol)} IS NOT NULL LIMIT 1")
            r = cur.fetchone()
            if r and str(r[0]).upper().endswith("POINT"):
                fast = (lon, lat)
        except Exception:
            return (cols, gcol, e, flt, None)    # don't cache a failed probe
    SCHEMA[key] = (cols, gcol, e, flt, fast)
    return SCHEMA[key]


def fetch(source_sql, geom_col, limit, bbox=None, wkb=False, holder=None, retry=True):
    """Run the query (optionally inside a lon/lat box) on this thread's warm connection.
    Returns (FeatureCollection, truncated, host, cols); with wkb=True features carry WKB in "wkb".
    holder (a dict) gets the running cursor, so another thread can cancel a stale query."""
    conn, host = get_conn()
    cur = conn.cursor()
    if holder is not None:
        holder["cursor"] = cur
    try:
        cols, gcol, e, flt, fast = plan(cur, source_sql, geom_col)
        where = ""
        if bbox:
            w, s_, ea, n = (float(v) for v in bbox)
            if fast:
                lo, la = (q(c) for c in fast)
                where = f" WHERE {lo} BETWEEN {w} AND {ea} AND {la} BETWEEN {s_} AND {n}"
            else:
                where = (f" WHERE ST_INTERSECTS({flt}, ST_GEOMFROMTEXT("
                         f"'POLYGON(({w} {s_}, {ea} {s_}, {ea} {n}, {w} {n}, {w} {s_}))', 4326))")
        others = ", ".join(q(c) for c, t in cols if c != gcol and not is_geo(t))
        sel = (others + ", " if others else "") + (f"ST_ASWKB({e}) AS __geojson" if wkb else f"ST_ASGEOJSON({e}) AS __geojson")
        cur.execute(f"SELECT {sel} FROM {source_sql} AS src{where} LIMIT {int(limit)}")
        try:
            tbl = cur.fetchall_arrow()
            names = tbl.column_names
            data = tbl.to_pydict()
            rows = list(zip(*[data[n] for n in names])) if names else []
        except Exception:
            rows = cur.fetchall()
            names = [d[0] for d in cur.description]
    except Exception as ex:
        msg = str(ex).lower()
        if retry and any(k in msg for k in ("session is closed", "invalid session", "session not found", "connection",
                                             "token expired", "expired token", "unauthenticated", "401")):
            get_conn(reset=True)           # stale session or token: reconnect once
            return fetch(source_sql, geom_col, limit, bbox, wkb, holder, retry=False)
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
    gi = names.index("__geojson")
    feats = []
    for r in rows:
        if r[gi] is None:
            continue
        props = {}
        for n, val in zip(names, r):
            if n == "__geojson":
                continue
            if isinstance(val, decimal.Decimal):
                val = float(val)
            elif val is not None and not isinstance(val, (str, int, float, bool)):
                val = str(val)
            if isinstance(val, float) and not math.isfinite(val):
                val = None                     # NaN / Infinity aren't JSON
            props[n] = val
        if wkb:
            feats.append({"wkb": bytes(r[gi]), "properties": props})
        else:
            feats.append({"type": "Feature", "geometry": json.loads(r[gi]), "properties": props})
    return {"type": "FeatureCollection", "features": feats}, len(rows) >= int(limit), host, cols


def load(source_sql, geom_col, name, limit, bbox=None):
    t0 = time.time()
    progress("Querying Databricks…")
    fc, trunc, host, cols = fetch(source_sql, geom_col, limit, bbox)
    call("host.addLayer", {"name": name, "geojson": fc})
    return f"Added '{name}': {len(fc['features']):,} features in {time.time() - t0:.1f} s" + (" (row limit reached)." if trunc else "."), cols


# ---------------------------------------------------------------------------- dynamic layers
LIVE = {}                  # layer name -> {source, geom, limit, path, bbox, truncated}


WGS84_WKT = ('GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],'
             'UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]')


def _gpkg_type(v):
    return "INTEGER" if isinstance(v, bool) or isinstance(v, int) else "REAL" if isinstance(v, float) else "TEXT"


RESERVED = {"fid", "geom"}


def _write(path, fc, table="features", types_hint=None):
    """Write (or rewrite in place) a one-layer GeoPackage, so RustGIS reads it as a linked source it can reload.
    Stdlib sqlite3 only: GeoPackage geometry = 8-byte GP header (little-endian, no envelope, SRID 4326) + WKB."""
    import sqlite3
    import struct
    feats = fc["features"]
    for f in feats:                             # 'fid' and 'geom' are the GeoPackage's own columns
        for r in RESERVED & set(f["properties"]):
            f["properties"][f"{r}_"] = f["properties"].pop(r)
    keys = []
    for f in feats:
        for k in f["properties"]:
            if k not in keys:
                keys.append(k)
    hint = types_hint or {}
    types = {k: hint.get(k) or next((_gpkg_type(f["properties"][k]) for f in feats if f["properties"].get(k) is not None), "TEXT")
             for k in keys}
    new = not os.path.exists(path)
    con = sqlite3.connect(path, timeout=10)
    try:
        cur = con.cursor()
        if new:
            cur.execute("PRAGMA application_id = 1196444487")      # 'GPKG'
            cur.execute("PRAGMA user_version = 10300")
            cur.executescript("""
              CREATE TABLE gpkg_spatial_ref_sys (srs_name TEXT NOT NULL, srs_id INTEGER PRIMARY KEY, organization TEXT NOT NULL,
                organization_coordsys_id INTEGER NOT NULL, definition TEXT NOT NULL, description TEXT);
              CREATE TABLE gpkg_contents (table_name TEXT NOT NULL PRIMARY KEY, data_type TEXT NOT NULL, identifier TEXT UNIQUE,
                description TEXT DEFAULT '', last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE, srs_id INTEGER);
              CREATE TABLE gpkg_geometry_columns (table_name TEXT NOT NULL, column_name TEXT NOT NULL, geometry_type_name TEXT NOT NULL,
                srs_id INTEGER NOT NULL, z TINYINT NOT NULL, m TINYINT NOT NULL, PRIMARY KEY (table_name, column_name));""")
            cur.executemany("INSERT INTO gpkg_spatial_ref_sys VALUES (?,?,?,?,?,?)", [
                ("Undefined cartesian SRS", -1, "NONE", -1, "undefined", None),
                ("Undefined geographic SRS", 0, "NONE", 0, "undefined", None),
                ("WGS 84 geodetic", 4326, "EPSG", 4326, WGS84_WKT, "longitude/latitude coordinates in decimal degrees")])
            cols_sql = "".join(f", {q(k).replace('`', chr(34))} {types[k]}" for k in keys)
            cur.execute(f'CREATE TABLE "{table}" (fid INTEGER PRIMARY KEY AUTOINCREMENT, geom BLOB{cols_sql})')
            cur.execute("INSERT INTO gpkg_contents (table_name, data_type, identifier, srs_id) VALUES (?, 'features', ?, 4326)", (table, table))
            cur.execute("INSERT INTO gpkg_geometry_columns VALUES (?, 'geom', 'GEOMETRY', 4326, 0, 0)", (table,))
        else:
            have = {r[1] for r in cur.execute(f'PRAGMA table_info("{table}")')}
            for k in keys:
                if k not in have:
                    cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{k}" {types[k]}')
        head = b"GP" + bytes([0, 0x01]) + struct.pack("<i", 4326)
        # sqlite3 opens the transaction itself; commit() below makes the rewrite atomic
        cur.execute(f'DELETE FROM "{table}"')
        names = ", ".join(['geom'] + [f'"{k}"' for k in keys])   # keys are already free of reserved names
        marks = ", ".join(["?"] * (len(keys) + 1))
        cur.executemany(f'INSERT INTO "{table}" ({names}) VALUES ({marks})',
                        [[head + f["wkb"]] + [f["properties"].get(k) for k in keys] for f in feats])
        cur.execute("UPDATE gpkg_contents SET last_change = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE table_name = ?", (table,))
        con.commit()
    finally:
        con.close()


class LiveWorker:
    """Runs dynamic-layer queries off the main thread. Latest view wins: a new job for a layer cancels the
    running stale one, and results older than the layer's newest job are dropped."""
    def __init__(self):
        self.jobs = {}                # layer -> newest pending job
        self.cv = threading.Condition()
        self.running = None           # (layer, seq, holder)
        self.results = queue.Queue()
        threading.Thread(target=self._loop, daemon=True).start()

    def submit(self, name, seq, source, geom, limit, box):
        stale = None
        with self.cv:
            self.jobs[name] = (seq, source, geom, limit, box)
            r = self.running
            if r and r[0] == name and r[1] < seq:
                stale = r[2].get("cursor")
            self.cv.notify()
        if stale is not None:
            try:
                stale.cancel()                  # stop the stale query (outside the lock); the new view is what matters
            except Exception:
                pass

    def _loop(self):
        while True:
            with self.cv:
                while not self.jobs:
                    self.cv.wait()
                name, (seq, source, geom, limit, box) = self.jobs.popitem()
                holder = {}
                self.running = (name, seq, holder)
            t0 = time.time()
            try:
                fc, trunc, _, _ = fetch(source, geom, limit, box, wkb=True, holder=holder)
                self.results.put((name, seq, box, fc, trunc, time.time() - t0, None))
            except Exception as e:
                self.results.put((name, seq, box, None, False, time.time() - t0, e))
            finally:
                with self.cv:
                    self.running = None


WORKER = None
_SEQ = itertools.count(1)   # global, so a re-added layer never accepts an old layer's in-flight result


def worker():
    global WORKER
    if WORKER is None:
        WORKER = LiveWorker()
    return WORKER


def gpkg_types(cols):
    out = {}
    for c, t in cols:
        if is_geo(t):
            continue
        u = str(t).upper()
        k = f"{c}_" if c in RESERVED else c
        out[k] = ("INTEGER" if u.split("(")[0] in ("INT", "BIGINT", "SMALLINT", "TINYINT", "LONG", "BOOLEAN")
                  else "REAL" if is_num(t) else "TEXT")
    return out


def start_live(table, geom, limit):
    os.makedirs(CACHE_DIR, exist_ok=True)
    name = table.split(".")[-1] + " (dynamic)"
    if name in LIVE and LIVE[name]["source"] != quote_table(table):
        name = ".".join(table.split(".")[-2:]) + " (dynamic)"     # same table name in another schema
    if name in LIVE:
        stop_live([name])
    ext = map_extent()
    if not ext:
        raise RuntimeError("Couldn't read the map's extent.")
    box = pad(ext)
    t0 = time.time()
    progress("Querying the view…")
    fc, trunc, host, cols = fetch(quote_table(table), geom, limit, box, wkb=True)
    path = os.path.join(CACHE_DIR, re.sub(r"[^\w\-]+", "_", table) + f"-{uuid.uuid4().hex[:6]}.gpkg")
    types = gpkg_types(cols)
    _write(path, fc, types_hint=types)
    call("layer.addData", {"path": path, "name": name, "stream": True})      # linked (read in place), so it can be reloaded
    LIVE[name] = {"source": quote_table(table), "geom": geom, "limit": limit, "path": path, "bbox": box, "truncated": trunc, "types": types,
                  "count": len(fc["features"]), "secs": time.time() - t0, "seq": next(_SEQ), "applied": 0}
    LIVE[name]["applied"] = LIVE[name]["seq"]
    worker()                                 # start the worker; its warm connection opens on its first refresh
    return name, len(fc["features"]), trunc, time.time() - t0


def schedule_refresh(force=False):
    """Queue a query for each dynamic layer whose loaded area doesn't cover the view (or is truncated)."""
    if not LIVE:
        return 0
    ext = map_extent()
    if not ext:
        return 0
    n = 0
    for name, L in LIVE.items():
        if not force and inside(ext, L["bbox"]) and not L["truncated"]:
            continue                                   # the view is inside what's already loaded: nothing to fetch
        L["seq"] = next(_SEQ)
        worker().submit(name, L["seq"], L["source"], L["geom"], L["limit"], pad(ext))
        n += 1
    return n


def apply_results():
    """Draw finished queries (main thread). Stale ones are dropped."""
    done = []
    while True:
        try:
            name, seq, box, fc, trunc, secs, err = worker().results.get_nowait() if WORKER else (None,) * 7
        except queue.Empty:
            break
        if name is None:
            break
        L = LIVE.get(name)
        if not L or seq < L["seq"]:
            continue                                   # a newer view is on its way
        if err is not None:
            L["applied"] = seq
            if "cancel" not in str(err).lower():
                warn_once((name, str(err)[:80]), f"{name}: couldn't refresh ({str(err).splitlines()[0][:160]})")
            continue
        _write(L["path"], fc, types_hint=L.get("types"))
        try:
            call("layer.reloadData", {"layer": name})
        except Exception as e:
            LIVE.pop(name, None)
            done.append(f"{name}: stopped ({e})")
            continue
        was_trunc = L["truncated"]
        L.update(bbox=box, truncated=trunc, count=len(fc["features"]), secs=secs, applied=seq)
        msg = f"{name}: {len(fc['features']):,} in view, {secs:.1f} s"
        if trunc:
            msg += f" (first {L['limit']:,}; zoom in for all)"
            if not was_trunc:
                notify(f"{name}: showing the first {L['limit']:,} rows here. Zoom in to see everything.")
        done.append(msg)
    return done


def refresh_live(force=False, wait=20):
    """Refresh now and wait for the result (the 'Refresh now' button and scripts)."""
    if not schedule_refresh(force):
        return []
    end, done = time.time() + wait, []
    while time.time() < end and any(L["applied"] < L["seq"] for L in LIVE.values()):
        time.sleep(0.05)
        done += apply_results()
    return done


def stop_live(names=None):
    for n in list(names or LIVE):
        LIVE.pop(n, None)


def live_text():
    if not LIVE:
        return "No dynamic layers. Choose a table, then Add table: it refreshes as you pan and zoom."
    return "\n".join(f"{n}: {L['count']:,} features in view" + (" (zoom in for all)" if L["truncated"] else "") for n, L in LIVE.items())


# ---------------------------------------------------------------------------- Genie
RULES_VERSION = "r2"          # bump when GENIE_RULES change, so cached spaces are recreated with the new instructions
GENIE_RULES = [
    "This Genie space answers questions for a GIS user in RustGIS, who sees every result on a map.\n",
    "ALWAYS return something the map can draw: keep the table's GEOMETRY column (or ST_ASWKT(geometry) AS wkt), "
    "or lon and lat for points, or an H3 cell id column named h3_cell.\n",
    "When the answer would return more than about 5,000 rows, aggregate: group points into H3 cells with "
    "h3_longlatash3(ST_X(geom), ST_Y(geom), 8) AS h3_cell and count or sum per cell, so the map shows a density surface.\n",
    "Keep a readable label column and the key numeric measure (name it clearly: customers, revenue_usd, avg_value).\n",
    "GROUP BY can't take a GEOMETRY column: aggregate first, then build geometry in an outer query.\n",
    "Coordinates are longitude/latitude degrees (SRID 4326, or 0 meaning lon/lat). For distances in metres use "
    "ST_DISTANCESPHERE(a, b) (e.g. ST_DISTANCESPHERE(s.geometry, c.geometry) <= 3000); never compare ST_DISTANCE on degrees with metres. "
    "Areas in m²: ST_AREA on ST_TRANSFORM(geom, 3857) is approximate; prefer counts per H3 cell.\n",
    "In your text answer, give a two-sentence insight and then two short, concrete recommendations.\n",
]


def genie_access(s):
    """True when this account can use Genie (the feature is on and the API answers)."""
    try:
        list(workspace(s).genie.list_spaces(page_size=1).spaces or [])
        return True, ""
    except Exception as e:
        return False, str(e).splitlines()[0][:160]


def _gid():
    return uuid.uuid4().hex


def sibling_tables(table, s, n=9):
    """Other spatial tables in the same schema (from the panel's tree), so Genie can join them."""
    sch = table.split(".")[1]
    for b in s.get("tree") or []:
        if b["id"] == f"schema:{sch}":
            return [c["id"] for c in b["children"] if c["id"] != table and not c["id"].split(".")[-1].startswith("__")][:n]
    return []


def ensure_space(table, s, related=True):
    spaces = s.get("genie_spaces") or {}
    key = table + ("+schema" if related else "") + "+" + RULES_VERSION
    if spaces.get(key):
        return spaces[key], False
    w = workspace(s)
    short = table.split(".")[-1]
    serialized = {
        "version": 2,
        "config": {"sample_questions": [
            {"id": _gid(), "question": [f"Map {short} and summarise what stands out"]},
            {"id": _gid(), "question": [f"Where are the densest areas of {short}? Aggregate to H3 cells"]},
        ]},
        "data_sources": {"tables": [{"identifier": t} for t in sorted([table] + (sibling_tables(table, s) if related else []))]},
        "instructions": {"text_instructions": [{"id": _gid(), "content": GENIE_RULES + [
            f"The user selected {table}: it is the main table; use the others only to join or enrich it.\n"]}]},
    }
    progress(f"Creating a Genie space for {short}…")
    sp = w.genie.create_space(warehouse_id=s["warehouse_id"], serialized_space=json.dumps(serialized),
                              title=f"RustGIS · {short}", description=f"Created by the RustGIS Databricks plugin for {table}.")
    spaces[key] = sp.space_id
    save_settings({"genie_spaces": spaces})
    return sp.space_id, True


def ask_genie(question, table, override_space=None, related=True):
    s = settings()
    if not s.get("profile"):
        raise RuntimeError("Connect first.")
    space = (override_space or "").strip()
    created = False
    if not space:
        if not table or table.startswith("schema:"):
            raise RuntimeError("Choose a table in the tree first: Genie answers about the selected table (or paste a Genie space ID).")
        try:
            space, created = ensure_space(table, s, related)
        except Exception as e:
            raise RuntimeError(f"Couldn't create a Genie space for {table} ({e}). Your account may not have Genie, or can't "
                               "create spaces here: paste an existing Genie space ID instead.")
    w = workspace(s)
    conv = s.get("genie_conv") or {}
    content = (f"{question}\n\n(Table: {table}. Return a mappable result. In your answer, start with a two-sentence insight, "
               "then give two short, concrete recommendations.)" if table else question)
    progress("Genie is writing and running the SQL…")
    timeout = datetime.timedelta(minutes=5)
    msg = None
    if conv.get("space") == space and conv.get("id"):
        try:
            msg = w.genie.create_message_and_wait(space, conv["id"], content, timeout=timeout)
        except Exception:
            save_settings({"genie_conv": None})       # the conversation expired or was deleted: start fresh
    if msg is None:
        msg = w.genie.start_conversation_and_wait(space, content, timeout=timeout)
        save_settings({"genie_conv": {"space": space, "id": msg.conversation_id}})
    text, sql, desc, follow = [], None, None, []
    for a in msg.attachments or []:
        if a.text and a.text.content:
            text.append(a.text.content)
        if a.query and a.query.query:
            sql, desc = a.query.query, a.query.description
        if a.suggested_questions and a.suggested_questions.questions:
            follow += list(a.suggested_questions.questions)
    if msg.error:
        text.append(f"Genie error: {msg.error}")
    return {"space": space, "created": created, "text": "\n\n".join(text).strip(), "sql": sql, "desc": desc, "follow": follow[:8]}


def mappable(sql_text):
    """Wrap Genie's SQL so it has a GEOMETRY column: native geometry, WKT text, lon/lat, or an H3 cell."""
    src = f"({sql_text.strip().rstrip(';')}\n)"        # newline: a trailing -- comment can't swallow the ')' 
    conn, _ = current()
    try:
        cols = describe(conn.cursor(), src)
    finally:
        conn.close()
    names = {c.lower(): c for c, _ in cols}
    if any(is_geo(t) for _, t in cols):
        return src, None, cols
    for key in ("wkt", "geom_wkt", "geometry_wkt", "wkt_geom", "geom", "geometry", "shape"):
        if key in names and str(dict(cols)[names[key]]).upper().startswith("STRING"):
            return f"(SELECT *, ST_SETSRID(ST_GEOMFROMWKT({q(names[key])}), 4326) AS __geom FROM {src})", "__geom", cols
    lon = next((names[k] for k in ("lon", "lng", "longitude") if k in names), None)      # not x/y: often projected metres
    lat = next((names[k] for k in ("lat", "latitude") if k in names), None)
    if lon and lat:
        return (f"(SELECT *, ST_SETSRID(ST_POINT(CAST({q(lon)} AS DOUBLE), CAST({q(lat)} AS DOUBLE)), 4326) AS __geom FROM {src})",
                "__geom", cols)
    h3 = next((c for c, t in cols if re.fullmatch(r"(h3_?(cell|index|id)?|h3_\w*cell\w*|cell|hex|hex_id)", c.lower()) and
               (str(t).upper().split("(")[0] in ("BIGINT", "LONG") or str(t).upper().startswith("STRING"))), None)
    if h3:
        return f"(SELECT *, ST_SETSRID(ST_GEOMFROMWKT(h3_boundaryaswkt({q(h3)})), 4326) AS __geom FROM {src})", "__geom", cols
    return None, None, cols


def zoom_to(name):
    """Put a layer on top and zoom the map to it."""
    for op, params in (("layer.move", {"layer": name, "to": 0}), ("layer.zoomTo", {"layer": name})):
        try:
            call(op, params)
        except Exception:
            pass


def style_by_measure(name, cols):
    skip = re.compile(r"(^|_)(id|lat|lon|lng|latitude|longitude|x|y|h3|cell|year|month)($|_)", re.I)
    measure = next((c for c, t in cols if is_num(t) and not skip.search(c)), None)
    if not measure:
        return None
    try:
        call("layer.symbology", {"layer": name, "type": "graduated", "field": measure, "classes": 5, "method": "quantile",
                                 "ramp": "Viridis"})
        return measure
    except Exception:
        return None


# ---------------------------------------------------------------------------- panel helpers
SECTIONS = {"tables": ["status", "conn", "connect", "catalog", "tables", "selected", "mode", "modehelp", "opts", "in-view",
                       "add-table", "area", "refresh-live", "stop-live"],
            "sql": ["status", "sqlform", "sql-layer"],
            "genie": ["status", "selected", "genieform", "ask-genie", "genie-new", "genie-answer", "followups"]}
ALL_ITEMS = sorted({i for v in SECTIONS.values() for i in v})
MODE_HELP = {
    "dynamic": "Add table makes a dynamic layer: pan or zoom and it refreshes by itself, querying only what's in view "
               "(up to the row limit), so you never pull more than you see.",
    "fixed": "Load once: Add table loads the table one time, whole or only the current view, or draw a box to load just that area.",
}


GENIE_ITEMS = ("genieform", "ask-genie", "genie-new", "followups")
NO_GENIE = ("Genie isn't available for this account or workspace, so these options are greyed out. Ask your Databricks "
            "admin to enable Genie (AI/BI) and grant you access, then Connect again.")


def visibility(section, mode, s=None):
    vis = {i: {"visible": i in SECTIONS.get(section, SECTIONS["tables"])} for i in ALL_ITEMS}
    ok = (s or {}).get("genie_ok")
    for i in GENIE_ITEMS:
        vis[i]["enabled"] = bool(ok)
    vis["ask-genie"]["label"] = "Ask Genie" if ok else "Ask Genie (needs Genie)"
    if section == "genie" and not ok:
        vis["genie-answer"] = {"visible": True, "text": NO_GENIE if (s or {}).get("host") else "Connect first; Genie options turn on when your account has Genie."}
    if section == "tables":
        dyn = mode == "dynamic"
        for i in ("in-view", "area"):
            vis[i]["visible"] = not dyn
        for i in ("refresh-live", "stop-live"):
            vis[i]["visible"] = dyn
        vis["add-table"]["label"] = "Add to map" if dyn else "Load once"
    return vis


def merge(*dicts):
    """Merge panel item updates key by key (so visibility doesn't wipe an item's text, fields or nodes)."""
    out = {}
    for d in dicts:
        for k, v in (d or {}).items():
            out[k] = {**out.get(k, {}), **v}
    return out


def form_fields(s):
    return {
        "conn": [{"id": "profile", "kind": "text", "label": "CLI profile", "default": s.get("profile", "DEFAULT"), "required": True,
                  "help": "A profile in ~/.databrickscfg, signed in with `databricks auth login`."},
                 {"id": "warehouse_id", "kind": "text", "label": "SQL warehouse ID", "default": s.get("warehouse_id", ""), "required": True,
                  "help": "From the warehouse's Connection details (the last part of its HTTP path)."}],
        "opts": [{"id": "limit", "kind": "number", "integer": True, "label": "Row limit (per view)", "default": s.get("limit", DEFAULT_LIMIT),
                  "help": "Dynamic layers fetch at most this many rows for the view; zoom in for more detail."},
                 {"id": "geometry", "kind": "text", "label": "Geometry column", "default": "",
                  "help": "Leave empty to use the table's first GEOMETRY / GEOGRAPHY column."}],
        "sqlform": [{"id": "sql", "kind": "text", "label": "SELECT", "default": "",
                     "help": "Any SELECT with a GEOMETRY / GEOGRAPHY column, e.g. ST_POINT(lon, lat) AS geom."},
                    {"id": "name", "kind": "text", "label": "Layer name", "default": "Databricks SQL"}],
        "genieform": [{"id": "question", "kind": "text", "label": "Ask Genie", "default": "",
                       "help": "A plain-English question about the selected table. Genie writes and runs the SQL; the result is mapped."},
                      {"id": "related", "kind": "checkbox", "label": "Include the schema's other tables", "default": True,
                       "help": "Lets Genie join the selected table with up to 9 other spatial tables in its schema."},
                      {"id": "space", "kind": "text", "label": "Genie space ID (optional)", "default": "",
                       "help": "Leave empty to use a space made for the selected table, or paste a curated space's ID."}],
    }


def status_text(s):
    if s.get("host"):
        genie = "Genie available" if s.get("genie_ok") else "Genie not available"
        return f"{s.get('user', '?')} on {s['host']}\nprofile {s.get('profile')} · warehouse {s.get('warehouse_id')} · {genie}"
    return "Not connected. Enter a Databricks CLI profile and a SQL warehouse ID, then Connect."


def tree_nodes(rows):
    by_schema = {}
    for cat, sch, tbl, col, typ in rows:
        by_schema.setdefault(sch, {}).setdefault(f"{cat}.{sch}.{tbl}", []).append(f"{col} {typ}")
    return [{"id": f"schema:{sch}", "label": sch, "count": len(tables),
             "children": [{"id": full, "label": full.split(".")[-1] + "  ·  " + ", ".join(cols)} for full, cols in sorted(tables.items())]}
            for sch, tables in sorted(by_schema.items())]


QUIET = [True]             # set per command: panel clicks (no params) get no pop-up report, just the panel text


def answer(report, items=None, values=None):
    panel = {"items": merge({"result": {"text": report}}, items or {})}
    if values:
        panel["values"] = values
    out = {"panel": panel}
    if not QUIET[0]:
        out["report"] = report      # scripts / rustgis-cli / agents still get a one-line result
    return out


def pick(params, panel, key, form=None, field=None):
    if key in params and params[key] not in (None, ""):
        return params[key]
    if form:
        return (panel.get(form) or {}).get(field or key)
    return panel.get(key)


def list_spatial(cat, sch=""):
    if not re.fullmatch(r"[\w\-]+", cat) or (sch and not re.fullmatch(r"[\w\-]+", sch)):
        raise RuntimeError("Catalog and schema names: letters, digits, _ and - only.")
    progress(f"Listing spatial tables in {cat}…")
    conn, _ = current()
    try:
        cur = conn.cursor()
        cur.execute(f"""SELECT table_catalog, table_schema, table_name, column_name, data_type
                        FROM {q(cat)}.information_schema.columns
                        WHERE (upper(data_type) LIKE 'GEOMETRY%' OR upper(data_type) LIKE 'GEOGRAPHY%')
                          AND table_schema <> 'information_schema'
                          {"AND table_schema = :s" if sch else ""}
                        ORDER BY 2, 3 LIMIT 2000""", {"s": sch} if sch else None)
        rows = cur.fetchall()
    finally:
        conn.close()
    return tree_nodes(rows)


def browse_report(cat, nodes, sch=""):
    if not nodes:
        return f"No GEOMETRY / GEOGRAPHY columns in {cat}{'.' + sch if sch else ''}. Choose another catalog."
    return f"{sum(len(b['children']) for b in nodes)} spatial table(s) in {len(nodes)} schema(s) of {cat}. Pick one in the tree."


# ---------------------------------------------------------------------------- commands
def cmd_panel_state(p, pv):
    s = settings()
    f = form_fields(s)
    section, mode = pv.get("section") or "tables", pv.get("mode") or "dynamic"
    items = merge(visibility(section, mode, s),
                  {"status": {"text": status_text(s)}, "conn": {"fields": f["conn"]}, "opts": {"fields": f["opts"]},
                   "sqlform": {"fields": f["sqlform"]}, "genieform": {"fields": f["genieform"]},
                   "catalog": {"options": [{"id": c, "label": c} for c in s.get("catalogs") or []]},
                   "modehelp": {"text": MODE_HELP[mode] + ("\n" + live_text() if mode == "dynamic" else "")}})
    values = {}
    if s.get("profile"):
        values["conn"] = {"profile": s["profile"], "warehouse_id": s.get("warehouse_id", "")}
    tree = s.get("tree")
    if s.get("catalog") and not tree and s.get("host"):
        try:
            tree = list_spatial(s["catalog"])
            save_settings({"tree": tree})
        except Exception as e:
            items["result"] = {"text": f"Couldn't list {s['catalog']}: {e}. Connect again."}
    if tree:
        items["tables"] = {"nodes": tree}
    if s.get("catalog"):
        values["catalog"] = s["catalog"]
    out = {"panel": {"items": items}}
    if values:
        out["panel"]["values"] = values
    return out


def cmd_section(p, pv):
    return {"panel": {"items": visibility(p.get("section") or pv.get("section") or "tables", pv.get("mode") or "dynamic", settings())}}


def cmd_mode(p, pv):
    mode = p.get("mode") or pv.get("mode") or "dynamic"
    items = merge(visibility(pv.get("section") or "tables", mode, settings()),
                  {"modehelp": {"text": MODE_HELP[mode] + ("\n" + live_text() if mode == "dynamic" else "")}})
    return {"panel": {"items": items}}


def cmd_connect(p, pv):
    profile = (pick(p, pv, "profile", "conn") or "").strip()
    wh = (pick(p, pv, "warehouse_id", "conn") or "").strip()
    if not profile or not wh:
        raise RuntimeError("Enter a CLI profile and a SQL warehouse ID.")
    conn, host = connect(profile, wh)
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_user(), current_catalog()")
        user, cat = cur.fetchone()
        cur.execute("SHOW CATALOGS")
        catalogs = sorted(r[0] for r in cur.fetchall() if r[0] not in ("system", "__databricks_internal", "samples"))[:1000]
    finally:
        conn.close()
    CONN.update(profile=profile, warehouse_id=wh)
    SCHEMA.clear()
    default_cat = cat if cat in catalogs else (catalogs[0] if catalogs else None)
    save_settings({"profile": profile, "warehouse_id": wh, "host": host, "user": user, "catalogs": catalogs,
                   "catalog": default_cat, "tree": None})
    s = {"profile": profile, "warehouse_id": wh, "host": host, "user": user}
    g_ok, g_why = genie_access(s)
    s["genie_ok"] = g_ok
    nodes = list_spatial(default_cat) if default_cat else []
    save_settings({"tree": nodes, "genie_ok": g_ok})
    report = (f"Connected as {user}. {len(catalogs)} catalogs. " + (browse_report(default_cat, nodes) if default_cat else "")
              + (" Genie: available." if g_ok else " Genie: not available for this account (Genie options are greyed out)."))
    values = {"conn": {"profile": profile, "warehouse_id": wh}}
    if default_cat:
        values["catalog"] = default_cat
    items = {"status": {"text": status_text(s)}, "catalog": {"options": [{"id": c, "label": c} for c in catalogs]},
             "tables": {"nodes": nodes}, "selected": {"text": "Choose a table in the tree."}}
    items = merge({k: v for k, v in visibility(pv.get("section") or "tables", pv.get("mode") or "dynamic", s).items() if k in GENIE_ITEMS}, items)
    return answer(report, items, values)


def cmd_browse(p, pv):
    cat = (pick(p, pv, "catalog") or "").strip()
    sch = (p.get("schema") or "").strip()
    if not cat:
        raise RuntimeError("Choose a catalog.")
    nodes = list_spatial(cat, sch)
    save_settings({"catalog": cat, "tree": nodes})
    out = answer(browse_report(cat, nodes, sch), {"tables": {"nodes": nodes}, "selected": {"text": "Choose a table in the tree."}},
                 {"catalog": cat})
    out["tables"] = [c["id"] for b in nodes for c in b["children"]]
    return out


def cmd_select_table(p, pv):
    t = p.get("table") or pv.get("tables") or ""
    if t.startswith("schema:") or t.count(".") != 2:
        return {"panel": {"items": {"selected": {"text": "Choose a table (not a schema) in the tree."}}}}
    s = settings()
    known = any(k.startswith(t + "+") or k == t for k in (s.get("genie_spaces") or {}))
    hint = " · Genie space ready" if known else " · Genie: a space is made on your first question"
    return {"panel": {"items": {"selected": {"text": t + hint}}}}


def _limit(p, pv):
    return min(int(pick(p, pv, "limit", "opts") or DEFAULT_LIMIT), MAX_ROWS)


def _table(p, pv):
    t = (p.get("table") or pv.get("tables") or "").strip()
    if not t or t.startswith("schema:"):
        raise RuntimeError("Choose a table in the panel's tree (or pass table).")
    return t


def _geom(p, pv):
    return (pick(p, pv, "geometry", "opts") or "").strip() or None


def cmd_add_table(p, pv):
    t = _table(p, pv)
    mode = p.get("mode") or pv.get("mode") or "dynamic"
    if mode == "dynamic" or p.get("dynamic"):
        name, n, trunc, secs = start_live(t, _geom(p, pv), _limit(p, pv))
        msg = (f"Dynamic layer '{name}': {n:,} features in view ({secs:.1f} s). It refreshes as you pan and zoom"
               + (f"; showing the first {_limit(p, pv):,}, zoom in for all." if trunc else "."))
        return answer(msg, {"modehelp": {"text": MODE_HELP["dynamic"] + "\n" + live_text()}})
    in_view = p["in_view"] if "in_view" in p else bool(pv.get("in-view"))
    bbox = map_extent() if in_view else None
    name = t.split(".")[-1] + (" (view)" if in_view else "")
    report, _ = load(quote_table(t), _geom(p, pv), name, _limit(p, pv), bbox)
    if not in_view:
        zoom_to(name)
    return answer(report)


def cmd_add_in_box(p, pv):
    t = _table(p, pv)
    bbox = p.get("bbox") or (pv.get("area") or {}).get("bbox")
    if not bbox or len(bbox) != 4:
        raise RuntimeError("Draw a box on the map with 'Add table inside a box…' (or pass bbox).")
    report, _ = load(quote_table(t), _geom(p, pv), t.split(".")[-1] + " (box)", _limit(p, pv), bbox)
    return answer(report)


def cmd_refresh_live(p, pv):
    if not LIVE:
        return answer("No dynamic layers to refresh.")
    done = refresh_live(force=True)
    return answer("\n".join(done) or "Dynamic layers are up to date.", {"modehelp": {"text": MODE_HELP["dynamic"] + "\n" + live_text()}})


def cmd_stop_live(p, pv):
    n = len(LIVE)
    stop_live()
    return answer(f"Stopped {n} dynamic layer(s). They stay on the map as they are.",
                  {"modehelp": {"text": MODE_HELP["dynamic"] + "\n" + live_text()}})


def cmd_sql_layer(p, pv):
    sql_text = (pick(p, pv, "sql", "sqlform") or "").strip().rstrip(";")
    if not re.match(r"(?is)^\s*(select|with)\b", sql_text):
        raise RuntimeError("Only SELECT (or WITH … SELECT) statements.")
    name = pick(p, pv, "name", "sqlform") or "Databricks SQL"
    report, _ = load(f"({sql_text})", (p.get("geometry") or "").strip() or None, name, _limit(p, pv))
    return answer(report)


def _genie(question, table, space_override, pv):
    t0 = time.time()
    related = (pv.get("genieform") or {}).get("related", True)
    g = ask_genie(question, table, space_override, related is not False)
    parts = []
    if g["created"]:
        parts.append(f"Created Genie space 'RustGIS · {table.split('.')[-1]}' for {table}.")
    if g["text"]:
        parts.append(g["text"])
    layer_note = ""
    if g["sql"]:
        src, gcol, cols = mappable(g["sql"])
        if src:
            name = "Genie: " + (question[:40] + ("…" if len(question) > 40 else ""))
            report, cols2 = load(src, gcol, name, _limit({}, pv))
            measure = style_by_measure(name, cols2)
            zoom_to(name)
            layer_note = report + (f" Styled by {measure}." if measure else "")
        else:
            layer_note = "Genie's result has no geometry, WKT, lon/lat or H3 cell to map; see the answer above."
        parts.append("SQL: " + re.sub(r"\s+", " ", g["sql"])[:600])
    parts.append(layer_note or "Genie answered without a query.")
    parts.append(f"({time.time() - t0:.0f} s)")
    text = "\n\n".join(p_ for p_ in parts if p_)
    items = {"genie-answer": {"text": text[:3990]},
             "followups": {"options": [{"id": f"f{i}", "label": fq[:190]} for i, fq in enumerate(g["follow"])]}}
    save_settings({"genie_follow": g["follow"]})
    return answer(layer_note or "Genie answered.", items)


def _require_genie():
    if not settings().get("genie_ok"):
        raise RuntimeError(NO_GENIE)


def cmd_ask_genie(p, pv):
    _require_genie()
    question = (pick(p, pv, "question", "genieform") or "").strip()
    if not question:
        raise RuntimeError("Type a question for Genie.")
    table = p.get("table") or pv.get("tables") or ""
    return _genie(question, table, pick(p, pv, "space", "genieform"), pv)


def cmd_genie_follow(p, pv):
    _require_genie()
    fid = p.get("followup") or pv.get("followups")
    follow = settings().get("genie_follow") or []
    try:
        question = follow[int(str(fid).lstrip("f"))]
    except (ValueError, IndexError, TypeError):
        raise RuntimeError("Choose a follow-up question.")
    return _genie(question, p.get("table") or pv.get("tables") or "", pick(p, pv, "space", "genieform"), pv)


def cmd_genie_new(p, pv):
    save_settings({"genie_conv": None, "genie_follow": []})
    return answer("New Genie conversation: the next question starts fresh.",
                  {"genie-answer": {"text": "—"}, "followups": {"options": []}})


COMMANDS = {"connect": cmd_connect, "browse": cmd_browse, "add-table": cmd_add_table, "add-in-box": cmd_add_in_box,
            "sql-layer": cmd_sql_layer, "select-table": cmd_select_table, "panel-state": cmd_panel_state,
            "section": cmd_section, "mode": cmd_mode, "refresh-live": cmd_refresh_live, "stop-live": cmd_stop_live,
            "ask-genie": cmd_ask_genie, "genie-follow": cmd_genie_follow, "genie-new": cmd_genie_new}


def run_command(msg):
    req = msg["command"]
    try:
        fn = COMMANDS.get(req.get("id"))
        pv = (req.get("panel") or {}).get("values") or {}
        if not fn:
            send({"error": f"unknown command '{req.get('id')}'", "id": msg.get("id")})
            return
        QUIET[0] = not (req.get("params") or {})
        send({"ok": fn(req.get("params") or {}, pv), "id": msg.get("id")})
    except SystemExit:
        raise
    except Exception as e:
        send({"error": f"{type(e).__name__}: {e}", "id": msg.get("id")})


# ---------------------------------------------------------------------------- main loop
def main():
    threading.Thread(target=_reader, daemon=True).start()
    view_changed_at = None
    while True:
        msg = _pending.pop(0) if _pending else None
        if msg is None:
            try:
                msg = _inbox.get(timeout=0.05)
            except queue.Empty:
                msg = "tick"
            if msg is _EOF:
                return                                  # stdin closed: exit with RustGIS
        if isinstance(msg, dict) and "command" in msg:
            run_command(msg)
        elif isinstance(msg, dict) and msg.get("event") == "view":
            view_changed_at = time.time()
        elif isinstance(msg, dict) and msg.get("event") == "layers" and LIVE:
            try:
                names = {l.get("name") for l in call("host.layers") or []}
                for n in [n for n in LIVE if n not in names]:
                    LIVE.pop(n, None)
            except Exception:
                pass
        try:
            if view_changed_at and LIVE and time.time() - view_changed_at >= DEBOUNCE_S:
                view_changed_at = None
                schedule_refresh()
            if WORKER is not None:
                apply_results()
        except SystemExit:
            raise
        except Exception as e:
            notify(f"Dynamic refresh failed: {e}", error=True)


if __name__ == "__main__":
    main()
