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
import json
import os
import queue
import re
import sys
import tempfile
import threading
import time
import uuid

MAX_ROWS = 200_000
DEFAULT_LIMIT = 50_000
PAD = 0.25                 # dynamic layers fetch 25% beyond the view, so small pans don't re-query
DEBOUNCE_S = 0.8           # wait this long after the last view change before re-querying
CACHE_DIR = os.path.join(tempfile.gettempdir(), "rustgis-databricks")

# ---------------------------------------------------------------------------- session transport
_inbox = queue.Queue()
_pending = []              # commands / events that arrived while we waited for a host reply
_lock = threading.Lock()
_seq = [0]


def _reader():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            _inbox.put(json.loads(line))
        except ValueError:
            continue
    _inbox.put(None)       # stdin closed: RustGIS is stopping us


def send(obj):
    with _lock:
        sys.stdout.write(json.dumps(obj) + "\n")
        sys.stdout.flush()


def call(op, params=None, timeout=120):
    _seq[0] += 1
    cid = f"c{_seq[0]}"
    send({"call": {"op": op, "params": params or {}}, "id": cid})
    end = time.time() + timeout
    while True:
        msg = _inbox.get(timeout=max(1, end - time.time()))
        if msg is None:
            raise SystemExit(0)
        if msg.get("id") == cid and ("ok" in msg or "error" in msg) and "command" not in msg:
            if "error" in msg:
                raise RuntimeError(msg["error"])
            return msg.get("ok")
        _pending.append(msg)


def progress(text):
    send({"progress": text})


def notify(text, error=False):
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


def connect(profile, warehouse_id):
    from databricks import sql
    cfg = sdk_config(profile)
    host = cfg.host.replace("https://", "").rstrip("/")
    conn = sql.connect(server_hostname=host, http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                       credentials_provider=lambda: cfg.authenticate, _user_agent_entry="rustgis-databricks-plugin/0.3")
    return conn, host


def current():
    s = settings()
    if not s.get("profile") or not s.get("warehouse_id"):
        raise RuntimeError("Not connected yet: in the Databricks panel, enter a CLI profile and a warehouse ID, then Connect.")
    return connect(s["profile"], s["warehouse_id"])


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
    return call("host.view") and (call("map.extent") or {}).get("extent")


def pad(b, f=PAD):
    w, s, e, n = b
    dx, dy = (e - w) * f, (n - s) * f
    return [w - dx, max(s - dy, -89.9), e + dx, min(n + dy, 89.9)]


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


def fetch(source_sql, geom_col, limit, bbox=None, wkb=False):
    """Run the query (optionally inside a lon/lat box). Returns (FeatureCollection, truncated, host, cols);
    with wkb=True the FeatureCollection's features carry WKB bytes in "wkb" instead of GeoJSON geometry."""
    conn, host = current()
    try:
        cur = conn.cursor()
        cols = describe(cur, source_sql)
        geom_col, e, flt = geo_expr(cols, geom_col)
        where = ""
        if bbox:
            w, s_, ea, n = (float(v) for v in bbox)
            where = (f" WHERE ST_INTERSECTS({flt}, ST_GEOMFROMTEXT("
                     f"'POLYGON(({w} {s_}, {ea} {s_}, {ea} {n}, {w} {n}, {w} {s_}))', 4326))")
        others = ", ".join(q(c) for c, t in cols if c != geom_col and not is_geo(t))
        sel = (others + ", " if others else "") + (f"ST_ASWKB({e}) AS __geojson" if wkb else f"ST_ASGEOJSON({e}) AS __geojson")
        cur.execute(f"SELECT {sel} FROM {source_sql} AS src{where} LIMIT {int(limit)}")
        rows = cur.fetchall()
        names = [d[0] for d in cur.description]
    finally:
        conn.close()
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
                val = float(val)                   # DECIMAL columns stay numeric, so they can drive styles
            elif val is not None and not isinstance(val, (str, int, float, bool)):
                val = str(val)
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


def _write(path, fc, table="features"):
    """Write (or rewrite in place) a one-layer GeoPackage, so RustGIS reads it as a linked source it can reload.
    Stdlib sqlite3 only: GeoPackage geometry = 8-byte GP header (little-endian, no envelope, SRID 4326) + WKB."""
    import sqlite3
    import struct
    feats = fc["features"]
    keys = []
    for f in feats:
        for k in f["properties"]:
            if k not in keys:
                keys.append(k)
    types = {k: next((_gpkg_type(f["properties"][k]) for f in feats if f["properties"].get(k) is not None), "TEXT") for k in keys}
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
        names = ", ".join(['geom'] + [f'"{k}"' for k in keys])
        marks = ", ".join(["?"] * (len(keys) + 1))
        cur.executemany(f'INSERT INTO "{table}" ({names}) VALUES ({marks})',
                        [[head + f["wkb"]] + [f["properties"].get(k) for k in keys] for f in feats])
        cur.execute("UPDATE gpkg_contents SET last_change = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE table_name = ?", (table,))
        con.commit()
    finally:
        con.close()


def start_live(table, geom, limit):
    os.makedirs(CACHE_DIR, exist_ok=True)
    name = table.split(".")[-1] + " (dynamic)"
    if name in LIVE:
        stop_live([name])
    ext = map_extent()
    if not ext:
        raise RuntimeError("Couldn't read the map's extent.")
    box = pad(ext)
    t0 = time.time()
    progress("Querying the view…")
    fc, trunc, host, _ = fetch(quote_table(table), geom, limit, box, wkb=True)
    path = os.path.join(CACHE_DIR, re.sub(r"[^\w\-]+", "_", table) + f"-{uuid.uuid4().hex[:6]}.gpkg")
    _write(path, fc)
    call("layer.addData", {"path": path, "name": name, "stream": True})      # linked (read in place), so it can be reloaded
    LIVE[name] = {"source": quote_table(table), "geom": geom, "limit": limit, "path": path, "bbox": box, "truncated": trunc,
                  "count": len(fc["features"]), "secs": time.time() - t0}
    return name, len(fc["features"]), trunc, time.time() - t0


def refresh_live(force=False):
    if not LIVE:
        return []
    ext = map_extent()
    if not ext:
        return []
    done = []
    for name, L in list(LIVE.items()):
        if not force and inside(ext, L["bbox"]) and not L["truncated"]:
            continue                               # the view is inside what's already loaded
        box = pad(ext)
        t0 = time.time()
        try:
            fc, trunc, _, _ = fetch(L["source"], L["geom"], L["limit"], box, wkb=True)
        except Exception as e:
            notify(f"{name}: {e}", error=True)
            continue
        _write(L["path"], fc)
        try:
            call("layer.reloadData", {"layer": name})
        except Exception as e:                     # the layer was removed or renamed in RustGIS
            LIVE.pop(name, None)
            done.append(f"{name}: stopped ({e})")
            continue
        L.update(bbox=box, truncated=trunc, count=len(fc["features"]), secs=time.time() - t0)
        msg = f"{name}: {len(fc['features']):,} features in view ({time.time() - t0:.1f} s)"
        if trunc:
            msg += f", the first {L['limit']:,} of more. Zoom in to see everything."
        notify(msg)
        done.append(msg)
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
    if conv.get("space") == space and conv.get("id"):
        msg = w.genie.create_message_and_wait(space, conv["id"], content, timeout=timeout)
    else:
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
    src = f"({sql_text.strip().rstrip(';')})"
    conn, _ = current()
    try:
        cols = describe(conn.cursor(), src)
    finally:
        conn.close()
    names = {c.lower(): c for c, _ in cols}
    if any(is_geo(t) for _, t in cols):
        return src, None, cols
    for key in ("wkt", "geom_wkt", "geometry_wkt", "wkt_geom", "geom", "geometry", "shape"):
        if key in names and not is_num(dict(cols)[names[key]]):
            return f"(SELECT *, ST_SETSRID(ST_GEOMFROMWKT({q(names[key])}), 4326) AS __geom FROM {src})", "__geom", cols
    lon = next((names[k] for k in ("lon", "lng", "longitude", "x") if k in names), None)
    lat = next((names[k] for k in ("lat", "latitude", "y") if k in names), None)
    if lon and lat:
        return (f"(SELECT *, ST_SETSRID(ST_POINT(CAST({q(lon)} AS DOUBLE), CAST({q(lat)} AS DOUBLE)), 4326) AS __geom FROM {src})",
                "__geom", cols)
    h3 = next((c for c, t in cols if ("h3" in c.lower() or c.lower() in ("cell", "hex", "hex_id")) and
               (is_num(t) or t.upper().startswith("STRING"))), None)
    if h3:
        return f"(SELECT *, ST_SETSRID(ST_GEOMFROMWKT(h3_boundaryaswkt({q(h3)})), 4326) AS __geom FROM {src})", "__geom", cols
    return None, None, cols


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
    "fixed": "My parameters: Add table loads once, with your row limit, the current view or a box you draw.",
    "dynamic": "Follow the map: Add table makes a dynamic layer. Each pan or zoom re-queries the warehouse with the "
               "view's box (ST_INTERSECTS), up to the row limit, so you never pull more than you see.",
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
        vis["add-table"]["label"] = "Add dynamic layer" if dyn else "Add table"
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
        "opts": [{"id": "limit", "kind": "number", "integer": True, "label": "Row limit", "default": s.get("limit", DEFAULT_LIMIT)},
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


def answer(report, items=None, values=None):
    panel = {"items": merge({"result": {"text": report}}, items or {})}
    if values:
        panel["values"] = values
    return {"report": report, "panel": panel}


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
    section, mode = pv.get("section") or "tables", pv.get("mode") or "fixed"
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
    return {"panel": {"items": visibility(p.get("section") or pv.get("section") or "tables", pv.get("mode") or "fixed", settings())}}


def cmd_mode(p, pv):
    mode = p.get("mode") or pv.get("mode") or "fixed"
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
    items = merge({k: v for k, v in visibility(pv.get("section") or "tables", pv.get("mode") or "fixed", s).items() if k in GENIE_ITEMS}, items)
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
    mode = p.get("mode") or pv.get("mode") or "fixed"
    if mode == "dynamic" or p.get("dynamic"):
        name, n, trunc, secs = start_live(t, _geom(p, pv), _limit(p, pv))
        msg = (f"Dynamic layer '{name}': {n:,} features in view ({secs:.1f} s). It refreshes as you pan and zoom"
               + (f"; showing the first {_limit(p, pv):,}, zoom in for all." if trunc else "."))
        return answer(msg, {"modehelp": {"text": MODE_HELP["dynamic"] + "\n" + live_text()}})
    in_view = p["in_view"] if "in_view" in p else bool(pv.get("in-view"))
    bbox = map_extent() if in_view else None
    report, _ = load(quote_table(t), _geom(p, pv), t.split(".")[-1] + (" (view)" if in_view else ""), _limit(p, pv), bbox)
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
                msg = _inbox.get(timeout=0.2)
            except queue.Empty:
                msg = "tick"
            if msg is None:
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
        if view_changed_at and LIVE and time.time() - view_changed_at >= DEBOUNCE_S:
            view_changed_at = None
            try:
                refresh_live()
            except SystemExit:
                raise
            except Exception as e:
                notify(f"Dynamic refresh failed: {e}", error=True)


if __name__ == "__main__":
    main()
