"""Databricks SQL connector for RustGIS (process plugin, ABI 1).

RustGIS starts this program once per command and talks to it over stdin/stdout, one JSON object per line
(see RustGIS docs/plugins.md, "Process plugins"). Layers are added with the host request `host.addLayer`.

Sign-in goes through the Databricks SDK and a Databricks CLI profile (~/.databrickscfg), so OAuth (U2M or
M2M), PATs and Azure auth all work as they do for the CLI; the plugin never stores a token itself.
"""
import json
import math
import re
import sys

MAX_ROWS = 200_000


# ---------------------------------------------------------------------------- RustGIS protocol
def call(op, params=None):
    print(json.dumps({"call": {"op": op, "params": params or {}}}), flush=True)
    reply = json.loads(sys.stdin.readline())
    if "error" in reply:
        raise RuntimeError(reply["error"])
    return reply["ok"]


def log(text):
    print(json.dumps({"log": text}), flush=True)


def progress(text):
    print(json.dumps({"progress": text}), flush=True)


# ---------------------------------------------------------------------------- Databricks
def settings():
    try:
        return call("host.settings") or {}
    except Exception:            # tools and some CLI runs have no settings
        return {}


def connect(profile, warehouse_id):
    from databricks import sql
    from databricks.sdk.core import Config

    cfg = Config(profile=profile)
    host = cfg.host.replace("https://", "").rstrip("/")
    conn = sql.connect(server_hostname=host, http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                       credentials_provider=lambda: cfg.authenticate, _user_agent_entry="rustgis-databricks-plugin/0.1")
    return conn, host


def current(params):
    s = settings()
    profile = params.get("profile") or s.get("profile")
    wh = params.get("warehouse_id") or s.get("warehouse_id")
    if not profile or not wh:
        raise RuntimeError("Not connected yet: run Databricks SQL › Connect first (or pass profile and warehouse_id).")
    return connect(profile, wh)


def quote_table(name):
    parts = [p.strip("` ") for p in name.split(".")]
    if len(parts) != 3 or not all(parts):
        raise RuntimeError(f"Use a three-part name, catalog.schema.table (got '{name}').")
    return ".".join(f"`{p.replace('`', '``')}`" for p in parts)


def q(ident):
    return f"`{ident.replace('`', '``')}`"


def is_geo(dtype):
    return str(dtype).upper().startswith(("GEOMETRY", "GEOGRAPHY"))


def view_bbox():
    """Approximate lon/lat box of the map view, from its centre and scale (assumes a 1600 x 1000 px window)."""
    v = call("host.view")
    lon, lat = v["center"]
    mpp = float(v["scale"]) * 0.00028                      # metres per pixel at the standard 0.28 mm pixel
    half_w, half_h = 800 * mpp, 500 * mpp
    dlat = half_h / 110_540
    dlon = half_w / (111_320 * max(math.cos(math.radians(lat)), 0.05))
    return [lon - dlon, max(lat - dlat, -89.9), lon + dlon, min(lat + dlat, 89.9)]


def to_feature_collection(cur, rows, geom_col):
    names = [d[0] for d in cur.description]
    gi = names.index("__geojson")
    feats = []
    for r in rows:
        g = r[gi]
        if g is None:
            continue
        props = {}
        for n, val in zip(names, r):
            if n in ("__geojson", geom_col):
                continue
            if val is not None and not isinstance(val, (str, int, float, bool)):
                val = str(val)
            props[n] = val
        feats.append({"type": "Feature", "geometry": json.loads(g), "properties": props})
    return {"type": "FeatureCollection", "features": feats}


def load(source_sql, geom_col, name, limit, bbox=None):
    """source_sql is a quoted table or '(SELECT ...)'. Returns a report string after adding the layer."""
    conn, host = current({})
    try:
        cur = conn.cursor()
        cur.execute(f"DESCRIBE QUERY SELECT * FROM {source_sql} AS src")
        cols = [(r[0], str(r[1])) for r in cur.fetchall() if r[0] and not r[0].startswith("#")]
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
        # GeoJSON in WGS 84: transform GEOMETRY with a known SRID; GEOGRAPHY is already lon/lat
        m = re.search(r"\((\d+)\)", dtype)
        srid = int(m.group(1)) if m else 0          # GEOMETRY with no or 0 SRID: assume lon/lat, as QGIS and RustGIS do
        if dtype.startswith("GEOGRAPHY") or srid == 4326:
            geo4326 = g
        elif srid == 0:
            geo4326 = f"ST_SETSRID({g}, 4326)"     # stamp it so ST_INTERSECTS with a 4326 box is allowed
        else:
            geo4326 = f"ST_TRANSFORM({g}, 4326)"
        where = ""
        if bbox:
            w, s_, e, n = bbox
            where = (f" WHERE ST_INTERSECTS({geo4326}, ST_GEOMFROMTEXT('POLYGON(({w} {s_}, {e} {s_}, {e} {n}, {w} {n}, {w} {s_}))', 4326))"
                     if not dtype.startswith("GEOGRAPHY") else
                     f" WHERE ST_INTERSECTS(ST_GEOMFROMWKB(ST_ASWKB({g}), 4326), ST_GEOMFROMTEXT('POLYGON(({w} {s_}, {e} {s_}, {e} {n}, {w} {n}, {w} {s_}))', 4326))")
        others = ", ".join(q(c) for c, t in cols if c != geom_col and not is_geo(t))
        sel = (others + ", " if others else "") + f"ST_ASGEOJSON({geo4326}) AS __geojson"
        progress(f"Querying {host}…")
        cur.execute(f"SELECT {sel} FROM {source_sql} AS src{where} LIMIT {int(limit)}")
        rows = cur.fetchall()
        fc = to_feature_collection(cur, rows, geom_col)
    finally:
        conn.close()
    call("host.addLayer", {"name": name, "geojson": fc})
    clipped = " (row limit reached)" if len(rows) >= int(limit) else ""
    return f"Added '{name}': {len(fc['features']):,} features from {host}{clipped}."


# ---------------------------------------------------------------------------- commands
def cmd_connect(p):
    profile, wh = (p.get("profile") or "").strip(), (p.get("warehouse_id") or "").strip()
    conn, host = connect(profile, wh)
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_user(), current_catalog()")
        user, cat = cur.fetchone()
    finally:
        conn.close()
    call("host.setSettings", {"values": {"profile": profile, "warehouse_id": wh, "host": host}})
    return {"report": f"Connected to {host} as {user} (warehouse {wh}, default catalog {cat}). Saved for this plugin."}


def cmd_browse(p):
    cat = (p.get("catalog") or "").strip()
    sch = (p.get("schema") or "").strip()
    if not re.fullmatch(r"[\w\-]+", cat) or (sch and not re.fullmatch(r"[\w\-]+", sch)):
        raise RuntimeError("Catalog and schema names: letters, digits, _ and - only.")
    conn, _ = current({})
    try:
        cur = conn.cursor()
        cur.execute(f"""SELECT table_schema, table_name, column_name, data_type
                        FROM {q(cat)}.information_schema.columns
                        WHERE (upper(data_type) LIKE 'GEOMETRY%' OR upper(data_type) LIKE 'GEOGRAPHY%')
                        {"AND table_schema = :s" if sch else ""}
                        ORDER BY 1, 2 LIMIT 500""", {"s": sch} if sch else None)
        rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return {"report": f"No GEOMETRY / GEOGRAPHY columns found in {cat}{'.' + sch if sch else ''}."}
    lines = [f"{cat}.{r[0]}.{r[1]}  ·  {r[2]} {r[3]}" for r in rows]
    return {"report": f"{len(rows)} spatial column(s):\n" + "\n".join(lines), "tables": [l.split("  ·  ")[0] for l in lines]}


def cmd_add_table(p):
    table = (p.get("table") or "").strip()
    src = quote_table(table)
    limit = min(int(p.get("limit") or 50000), MAX_ROWS)
    bbox = view_bbox() if p.get("in_view") else None
    return {"report": load(src, (p.get("geometry") or "").strip() or None, table.split(".")[-1], limit, bbox)}


def cmd_sql_layer(p):
    sql_text = (p.get("sql") or "").strip().rstrip(";")
    if not re.match(r"(?is)^\s*(select|with)\b", sql_text):
        raise RuntimeError("Only SELECT (or WITH … SELECT) statements.")
    limit = min(int(p.get("limit") or 50000), MAX_ROWS)
    return {"report": load(f"({sql_text})", (p.get("geometry") or "").strip() or None, p.get("name") or "Databricks SQL", limit)}


COMMANDS = {"connect": cmd_connect, "browse": cmd_browse, "add-table": cmd_add_table, "sql-layer": cmd_sql_layer}


def main():
    req = json.loads(sys.stdin.readline())
    try:
        fn = COMMANDS.get(req.get("id"))
        out = {"ok": fn(req.get("params") or {})} if fn else {"error": f"unknown command '{req.get('id')}'"}
    except Exception as e:      # report the reason in the RustGIS dialog / CLI
        out = {"error": f"{type(e).__name__}: {e}"}
    print(json.dumps(out), flush=True)


if __name__ == "__main__":
    main()
