"""Databricks SQL connector for RustGIS (process plugin, ABI 1).

RustGIS starts this program once per command and talks to it over stdin/stdout, one JSON object per line
(see RustGIS docs/plugins.md, "Process plugins"). Layers are added with the host request `host.addLayer`.

Every command works three ways: from the **Databricks panel** (values arrive in `request["panel"]["values"]`),
from the ribbon, and from scripts / rustgis-cli with explicit params, which win over the panel's values.

Sign-in goes through the Databricks SDK and a Databricks CLI profile (~/.databrickscfg), so OAuth (U2M or
M2M), PATs and Azure auth all work as they do for the CLI; the plugin never stores a token itself.
"""
import json
import math
import re
import sys
import time

MAX_ROWS = 200_000
DEFAULT_LIMIT = 50_000


# ---------------------------------------------------------------------------- RustGIS protocol
def call(op, params=None):
    print(json.dumps({"call": {"op": op, "params": params or {}}}), flush=True)
    reply = json.loads(sys.stdin.readline())
    if "error" in reply:
        raise RuntimeError(reply["error"])
    return reply["ok"]


def progress(text):
    print(json.dumps({"progress": text}), flush=True)


def settings():
    try:
        return call("host.settings") or {}
    except Exception:            # tools and some CLI runs have no settings
        return {}


def save_settings(values):
    try:
        call("host.setSettings", {"values": values})
    except Exception:
        pass


# ---------------------------------------------------------------------------- Databricks
def connect(profile, warehouse_id):
    from databricks import sql
    from databricks.sdk.core import Config

    cfg = Config(profile=profile)
    host = cfg.host.replace("https://", "").rstrip("/")
    conn = sql.connect(server_hostname=host, http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                       credentials_provider=lambda: cfg.authenticate, _user_agent_entry="rustgis-databricks-plugin/0.2")
    return conn, host


def current():
    s = settings()
    if not s.get("profile") or not s.get("warehouse_id"):
        raise RuntimeError("Not connected yet: open the Databricks panel (or run Connect) with a CLI profile and a warehouse ID.")
    return connect(s["profile"], s["warehouse_id"])


def q(ident):
    return f"`{ident.replace('`', '``')}`"


def quote_table(name):
    parts = [p.strip("` ") for p in name.split(".")]
    if len(parts) != 3 or not all(parts):
        raise RuntimeError(f"Choose a table: a three-part name, catalog.schema.table (got '{name}').")
    return ".".join(q(p) for p in parts)


def is_geo(dtype):
    return str(dtype).upper().startswith(("GEOMETRY", "GEOGRAPHY"))


def view_bbox():
    """Approximate lon/lat box of the map view, from its centre and scale (assumes a 1600 x 1000 px window)."""
    v = call("host.view")
    lon, lat = v["center"]
    mpp = float(v["scale"]) * 0.00028                      # metres per pixel at the standard 0.28 mm pixel
    dlat = 500 * mpp / 110_540
    dlon = 800 * mpp / (111_320 * max(math.cos(math.radians(lat)), 0.05))
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
    """source_sql is a quoted table or '(SELECT ...)'. Adds the layer and answers a one-line report."""
    t0 = time.time()
    conn, host = current()
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
        m = re.search(r"\((\d+)\)", dtype)
        srid = int(m.group(1)) if m else 0
        if dtype.startswith("GEOGRAPHY") or srid == 4326:
            geo4326 = g
        elif srid == 0:
            geo4326 = f"ST_SETSRID({g}, 4326)"     # SRID 0: treat as lon/lat, so ST_INTERSECTS with a 4326 box is allowed
        else:
            geo4326 = f"ST_TRANSFORM({g}, 4326)"
        where = ""
        if bbox:
            w, s_, e, n = (float(v) for v in bbox)
            box = f"ST_GEOMFROMTEXT('POLYGON(({w} {s_}, {e} {s_}, {e} {n}, {w} {n}, {w} {s_}))', 4326)"
            geom_for_filter = f"ST_GEOMFROMWKB(ST_ASWKB({g}), 4326)" if dtype.startswith("GEOGRAPHY") else geo4326
            where = f" WHERE ST_INTERSECTS({geom_for_filter}, {box})"
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
    return f"Added '{name}': {len(fc['features']):,} features in {time.time() - t0:.1f} s{clipped}."


# ---------------------------------------------------------------------------- panel helpers
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
    }


def status_text(s):
    if s.get("host"):
        return f"{s.get('user', '?')} on {s['host']}\nprofile {s.get('profile')} · warehouse {s.get('warehouse_id')}"
    return "Not connected. Enter a Databricks CLI profile and a SQL warehouse ID, then Connect."


def tree_nodes(rows):
    """rows: [(catalog, schema, table, column, type)] -> schema branches with their tables."""
    by_schema = {}
    for cat, sch, tbl, col, typ in rows:
        by_schema.setdefault(sch, {}).setdefault(f"{cat}.{sch}.{tbl}", []).append(f"{col} {typ}")
    return [{"id": f"schema:{sch}", "label": sch, "count": len(tables),
             "children": [{"id": full, "label": full.split(".")[-1] + "  ·  " + ", ".join(cols)} for full, cols in sorted(tables.items())]}
            for sch, tables in sorted(by_schema.items())]


def answer(report, items=None, values=None):
    panel = {"items": {"result": {"text": report}, **(items or {})}}
    if values:
        panel["values"] = values
    return {"report": report, "panel": panel}


def pick(params, panel, key, form=None, field=None):
    """An explicit param wins; otherwise the panel's widget value (or a field of a panel form)."""
    if key in params and params[key] not in (None, ""):
        return params[key]
    if form:
        return (panel.get(form) or {}).get(field or key)
    return panel.get(key)


# ---------------------------------------------------------------------------- commands
def cmd_panel_state(p, pv):
    s = settings()
    f = form_fields(s)
    items = {"status": {"text": status_text(s)}, "conn": {"fields": f["conn"]}, "opts": {"fields": f["opts"]},
             "sqlform": {"fields": f["sqlform"]}, "catalog": {"options": [{"id": c, "label": c} for c in s.get("catalogs") or []]}}
    if s.get("tree"):
        items["tables"] = {"nodes": s["tree"]}
    out = {"panel": {"items": items}}
    if s.get("catalog"):
        out["panel"]["values"] = {"catalog": s["catalog"]}
    return out


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
    default_cat = cat if cat in catalogs else None
    save_settings({"profile": profile, "warehouse_id": wh, "host": host, "user": user, "catalogs": catalogs,
                   "catalog": default_cat, "tree": None})
    s = {"profile": profile, "warehouse_id": wh, "host": host, "user": user}
    return answer(f"Connected to {host} as {user}. {len(catalogs)} catalogs; choose one to list its spatial tables.",
                  {"status": {"text": status_text(s)}, "catalog": {"options": [{"id": c, "label": c} for c in catalogs]},
                   "tables": {"nodes": []}},
                  {"catalog": default_cat} if default_cat else None)


def cmd_browse(p, pv):
    cat = (pick(p, pv, "catalog") or "").strip()
    sch = (p.get("schema") or "").strip()
    if not cat:
        raise RuntimeError("Choose a catalog.")
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
    nodes = tree_nodes(rows)
    save_settings({"catalog": cat, "tree": nodes})
    n_tables = sum(len(b["children"]) for b in nodes)
    report = (f"{n_tables} spatial table(s) in {len(nodes)} schema(s) of {cat}." if rows
              else f"No GEOMETRY / GEOGRAPHY columns in {cat}{'.' + sch if sch else ''}.")
    out = answer(report, {"tables": {"nodes": nodes}, "selected": {"text": "Choose a table in the tree."}})
    out["tables"] = [c["id"] for b in nodes for c in b["children"]]
    return out


def cmd_select_table(p, pv):
    t = p.get("table") or pv.get("tables") or ""
    if t.startswith("schema:") or t.count(".") != 2:
        return {"panel": {"items": {"selected": {"text": "Choose a table (not a schema) in the tree."}}}}
    return {"panel": {"items": {"selected": {"text": t}}}}


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
    in_view = p["in_view"] if "in_view" in p else bool(pv.get("in-view"))
    bbox = view_bbox() if in_view else None
    return answer(load(quote_table(t), _geom(p, pv), t.split(".")[-1] + (" (view)" if in_view else ""), _limit(p, pv), bbox))


def cmd_add_in_box(p, pv):
    t = _table(p, pv)
    bbox = p.get("bbox") or (pv.get("area") or {}).get("bbox")
    if not bbox or len(bbox) != 4:
        raise RuntimeError("Draw a box on the map with 'Add table inside a box…' (or pass bbox).")
    return answer(load(quote_table(t), _geom(p, pv), t.split(".")[-1] + " (box)", _limit(p, pv), bbox))


def cmd_sql_layer(p, pv):
    sql_text = (pick(p, pv, "sql", "sqlform") or "").strip().rstrip(";")
    if not re.match(r"(?is)^\s*(select|with)\b", sql_text):
        raise RuntimeError("Only SELECT (or WITH … SELECT) statements.")
    name = pick(p, pv, "name", "sqlform") or "Databricks SQL"
    return answer(load(f"({sql_text})", (p.get("geometry") or "").strip() or None, name, _limit(p, pv)))


COMMANDS = {"connect": cmd_connect, "browse": cmd_browse, "add-table": cmd_add_table, "add-in-box": cmd_add_in_box,
            "sql-layer": cmd_sql_layer, "select-table": cmd_select_table, "panel-state": cmd_panel_state}


def main():
    req = json.loads(sys.stdin.readline())
    try:
        fn = COMMANDS.get(req.get("id"))
        pv = (req.get("panel") or {}).get("values") or {}
        out = {"ok": fn(req.get("params") or {}, pv)} if fn else {"error": f"unknown command '{req.get('id')}'"}
    except Exception as e:      # shown in the panel's status bar / dialog / CLI
        out = {"error": f"{type(e).__name__}: {e}"}
    print(json.dumps(out), flush=True)


if __name__ == "__main__":
    main()
