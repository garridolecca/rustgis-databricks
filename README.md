# Databricks SQL for RustGIS

**Map your Databricks lakehouse in [RustGIS](https://rustgis.app), with no exports.** This plugin connects the fast,
open-source RustGIS desktop to a **Databricks SQL warehouse**. You can browse Unity Catalog for spatial tables, add
them as layers, or turn any `SELECT` into a layer, with `GEOMETRY` / `GEOGRAPHY` columns, H3 and Databricks
spatial SQL running on the warehouse.

![RustGIS showing 646 H3 cells of customer density and 14 store locations in Los Angeles, queried live from Databricks](docs/rustgis-la-h3.png)
*Los Angeles, queried live from a Databricks SQL warehouse: customers aggregated to H3 resolution-8 cells (one SQL
layer), 14 store points and three road-closure polygons (Unity Catalog tables), styled in RustGIS.*

## Why

GIS teams often copy warehouse data into shapefiles or GeoPackages to look at it. Those copies go stale, lose
governance, and break at scale. With this plugin the data stays in Databricks:

- **One governed copy:** every query runs on your SQL warehouse as your identity, so Unity Catalog grants, row
  filters and column masks decide what you see.
- **Push the work down:** filtering to the current view (`ST_INTERSECTS`), H3 aggregation and spatial joins run on
  the warehouse. RustGIS only draws the result.
- **Open stack end to end:** RustGIS is Apache-2.0, this plugin is Apache-2.0, and Databricks spatial SQL speaks
  GeoJSON, WKT and WKB.

It is the RustGIS counterpart to the
[Databricks DBSQL Connector for QGIS](https://plugins.qgis.org/plugins/databricks_dbsql_connector/).

## What you get

| Command | What it does |
|---|---|
| **Connect** | Pick a Databricks CLI profile (`~/.databrickscfg`) and a SQL warehouse ID. The plugin tests the connection (current user, default catalog) and remembers it. |
| **Browse Spatial Tables** | Lists every table in a catalog, or one of its schemas, that has a `GEOMETRY` or `GEOGRAPHY` column, read from `information_schema`. |
| **Add Table** | Adds `catalog.schema.table` as a layer, with all its columns as attributes. Tick **Only the current view** to fetch just what's on screen, for tables too big to load whole. |
| **SQL Layer** | Runs any `SELECT` (or `WITH … SELECT`) on the warehouse and adds the result as a layer: H3 aggregates, spatial joins, buffers, views. The first `GEOMETRY` / `GEOGRAPHY` column becomes the geometry. |

All four commands appear on the RustGIS **Plugins** tab with their own dialogs. They also work from `rustgis-cli`,
the RustGIS Python package and its MCP server, so scripts and AI agents can use them too.

**Geometry handling:**
- Geometry is returned as GeoJSON in WGS 84.
- `GEOMETRY` with a known SRID is reprojected with `ST_TRANSFORM`.
- `GEOMETRY` with SRID 0 is treated as longitude/latitude.
- `GEOGRAPHY` is used as is.
- Other complex types (arrays, structs, timestamps) come through as text attributes.

## Requirements

- **RustGIS:** 0.8.0 or later, desktop or command line. This is a process plugin, so it doesn't run in the web app.
- **Databricks:** a workspace with a SQL warehouse (serverless or pro). You need `CAN USE` on the warehouse, plus
  `USE CATALOG`, `USE SCHEMA` and `SELECT` on the tables.
- **Sign-in:** the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) logged in once, for example
  `databricks auth login --host https://<your-workspace> --profile my-workspace`.

## Install

1. **Download:** get `databricks-sql-<version>.rgplugin` from [Releases](https://github.com/garridolecca/rustgis-databricks/releases).
2. **Install:** in RustGIS, go to **Plugins → Plugin Manager → Settings → Install From File…** (or drop the file on the
   Plugin Manager). On the command line: `rustgis-cli plugins install databricks-sql-0.1.0.rgplugin`.
3. **Environment:** RustGIS builds the plugin's Python environment with uv: Python 3.12, `databricks-sql-connector`
   and `databricks-sdk`. This takes a minute, the first time only.
4. **Connect:** run **Plugins → Databricks SQL → Connect** with your profile and warehouse ID.

**Behind a PyPI mirror or a firewall?** RustGIS downloads uv from PyPI the first time. Point it at a uv that's
already configured for your mirror instead, then click **Prepare** in the Plugin Manager:
`export RUSTGIS_UV=$HOME/.local/bin/uv`, plus `launchctl setenv RUSTGIS_UV "$HOME/.local/bin/uv"` for the desktop
app on macOS.

## Authentication

The plugin doesn't ask for or store credentials. It uses the [Databricks SDK](https://docs.databricks.com/dev-tools/sdk-python.html)
with the profile you pick, so whatever that profile uses works: OAuth user-to-machine (browser sign-in), OAuth
machine-to-machine (service principal), personal access tokens and Azure sign-in. To switch workspace or identity,
run **Connect** with another profile.

## Command line

```bash
rustgis-cli run \
  --cmd 'plugin:databricks-sql/connect={"profile":"my-workspace","warehouse_id":"<warehouse-id>"}' \
  --cmd 'plugin:databricks-sql/browse={"catalog":"main","schema":"spatial_demo"}' \
  --cmd 'plugin:databricks-sql/add-table={"table":"main.spatial_demo.stores"}' \
  --cmd 'plugin:databricks-sql/sql-layer={"name":"Customers per H3 cell","sql":"SELECT cell, n, st_setsrid(st_geomfromwkt(h3_boundaryaswkt(cell)), 4326) AS geom FROM (SELECT h3_longlatash3(lon, lat, 8) AS cell, count(*) AS n FROM main.spatial_demo.customers GROUP BY 1)"}' \
  --cmd 'layer.symbology={"layer":"Customers per H3 cell","type":"graduated","field":"n","classes":5,"method":"quantile","ramp":"Viridis"}' \
  --save stores.rgis

rustgis-cli render stores.rgis stores.png --tiles 'https://tile.openstreetmap.org/{z}/{x}/{y}.png'
```

**SQL tips:**
- `GROUP BY` can't take a `GEOMETRY` column. Aggregate first, then build the geometry in an outer query, as above.
- Build points with `ST_POINT(lon, lat)` and polygons with `ST_GEOMFROMWKT(...)`.
- Tag the geometry with `ST_SETSRID(..., 4326)` when the coordinates are longitude/latitude.

## Tested

Tested on Nov 25, 2026 with RustGIS 0.8.0 on macOS (arm64), against a Databricks SQL warehouse:

| Test | Result |
|---|---|
| Connect (OAuth / profile sign-in) | ✓ |
| Browse a schema | 8 spatial tables |
| Add a point table | 14 features |
| H3 SQL layer | 646 hexagons |
| Add a polygon table | 3 features |
| **Only the current view**, around a neighbourhood | 197 of 1,207 points |

The whole sequence took about 11 seconds, including sign-in. Installing from the packaged `.rgplugin`, in a clean
plugins folder, was tested too.

## How it works

- **Plugin model:** a RustGIS [process plugin](https://github.com/opengeos/RustGIS/blob/main/docs/plugins.md#process-plugins).
  RustGIS starts `main.py` for each command and talks to it over standard input and output with JSON lines (ABI 1).
- **Running a command:** the plugin opens a `databricks-sql-connector` session with the SDK's credentials. It reads the
  source's schema with `DESCRIBE QUERY`, selects the attributes plus `ST_ASGEOJSON(geometry)`, and hands RustGIS a
  GeoJSON FeatureCollection through `host.addLayer`.
- **Settings:** the profile and warehouse are kept with `host.setSettings`.
- **Files:** `plugin.json` (manifest), `main.py` (the plugin, about 200 lines), `requirements.txt`.

## Limits and roadmap

- Rows per layer are capped at 200,000 (default 50,000). Use **Only the current view**, or aggregate in SQL, for
  bigger tables.
- **Only the current view** estimates the view's extent from the map's centre and scale.
- **Planned:** live layers that refresh as you pan, Genie (natural-language) queries, and writing edits back to
  Lakebase / PostGIS.

## Contributing

Issues and pull requests are welcome. To develop, add the folder in **Plugin Manager → Settings → Add Plugin Folder…**
(or `rustgis-cli plugins install ./`). Changes to `main.py` take effect on the next command.

## License

Apache-2.0. This is a community project, not an official Databricks or RustGIS product. Databricks, Unity Catalog
and Delta Lake are trademarks of Databricks, Inc.
