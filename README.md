# optimist-data

Interactive map of Austrian OTA sites and power substations.

## What's in here

| File | Description |
|------|-------------|
| `map.html` | Self-contained interactive map (open in any browser) |
| `data/Copia Sitelist draft 20260313.xlsx` | OTA site list with coordinates |

## Map features

- **1,514 OTA sites** colour-coded by region (Wien, Niederösterreich, Steiermark, …)
- **1,580 Austrian substations** from OpenStreetMap (amber diamonds)
- **Proximity filter** — colour sites by distance to nearest substation (green → yellow → red) and filter by max distance
- **Region filter** — toggle regions on/off
- **Search** — filter by city, street, Site ID or postcode
- Clustering at low zoom levels; click any marker for details

## Data sources

- OTA site list: internal (`data/` folder)
- Substations: [OpenStreetMap via Overpass API](https://overpass-api.de) — `node[power=substation]` within Austria

## Usage

Just open `map.html` in a browser. No server or build step required.
