"""
osm_environment_builder.py
─────────────────────────────────────────────────────────
Queries the Overpass API (OpenStreetMap) to extract the
RAT (Routine Activity Theory) environment layers for Casablanca:

  1. GUARDIANSHIP LAYER (deters crime)
     - Police stations
     - Public surveillance cameras
     - Street lamps / lighting

  2. TARGET / ATTRACTOR LAYER (draws crime)
     - Banks & ATMs
     - Chic restaurants, cafes & luxury shops
     - Markets / souks
     - Transport hubs (tram stops, train stations)

Saves results to:
  output/osm_environment.json          — raw node data
  output/casablanca_environment_map.html — interactive Leaflet map

Usage:
  python osm_environment_builder.py            # full Casablanca
  python osm_environment_builder.py --anfa     # test on Anfa only
  python osm_environment_builder.py --no-map   # skip HTML generation
─────────────────────────────────────────────────────────
"""

import argparse
import json
import time
import requests
from pathlib import Path
from collections import defaultdict

# ─── Configuration ────────────────────────────────────────────────────────────

BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"

# Bounding boxes: [south_lat, west_lng, north_lat, east_lng]
BBOXES = {
    "casablanca":    [33.48, -7.75, 33.67, -7.48],   # Full Casablanca
    "anfa":          [33.56, -7.68, 33.62, -7.60],   # Anfa arrondissement (chic)
    "hay_mohammadi": [33.575, -7.61, 33.605, -7.565], # Hay Mohammadi (working-class)
}

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
SLEEP_BETWEEN_QUERIES = 2  # seconds — be polite to Overpass

# ─── OSM Query Definitions (RAT layers) ───────────────────────────────────────
#
# Each entry: (category, label, emoji, color, overpass_filter_body)
# The filter body uses Overpass QL syntax to select nodes/ways.

QUERY_LAYERS = [
    # ── Guardianship ──────────────────────────────────────────────
    {
        "id":      "police",
        "label":   "Police Station",
        "rat":     "guardianship",
        "emoji":   "🚓",
        "color":   "#3b82f6",   # blue
        "filters": [
            '["amenity"="police"]',
            '["name"~"commissariat|police|sûreté|gendarmerie|daraa",i]',
        ]
    },
    {
        "id":      "camera",
        "label":   "Security Camera",
        "rat":     "guardianship",
        "emoji":   "📹",
        "color":   "#8b5cf6",   # violet
        "filters": [
            '["surveillance"="public"]',
            '["surveillance"="outdoor"]',
            '["man_made"="surveillance"]',
        ]
    },
    {
        "id":      "lighting",
        "label":   "Street Lamp",
        "rat":     "guardianship",
        "emoji":   "💡",
        "color":   "#fbbf24",   # amber
        "filters": [
            '["highway"="street_lamp"]',
        ]
    },

    # ── High-Value Targets ────────────────────────────────────────
    {
        "id":      "bank",
        "label":   "Bank / ATM",
        "rat":     "target",
        "emoji":   "🏦",
        "color":   "#ef4444",   # red
        "filters": [
            '["amenity"="bank"]',
            '["amenity"="atm"]',
        ]
    },
    {
        "id":      "chic_cafe",
        "label":   "Café / Restaurant",
        "rat":     "target",
        "emoji":   "☕",
        "color":   "#f97316",   # orange
        "filters": [
            '["amenity"="cafe"]',
            '["amenity"="restaurant"]',
        ]
    },
    {
        "id":      "shop",
        "label":   "Shop / Boutique",
        "rat":     "target",
        "emoji":   "🛍️",
        "color":   "#ec4899",   # pink
        "filters": [
            '["shop"="clothes"]',
            '["shop"="jewelry"]',
            '["shop"="electronics"]',
            '["shop"="mobile_phone"]',
            '["shop"="department_store"]',
        ]
    },

    # ── Crime Generators ──────────────────────────────────────────
    {
        "id":      "transport",
        "label":   "Tram / Train Station",
        "rat":     "generator",
        "emoji":   "🚋",
        "color":   "#06b6d4",   # cyan
        "filters": [
            '["railway"="station"]',
            '["railway"="tram_stop"]',
            '["amenity"="bus_station"]',
        ]
    },
    {
        "id":      "market",
        "label":   "Market / Souk",
        "rat":     "attractor",
        "emoji":   "🛒",
        "color":   "#84cc16",   # lime
        "filters": [
            '["amenity"="marketplace"]',
            '["shop"="supermarket"]',
        ]
    },
]

# Zone colour for RAT category
RAT_COLORS = {
    "guardianship": "#22c55e",   # green  → deters crime
    "target":       "#ef4444",   # red    → attracts crime
    "generator":    "#f97316",   # orange → generates crime
    "attractor":    "#eab308",   # yellow → attracts offenders
}

# ─── Overpass Query Builder ────────────────────────────────────────────────────

def build_query(layer: dict, bbox: list) -> str:
    """Build an Overpass QL query string for a layer + bounding box."""
    s, w, n, e = bbox
    bbox_str = f"({s},{w},{n},{e})"
    parts = []
    for f in layer["filters"]:
        parts.append(f"node{f}{bbox_str};")
        parts.append(f"way{f}{bbox_str};")
    return f'[out:json][timeout:30];\n(\n  ' + '\n  '.join(parts) + '\n);\nout center;'


def query_overpass(query: str, retries: int = 3) -> dict | None:
    """Send query to Overpass API with retry on 429/504."""
    for attempt in range(retries):
        try:
            resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=60)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (429, 504):
                wait = 10 * (attempt + 1)
                print(f"    [Rate limit / timeout] Waiting {wait}s... (attempt {attempt+1})")
                time.sleep(wait)
            else:
                print(f"    [HTTP {resp.status_code}] {resp.text[:200]}")
                return None
        except requests.RequestException as e:
            print(f"    [Request error] {e}")
            time.sleep(5)
    return None


def extract_coords(element: dict) -> tuple[float, float] | None:
    """Extract lat/lng from a node or way-with-center."""
    if element.get("type") == "node":
        return element.get("lat"), element.get("lon")
    elif "center" in element:  # way
        return element["center"].get("lat"), element["center"].get("lon")
    return None


# ─── Main Extraction ──────────────────────────────────────────────────────────

def extract_environment(bbox: list, bbox_name: str) -> dict:
    """Query all RAT layers from Overpass and return structured data."""
    print(f"\n{'=' * 60}")
    print(f"  Extracting OSM environment for: {bbox_name.upper()}")
    print(f"  Bounding box: {bbox}")
    print(f"{'=' * 60}\n")

    results = {
        "bbox_name": bbox_name,
        "bbox":      bbox,
        "generated_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "layers": {}
    }
    totals = defaultdict(int)

    for layer in QUERY_LAYERS:
        lid = layer["id"]
        print(f"  [{layer['emoji']}] Querying {layer['label']} ({lid})...")
        query = build_query(layer, bbox)
        data = query_overpass(query)

        nodes = []
        if data and "elements" in data:
            for el in data["elements"]:
                coords = extract_coords(el)
                if not coords or None in coords:
                    continue
                lat, lng = coords
                tags = el.get("tags", {})
                nodes.append({
                    "lat":   lat,
                    "lng":   lng,
                    "name":  tags.get("name") or tags.get("name:ar") or tags.get("brand") or layer["label"],
                    "rat":   layer["rat"],
                    "layer": lid,
                    "tags":  {k: v for k, v in tags.items() if k in
                              ("name", "name:ar", "amenity", "shop", "railway",
                               "man_made", "surveillance", "operator", "brand")}
                })

        count = len(nodes)
        totals[layer["rat"]] += count
        results["layers"][lid] = {
            "label":  layer["label"],
            "emoji":  layer["emoji"],
            "rat":    layer["rat"],
            "color":  layer["color"],
            "count":  count,
            "nodes":  nodes,
        }
        print(f"      ✓ Found {count} features")
        time.sleep(SLEEP_BETWEEN_QUERIES)

    # Summary
    total_nodes = sum(v["count"] for v in results["layers"].values())
    print(f"\n{'─' * 60}")
    print(f"  SUMMARY:")
    for rat_type, cnt in sorted(totals.items()):
        print(f"    {rat_type:15s}: {cnt:4d} nodes")
    print(f"  TOTAL: {total_nodes} nodes")
    print(f"{'─' * 60}\n")

    results["totals"] = dict(totals)
    results["total_nodes"] = total_nodes
    return results


# ─── HTML Map Generator ───────────────────────────────────────────────────────

def generate_html_map(env_data: dict, out_path: Path):
    """Generate a standalone Leaflet.js HTML file overlaying the OSM data."""
    bbox = env_data["bbox"]
    center_lat = (bbox[0] + bbox[2]) / 2
    center_lng = (bbox[1] + bbox[3]) / 2
    bbox_name  = env_data["bbox_name"].upper()

    # Build JS layer data
    layers_js = json.dumps(env_data["layers"], ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="fr" dir="ltr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OSM Environment — {bbox_name} | Casablanca RAT Map</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=Cairo:wght@400;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    html, body {{ margin:0; padding:0; height:100%; font-family:'Inter',sans-serif; background:#0a0e1a; color:#f1f5f9; }}
    .app {{ display:flex; flex-direction:column; height:100%; }}

    header {{
      background:linear-gradient(135deg,#0a0e1a,#1a1035);
      border-bottom:1px solid rgba(255,255,255,0.08);
      padding:10px 20px;
      display:flex; align-items:center; justify-content:space-between;
      flex-shrink:0; position:relative;
    }}
    header::after {{
      content:''; position:absolute; bottom:0; left:0; right:0; height:1px;
      background:linear-gradient(90deg,transparent,#6366f1,#22c55e,#6366f1,transparent);
    }}
    .h-title {{ display:flex; align-items:center; gap:12px; }}
    .h-icon {{ width:38px; height:38px; background:linear-gradient(135deg,#6366f1,#8b5cf6);
      border-radius:10px; display:flex; align-items:center; justify-content:center;
      font-size:18px; box-shadow:0 0 16px rgba(99,102,241,0.4); }}
    h1 {{ font-size:15px; font-weight:700; margin:0; }}
    .h-sub {{ font-size:10px; color:#64748b; }}
    .h-pills {{ display:flex; gap:8px; }}
    .pill {{
      background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08);
      border-radius:20px; padding:4px 12px; font-size:11px; color:#94a3b8;
      display:flex; align-items:center; gap:5px;
    }}
    .pill strong {{ color:#f1f5f9; font-size:13px; }}

    .body-wrap {{ display:flex; flex:1; overflow:hidden; }}

    /* Sidebar */
    .sidebar {{
      width:280px; flex-shrink:0; background:#111827;
      border-right:1px solid rgba(255,255,255,0.07);
      display:flex; flex-direction:column; overflow:hidden;
    }}
    .sb-section {{ padding:12px 14px; border-bottom:1px solid rgba(255,255,255,0.06); }}
    .sb-title {{ font-size:9px; font-weight:700; text-transform:uppercase;
      letter-spacing:1.5px; color:#475569; margin-bottom:8px; }}

    .layer-item {{
      display:flex; align-items:center; gap:8px;
      padding:6px 8px; border-radius:8px; cursor:pointer;
      transition:background 0.15s; margin-bottom:3px; border:1px solid transparent;
    }}
    .layer-item:hover {{ background:rgba(255,255,255,0.04); }}
    .layer-item.active {{ background:rgba(255,255,255,0.06); border-color:rgba(255,255,255,0.1); }}
    .layer-dot {{ width:10px; height:10px; border-radius:50%; flex-shrink:0; }}
    .layer-name {{ flex:1; font-size:12px; color:#cbd5e1; }}
    .layer-count {{ font-size:13px; font-weight:700; }}

    .rat-section {{ margin-bottom:6px; }}
    .rat-label {{
      font-size:8px; font-weight:700; text-transform:uppercase;
      letter-spacing:1px; padding:3px 8px; border-radius:4px;
      margin-bottom:6px; display:inline-block;
    }}
    .rat-guardianship {{ background:rgba(34,197,94,0.12); color:#22c55e; }}
    .rat-target {{ background:rgba(239,68,68,0.12); color:#ef4444; }}
    .rat-generator {{ background:rgba(249,115,22,0.12); color:#f97316; }}
    .rat-attractor {{ background:rgba(234,179,8,0.12); color:#eab308; }}

    /* Map */
    #map {{
      flex:1; background:#0d1117;
      filter:brightness(0.97);
    }}
    .leaflet-popup-content-wrapper {{
      background:rgba(17,24,39,0.97)!important;
      border:1px solid rgba(255,255,255,0.1)!important;
      border-radius:12px!important;
      box-shadow:0 10px 40px rgba(0,0,0,0.8)!important;
      color:#f1f5f9!important;
      font-family:'Inter',sans-serif!important;
    }}
    .leaflet-popup-tip {{ background:rgba(17,24,39,0.97)!important; }}
    .popup-row {{ font-size:12px; margin:3px 0; color:#94a3b8; }}
    .popup-name {{ font-size:14px; font-weight:700; color:#f1f5f9; margin-bottom:6px; }}
    .popup-badge {{
      display:inline-block; padding:2px 8px; border-radius:12px;
      font-size:10px; font-weight:700; margin-top:4px;
    }}
  </style>
</head>
<body>
<div class="app">
  <header>
    <div class="h-title">
      <div class="h-icon">🏙</div>
      <div>
        <h1>OSM Environment Layer — {bbox_name}</h1>
        <div class="h-sub">Routine Activity Theory Infrastructure · Casablanca Crime Map</div>
      </div>
    </div>
    <div class="h-pills">
      <div class="pill">🛡 Guardianship <strong id="cnt-guardianship">0</strong></div>
      <div class="pill">🎯 Targets <strong id="cnt-target">0</strong></div>
      <div class="pill">⚡ Generators <strong id="cnt-generator">0</strong></div>
      <div class="pill">🛒 Attractors <strong id="cnt-attractor">0</strong></div>
    </div>
  </header>

  <div class="body-wrap">
    <div class="sidebar">
      <div class="sb-section">
        <div class="sb-title">Layers Toggle</div>
        <div id="layer-list"></div>
      </div>
      <div class="sb-section" style="flex:1; overflow-y:auto;">
        <div class="sb-title">Legend · RAT Framework</div>
        <div style="font-size:11px; color:#64748b; line-height:1.7;">
          <div><span style="color:#22c55e">🟢 Guardianship</span> — Deters crime (police, cameras, lights)</div>
          <div><span style="color:#ef4444">🔴 Target</span> — Attracts crime (banks, luxury stores)</div>
          <div><span style="color:#f97316">🟠 Generator</span> — Generates crime (transport hubs)</div>
          <div><span style="color:#eab308">🟡 Attractor</span> — Draws offenders (markets, souks)</div>
        </div>
      </div>
    </div>
    <div id="map"></div>
  </div>
</div>

<script>
const ENV = {layers_js};

// ─── Map Init ────────────────────────────────────────────────────────────────
const map = L.map('map', {{zoomControl:true}}).setView([{center_lat}, {center_lng}], 14);

const cartoLayer = L.tileLayer(
  'https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png',
  {{attribution:'© OSM · CartoDB', maxZoom:19, subdomains:'abcd', errorTileUrl:''}}
);
let tilesFailed = false;
cartoLayer.on('tileerror', () => {{
  if (!tilesFailed) {{
    tilesFailed = true;
    map.removeLayer(cartoLayer);
    const osm = L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png',
      {{attribution:'© OpenStreetMap', maxZoom:19}});
    osm.on('tileload', e => {{
      e.tile.style.filter = 'invert(1) hue-rotate(180deg) saturate(0.5) brightness(0.75)';
    }});
    osm.addTo(map);
  }}
}});
cartoLayer.addTo(map);

// ─── Layers ──────────────────────────────────────────────────────────────────
const leafletLayers = {{}};
const RAT_COLORS = {{
  guardianship:'#22c55e', target:'#ef4444', generator:'#f97316', attractor:'#eab308'
}};

function makeBadge(rat) {{
  const colors = {{ guardianship:'#22c55e', target:'#ef4444', generator:'#f97316', attractor:'#eab308' }};
  const labels = {{ guardianship:'Guardianship', target:'Target', generator:'Generator', attractor:'Attractor' }};
  return `<span class="popup-badge" style="background:${{colors[rat]}}22;color:${{colors[rat]}};border:1px solid ${{colors[rat]}}44">${{labels[rat]}}</span>`;
}}

const ratTotals = {{guardianship:0, target:0, generator:0, attractor:0}};

Object.entries(ENV).forEach(([lid, ld]) => {{
  const layerGroup = L.layerGroup();
  ld.nodes.forEach(n => {{
    const marker = L.circleMarker([n.lat, n.lng], {{
      radius:5,
      color: ld.color,
      fillColor: ld.color,
      fillOpacity: 0.85,
      weight:1.5
    }});
    marker.bindPopup(`
      <div class="popup-name">${{ld.emoji}} ${{n.name}}</div>
      <div class="popup-row">📍 ${{n.lat.toFixed(5)}}, ${{n.lng.toFixed(5)}}</div>
      ${{makeBadge(n.rat)}}
    `);
    marker.addTo(layerGroup);
  }});
  leafletLayers[lid] = layerGroup;
  layerGroup.addTo(map);
  ratTotals[ld.rat] = (ratTotals[ld.rat] || 0) + ld.count;
}});

// Update header counts
Object.entries(ratTotals).forEach(([rat, cnt]) => {{
  const el = document.getElementById('cnt-' + rat);
  if (el) el.textContent = cnt;
}});

// ─── Sidebar Layer Toggles ───────────────────────────────────────────────────
// Group by RAT
const ratGroups = {{}};
Object.entries(ENV).forEach(([lid, ld]) => {{
  if (!ratGroups[ld.rat]) ratGroups[ld.rat] = [];
  ratGroups[ld.rat].push({{lid, ...ld}});
}});

const ratOrder = ['guardianship', 'target', 'generator', 'attractor'];
const ll = document.getElementById('layer-list');

ratOrder.forEach(rat => {{
  if (!ratGroups[rat]) return;
  const div = document.createElement('div');
  div.className = 'rat-section';
  const badge = document.createElement('div');
  badge.className = `rat-label rat-${{rat}}`;
  badge.textContent = rat.charAt(0).toUpperCase() + rat.slice(1);
  div.appendChild(badge);

  ratGroups[rat].forEach(ld => {{
    const item = document.createElement('div');
    item.className = 'layer-item active';
    item.id = 'li-' + ld.lid;
    item.innerHTML = `
      <div class="layer-dot" style="background:${{ld.color}}; box-shadow:0 0 6px ${{ld.color}}44"></div>
      <div class="layer-name">${{ld.emoji}} ${{ld.label}}</div>
      <div class="layer-count" style="color:${{ld.color}}">${{ld.count}}</div>`;
    item.onclick = () => {{
      const lg = leafletLayers[ld.lid];
      if (map.hasLayer(lg)) {{ map.removeLayer(lg); item.classList.remove('active'); }}
      else {{ lg.addTo(map); item.classList.add('active'); }}
    }};
    div.appendChild(item);
  }});
  ll.appendChild(div);
}});
</script>
</body>
</html>"""

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✓ HTML map written to: {out_path}")


# ─── Entry Point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="OSM Environment Builder for Casablanca RAT map")
    parser.add_argument("--anfa",          action="store_true", help="Query only Anfa bounding box (fast test)")
    parser.add_argument("--hay-mohammadi", action="store_true", help="Query only Hay Mohammadi bounding box")
    parser.add_argument("--no-map",        action="store_true", help="Skip HTML map generation")
    args = parser.parse_args()

    if args.anfa:
        bbox_name = "anfa"
    elif getattr(args, "hay_mohammadi", False):
        bbox_name = "hay_mohammadi"
    else:
        bbox_name = "casablanca"
    bbox = BBOXES[bbox_name]

    # Extract
    env_data = extract_environment(bbox, bbox_name)

    # Save JSON
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / f"osm_environment_{bbox_name}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(env_data, f, ensure_ascii=False, indent=2)
    print(f"✓ JSON saved to {json_path}")

    # Generate HTML
    if not args.no_map:
        suffix = f"_{bbox_name}" if args.anfa else ""
        html_path = OUTPUT_DIR / f"casablanca_environment_map{suffix}.html"
        generate_html_map(env_data, html_path)
        print(f"\n✅ Done! Open this file in your browser:\n   {html_path}")


if __name__ == "__main__":
    main()
