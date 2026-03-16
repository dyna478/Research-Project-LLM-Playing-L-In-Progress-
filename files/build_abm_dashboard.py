"""
build_abm_dashboard.py
─────────────────────────────────────────────────────────
Reads all three data layers and generates:
  output/abm_dashboard.html  — The Unified ABM Dashboard

Data Layers:
  1. Crime Layer       → from casablanca_crime_map.html (NEIGHBORHOOD_DATA embedded)
  2. OSM Environment   → output/osm_environment_casablanca.json
  3. Socio-Economic    → output/rent_data.json

Usage:
  python build_abm_dashboard.py
"""

import json
from pathlib import Path

BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
OSM_JSON   = OUTPUT_DIR / "osm_environment_casablanca.json"
RENT_JSON  = OUTPUT_DIR / "rent_data.json"
SIM_JSON   = OUTPUT_DIR / "simulated_crimes.json"
OUT_HTML   = OUTPUT_DIR / "abm_dashboard.html"

# ── Crime data (extracted from casablanca_crime_map.html) ─────────────────────
CRIME_DATA = {
    "عين السبع":        {"total":45,"articles":37,"youtube":8, "zone":"red",    "lat":33.5731,"lng":-7.5800},
    "أنفا":             {"total":43,"articles":38,"youtube":5, "zone":"red",    "lat":33.5853,"lng":-7.6498},
    "الحي الحسني":      {"total":38,"articles":31,"youtube":7, "zone":"red",    "lat":33.5482,"lng":-7.6340},
    "عين الشق":         {"total":30,"articles":29,"youtube":1, "zone":"red",    "lat":33.5608,"lng":-7.5370},
    "مولاي رشيد":       {"total":25,"articles":21,"youtube":4, "zone":"red",    "lat":33.5438,"lng":-7.5937},
    "سيدي البرنوصي":    {"total":22,"articles":22,"youtube":0, "zone":"red",    "lat":33.6033,"lng":-7.6203},
    "الفداء":           {"total":21,"articles":20,"youtube":1, "zone":"red",    "lat":33.5943,"lng":-7.6112},
    "المدينة القديمة":   {"total":15,"articles":11,"youtube":4, "zone":"red",    "lat":33.6013,"lng":-7.6238},
    "البرنوصي":         {"total":14,"articles":8, "youtube":6, "zone":"orange", "lat":33.6100,"lng":-7.6300},
    "الحسين":           {"total":8, "articles":8, "youtube":0, "zone":"orange", "lat":33.5750,"lng":-7.6150},
    "الحي المحمدي":     {"total":8, "articles":4, "youtube":4, "zone":"orange", "lat":33.5970,"lng":-7.5830},
    "الرحمة":           {"total":7, "articles":6, "youtube":1, "zone":"orange", "lat":33.5340,"lng":-7.6720},
    "المعاريف":         {"total":5, "articles":5, "youtube":0, "zone":"orange", "lat":33.5730,"lng":-7.6540},
    "التضامن":          {"total":5, "articles":5, "youtube":0, "zone":"orange", "lat":33.5210,"lng":-7.6650},
    "بن مسيك":          {"total":5, "articles":5, "youtube":0, "zone":"orange", "lat":33.5520,"lng":-7.6000},
    "المحمدية":         {"total":4, "articles":4, "youtube":0, "zone":"yellow", "lat":33.7272,"lng":-7.3893},
    "النخيل":           {"total":4, "articles":4, "youtube":0, "zone":"yellow", "lat":33.5800,"lng":-7.5200},
    "النصر":            {"total":4, "articles":4, "youtube":0, "zone":"yellow", "lat":33.5670,"lng":-7.6620},
    "العالية":          {"total":3, "articles":0, "youtube":3, "zone":"yellow", "lat":33.5270,"lng":-7.5900},
    "ليساسفة":          {"total":3, "articles":2, "youtube":1, "zone":"yellow", "lat":33.6050,"lng":-7.6180},
    "درب عمر":          {"total":3, "articles":0, "youtube":3, "zone":"yellow", "lat":33.5960,"lng":-7.6070},
    "درب السلطان":      {"total":1, "articles":1, "youtube":0, "zone":"green",  "lat":33.5897,"lng":-7.6250},
    "سيدي مومن":        {"total":2, "articles":1, "youtube":1, "zone":"yellow", "lat":33.5870,"lng":-7.5600},
}

# ── Rent / socio-economic data ─────────────────────────────────────────────────
RENT_LOOKUP = {
    "Hay Mohammadi":    {"lat":33.590, "lng":-7.587, "median":2100,  "cat":1, "label":"Très défavorisé", "color":"#ef4444"},
    "Sidi Moumen":      {"lat":33.598, "lng":-7.554, "median":None,  "cat":1, "label":"Très défavorisé", "color":"#ef4444"},
    "Moulay Rachid":    {"lat":33.543, "lng":-7.591, "median":None,  "cat":1, "label":"Très défavorisé", "color":"#ef4444"},
    "Ben Msick":        {"lat":33.552, "lng":-7.570, "median":6000,  "cat":2, "label":"Défavorisé",      "color":"#f97316"},
    "Derb Sultan":      {"lat":33.584, "lng":-7.601, "median":None,  "cat":1, "label":"Très défavorisé", "color":"#ef4444"},
    "Ain Sebaa":        {"lat":33.599, "lng":-7.525, "median":4050,  "cat":2, "label":"Défavorisé",      "color":"#f97316"},
    "Maarif":           {"lat":33.569, "lng":-7.639, "median":8500,  "cat":3, "label":"Classe moyenne",  "color":"#eab308"},
    "Hay Hassani":      {"lat":33.549, "lng":-7.634, "median":11900, "cat":4, "label":"Classe aisée",    "color":"#22c55e"},
    "Anfa":             {"lat":33.585, "lng":-7.650, "median":11000, "cat":4, "label":"Classe aisée",    "color":"#22c55e"},
    "Racine":           {"lat":33.580, "lng":-7.648, "median":None,  "cat":4, "label":"Classe aisée",    "color":"#22c55e"},
    "Gauthier":         {"lat":33.577, "lng":-7.636, "median":None,  "cat":4, "label":"Classe aisée",    "color":"#22c55e"},
    "Ain Diab":         {"lat":33.593, "lng":-7.697, "median":None,  "cat":5, "label":"Aisé / Chic",    "color":"#3b82f6"},
    "Ain Chock":        {"lat":33.557, "lng":-7.538, "median":None,  "cat":1, "label":"Très défavorisé", "color":"#ef4444"},
    "Bourgogne":        {"lat":33.582, "lng":-7.637, "median":None,  "cat":4, "label":"Classe aisée",    "color":"#22c55e"},
}

def load_osm():
    with open(OSM_JSON, encoding="utf-8") as f:
        data = json.load(f)
    # The JSON wraps layers inside a 'layers' key
    return data.get("layers", data)

def compact_nodes(nodes, max_nodes=400):
    """Limit nodes for dashboard performance while keeping coverage."""
    if len(nodes) <= max_nodes:
        return nodes
    step = len(nodes) / max_nodes
    return [nodes[int(i * step)] for i in range(max_nodes)]

# Normalise OSM layer keys to the sidebar IDs used in HTML
KEY_MAP = {
    "chic_cafe": "cafe",
    "shop":      "cafe",   # merge shops into cafe layer
}

def load_simulated_crimes():
    if not SIM_JSON.exists():
        print("  [!] simulated_crimes.json not found — run abm_engine.py first")
        return []
    with open(SIM_JSON, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("crimes", [])

def build_dashboard():
    print("Loading OSM data…")
    osm = load_osm()
    print("Loading simulated crimes…")
    sim_crimes = load_simulated_crimes()

    # Prepare compact layer subsets for the map
    layers_js = {}
    for raw_key, meta in osm.items():
        if not isinstance(meta, dict) or "nodes" not in meta:
            continue
        key = KEY_MAP.get(raw_key, raw_key)
        nodes = meta.get("nodes", [])
        compact = compact_nodes(nodes, 300)
        layers_js[key] = {
            "label": meta["label"],
            "emoji": meta["emoji"],
            "rat":   meta["rat"],
            "color": meta["color"],
            "count": meta["count"],
            "nodes": [{"lat": n["lat"], "lng": n["lng"], "name": n.get("name", "")} for n in compact]
        }

    crime_js   = json.dumps(CRIME_DATA, ensure_ascii=False, indent=2)
    rent_js    = json.dumps(RENT_LOOKUP, ensure_ascii=False, indent=2)
    osm_js     = json.dumps(layers_js, ensure_ascii=False)
    sim_js     = json.dumps(sim_crimes, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="fr" dir="ltr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ABM Dashboard — Casablanca Crime Simulation</title>
  <meta name="description" content="Unified Routine Activity Theory environment for the Casablanca ABM simulation">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;900&family=Cairo:wght@400;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
  <style>
    :root {{
      --bg: #080c18;
      --bg2: #0f1628;
      --bg3: #141c35;
      --border: rgba(255,255,255,0.08);
      --txt: #f1f5f9;
      --muted: #64748b;
      --accent: #6366f1;
      --red:   #ef4444;
      --orange:#f97316;
      --yellow:#eab308;
      --green: #22c55e;
      --blue:  #3b82f6;
      --purple:#8b5cf6;
    }}
    *, *::before, *::after {{ box-sizing: border-box; margin:0; padding:0; }}
    html, body {{ height:100%; overflow:hidden; font-family:'Inter','Cairo',sans-serif; background:var(--bg); color:var(--txt); }}
    body {{ display:flex; flex-direction:column; }}

    /* ── HEADER ─────────────────────────────────────────── */
    header {{
      background: linear-gradient(135deg, #080c18 0%, #110a28 50%, #080c18 100%);
      border-bottom: 1px solid var(--border);
      padding: 10px 20px;
      display: flex; align-items: center; justify-content: space-between;
      flex-shrink: 0; position: relative; z-index: 1000;
    }}
    header::after {{
      content:''; position:absolute; bottom:0; left:0; right:0; height:1px;
      background: linear-gradient(90deg, transparent, var(--accent), var(--red), var(--green), var(--accent), transparent);
    }}
    .h-logo {{ display:flex; align-items:center; gap:12px; }}
    .h-icon {{
      width:40px; height:40px;
      background: linear-gradient(135deg, var(--accent), #7c3aed);
      border-radius:10px; display:flex; align-items:center; justify-content:center;
      font-size:20px; box-shadow: 0 0 20px rgba(99,102,241,0.4);
    }}
    .h-title {{ font-size:15px; font-weight:700; }}
    .h-sub {{ font-size:10px; color:var(--muted); }}
    .h-pills {{ display:flex; gap:8px; }}
    .pill {{
      background: rgba(255,255,255,0.04); border:1px solid var(--border);
      border-radius:20px; padding:4px 12px; font-size:11px; color:#94a3b8;
      display:flex; align-items:center; gap:5px;
    }}
    .pill strong {{ color:var(--txt); font-size:13px; font-weight:700; }}

    /* ── LAYOUT ─────────────────────────────────────────── */
    .body-wrap {{ display:flex; flex:1; overflow:hidden; }}
    #map {{ flex:1; background:#0d1117; }}

    /* ── SIDEBAR ─────────────────────────────────────────── */
    .sidebar {{
      width:290px; flex-shrink:0; background:var(--bg2);
      border-right:1px solid var(--border);
      display:flex; flex-direction:column; overflow:hidden; z-index:100;
    }}
    .sb-scroll {{ flex:1; overflow-y:auto; }}
    .sb-scroll::-webkit-scrollbar {{ width:4px; }}
    .sb-scroll::-webkit-scrollbar-thumb {{ background:rgba(255,255,255,0.1); border-radius:2px; }}
    .sb-section {{ padding:12px 14px; border-bottom:1px solid var(--border); }}
    .sb-label {{
      font-size:9px; font-weight:700; text-transform:uppercase;
      letter-spacing:1.5px; color:var(--muted); margin-bottom:8px;
    }}

    /* Layer items */
    .layer-group-title {{
      font-size:9px; color:var(--muted); text-transform:uppercase;
      letter-spacing:1px; padding:6px 0 3px; font-weight:600;
    }}
    .layer-item {{
      display:flex; align-items:center; gap:8px;
      padding:6px 8px; border-radius:8px; cursor:pointer;
      transition:background 0.15s; margin-bottom:2px; border:1px solid transparent;
      user-select:none;
    }}
    .layer-item:hover {{ background:rgba(255,255,255,0.04); }}
    .layer-item.active {{ background:rgba(255,255,255,0.07); border-color:rgba(255,255,255,0.1); }}
    .layer-dot {{ width:10px; height:10px; border-radius:50%; flex-shrink:0; }}
    .layer-name {{ flex:1; font-size:12px; color:#cbd5e1; }}
    .layer-count {{ font-size:12px; font-weight:700; }}
    .layer-check {{ font-size:13px; color:var(--muted); transition:color 0.15s; }}
    .layer-item.active .layer-check {{ color:var(--green); }}

    /* Wealth legend */
    .wealth-bar {{
      display:flex; flex-direction:column; gap:3px; margin-top:4px;
    }}
    .wealth-row {{
      display:flex; align-items:center; gap:8px;
      font-size:11px; padding:3px 0;
    }}
    .wealth-swatch {{ width:12px; height:12px; border-radius:3px; flex-shrink:0; }}

    /* Stats panel */
    .stat-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:6px; }}
    .stat-card {{
      background:var(--bg3); border:1px solid var(--border); border-radius:8px;
      padding:8px 10px;
    }}
    .stat-val {{ font-size:18px; font-weight:700; }}
    .stat-lbl {{ font-size:9px; color:var(--muted); text-transform:uppercase; margin-top:1px; }}

    /* RAT legend pills */
    .rat-pill {{
      display:inline-block; padding:2px 7px; border-radius:4px;
      font-size:9px; font-weight:700; margin-bottom:4px; margin-right:3px;
    }}

    /* Leaflet popup override */
    .leaflet-popup-content-wrapper {{
      background:rgba(15,22,40,0.97)!important;
      border:1px solid rgba(255,255,255,0.12)!important;
      border-radius:12px!important; color:var(--txt)!important;
      font-family:'Inter',sans-serif!important;
      box-shadow:0 10px 40px rgba(0,0,0,0.8)!important;
    }}
    .leaflet-popup-tip {{ background:rgba(15,22,40,0.97)!important; }}
    .popup-name {{ font-size:14px; font-weight:700; margin-bottom:6px; }}
    .popup-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:5px; }}
    .popup-cell {{ background:rgba(255,255,255,0.05); border-radius:6px; padding:5px 7px; }}
    .popup-cell-val {{ font-size:13px; font-weight:700; }}
    .popup-cell-lbl {{ font-size:9px; color:var(--muted); }}

    /* Map floating controls */
    .map-fab {{
      position:absolute; bottom:20px; right:20px; z-index:500;
      display:flex; flex-direction:column; gap:6px;
    }}
    .fab-btn {{
      background:rgba(15,22,40,0.92); border:1px solid rgba(255,255,255,0.12);
      border-radius:10px; padding:8px 14px; font-size:12px; color:#94a3b8;
      cursor:pointer; backdrop-filter:blur(10px); transition:all 0.2s;
      display:flex; align-items:center; gap:6px; white-space:nowrap;
    }}
    .fab-btn:hover {{ background:rgba(30,41,59,.95); color:var(--txt); border-color:var(--accent); }}
    .fab-btn.on {{ background:rgba(99,102,241,.15); color:var(--accent); border-color:var(--accent); }}

    /* Crime heatmap legend */
    .heat-legend {{
      position:absolute; bottom:20px; left:20px; z-index:500;
      background:rgba(15,22,40,0.9); border:1px solid rgba(255,255,255,0.1);
      border-radius:10px; padding:10px 14px; backdrop-filter:blur(10px);
    }}
    .heat-legend .hl-title {{ font-size:9px; text-transform:uppercase; letter-spacing:1px; color:var(--muted); margin-bottom:5px; }}
    .heat-grad {{ height:8px; border-radius:4px; background:linear-gradient(90deg,#1a1a2e,#0f3460,#f97316,#ef4444,#fff); }}
    .heat-labels {{ display:flex; justify-content:space-between; font-size:9px; color:var(--muted); margin-top:3px; }}
  </style>
</head>
<body>

<header>
  <div class="h-logo">
    <div class="h-icon">🧠</div>
    <div>
      <div class="h-title">Casablanca ABM Environment</div>
      <div class="h-sub">Routine Activity Theory · Crime + Environment + Socio-Economic Layers</div>
    </div>
  </div>
  <div class="h-pills">
    <div class="pill">🚨 Crime <strong id="cnt-crime">0</strong></div>
    <div class="pill">🛡 Guardianship <strong id="cnt-guard">0</strong></div>
    <div class="pill">🎯 Targets <strong id="cnt-target">0</strong></div>
    <div class="pill">💰 Rent Zones <strong id="cnt-rent">0</strong></div>
    <div class="pill">🤖 Simulated <strong id="cnt-sim">0</strong></div>
  </div>
</header>

<div class="body-wrap">
  <!-- ── SIDEBAR ──────────────────────────────────────── -->
  <div class="sidebar">
    <div class="sb-scroll">
      <!-- Stats -->
      <div class="sb-section">
        <div class="sb-label">ABM Summary</div>
        <div class="stat-grid">
          <div class="stat-card">
            <div class="stat-val" id="s-incidents" style="color:var(--red)">0</div>
            <div class="stat-lbl">Crime Incidents</div>
          </div>
          <div class="stat-card">
            <div class="stat-val" id="s-guard" style="color:var(--green)">0</div>
            <div class="stat-lbl">Guard Nodes</div>
          </div>
          <div class="stat-card">
            <div class="stat-val" id="s-targets" style="color:var(--orange)">0</div>
            <div class="stat-lbl">Target Nodes</div>
          </div>
          <div class="stat-card">
            <div class="stat-val" id="s-sim" style="color:var(--purple)">0</div>
            <div class="stat-lbl">🤖 Simulated</div>
          </div>
        </div>
      </div>

      <!-- Crime Layer -->
      <div class="sb-section">
        <div class="sb-label">🚨 Crime Layer</div>
        <div class="layer-item active" id="li-crime-heat" onclick="toggleLayer('crime-heat')">
          <div class="layer-dot" style="background:var(--red)"></div>
          <div class="layer-name">Crime Heatmap</div>
          <div class="layer-check" id="chk-crime-heat">✓</div>
        </div>
        <div class="layer-item active" id="li-crime-circles" onclick="toggleLayer('crime-circles')">
          <div class="layer-dot" style="background:var(--orange)"></div>
          <div class="layer-name">Crime Circles</div>
          <div class="layer-check" id="chk-crime-circles">✓</div>
        </div>
      </div>

      <!-- Socio-economic Layer -->
      <div class="sb-section">
        <div class="sb-label">💰 Socio-Economic Layer (Rent)</div>
        <div class="layer-item active" id="li-wealth" onclick="toggleLayer('wealth')">
          <div class="layer-dot" style="background:linear-gradient(135deg,var(--blue),var(--green))"></div>
          <div class="layer-name">Wealth Zones</div>
          <div class="layer-check" id="chk-wealth">✓</div>
        </div>
        <div class="wealth-bar">
          <div class="wealth-row"><div class="wealth-swatch" style="background:#3b82f6"></div><span>Aisé / Chic (Cat 5)</span></div>
          <div class="wealth-row"><div class="wealth-swatch" style="background:#22c55e"></div><span>Classe aisée (Cat 4)</span></div>
          <div class="wealth-row"><div class="wealth-swatch" style="background:#eab308"></div><span>Classe moyenne (Cat 3)</span></div>
          <div class="wealth-row"><div class="wealth-swatch" style="background:#f97316"></div><span>Défavorisé (Cat 2)</span></div>
          <div class="wealth-row"><div class="wealth-swatch" style="background:#ef4444"></div><span>Très défavorisé (Cat 1)</span></div>
        </div>
      </div>

      <!-- Guardianship Layer -->
      <div class="sb-section">
        <div class="sb-label">🛡 Guardianship (RAT)</div>
        <div class="layer-item active" id="li-police" onclick="toggleLayer('police')">
          <div class="layer-dot" style="background:#3b82f6"></div>
          <div class="layer-name">🚓 Police Stations</div>
          <div class="layer-count" id="cnt-police" style="color:#3b82f6">0</div>
          <div class="layer-check" id="chk-police">✓</div>
        </div>
        <div class="layer-item active" id="li-camera" onclick="toggleLayer('camera')">
          <div class="layer-dot" style="background:#8b5cf6"></div>
          <div class="layer-name">📹 Cameras</div>
          <div class="layer-count" id="cnt-camera" style="color:#8b5cf6">0</div>
          <div class="layer-check" id="chk-camera">✓</div>
        </div>
      </div>

      <!-- Target Layer -->
      <div class="sb-section">
        <div class="sb-label">🎯 Targets (RAT)</div>
        <div class="layer-item active" id="li-bank" onclick="toggleLayer('bank')">
          <div class="layer-dot" style="background:#ef4444"></div>
          <div class="layer-name">🏦 Banks / ATMs</div>
          <div class="layer-count" id="cnt-bank" style="color:#ef4444">0</div>
          <div class="layer-check" id="chk-bank">✓</div>
        </div>
        <div class="layer-item" id="li-cafe" onclick="toggleLayer('cafe')">
          <div class="layer-dot" style="background:#f97316"></div>
          <div class="layer-name">☕ Cafes / Restaurants</div>
          <div class="layer-count" id="cnt-cafe" style="color:#f97316">0</div>
          <div class="layer-check" id="chk-cafe">○</div>
        </div>
      </div>

      <!-- Generator Layer -->
      <div class="sb-section">
        <div class="sb-label">⚡ Generators / Attractors (RAT)</div>
        <div class="layer-item" id="li-transport" onclick="toggleLayer('transport')">
          <div class="layer-dot" style="background:#f97316"></div>
          <div class="layer-name">🚋 Transport Hubs</div>
          <div class="layer-count" id="cnt-transport" style="color:#f97316">0</div>
          <div class="layer-check" id="chk-transport">○</div>
        </div>
        <div class="layer-item" id="li-market" onclick="toggleLayer('market')">
          <div class="layer-dot" style="background:#eab308"></div>
          <div class="layer-name">🛒 Markets / Souks</div>
          <div class="layer-count" id="cnt-market" style="color:#eab308">0</div>
          <div class="layer-check" id="chk-market">○</div>
        </div>
      </div>

      <!-- Simulated Crime Layer -->
      <div class="sb-section">
        <div class="sb-label">🤖 ABM Simulated Crimes</div>
        <div class="layer-item active" id="li-sim-heat" onclick="toggleLayer('sim-heat')">
          <div class="layer-dot" style="background:var(--purple)"></div>
          <div class="layer-name">Simulated Heatmap</div>
          <div class="layer-check" id="chk-sim-heat">✓</div>
        </div>
        <div class="layer-item active" id="li-sim-dots" onclick="toggleLayer('sim-dots')">
          <div class="layer-dot" style="background:#a78bfa"></div>
          <div class="layer-name">Simulated Dots</div>
          <div class="layer-count" id="cnt-sim-dots" style="color:#a78bfa">0</div>
          <div class="layer-check" id="chk-sim-dots">✓</div>
        </div>
        <div class="wealth-bar" style="margin-top:6px">
          <div style="font-size:9px; color:var(--muted); text-transform:uppercase; letter-spacing:1px; margin-bottom:3px;">Victim wealth</div>
          <div class="wealth-row"><div class="wealth-swatch" style="background:#3b82f6"></div><span>Cat 5 · Aisé/Chic</span></div>
          <div class="wealth-row"><div class="wealth-swatch" style="background:#22c55e"></div><span>Cat 4 · Classe aisée</span></div>
          <div class="wealth-row"><div class="wealth-swatch" style="background:#eab308"></div><span>Cat 3 · Classe moyenne</span></div>
          <div class="wealth-row"><div class="wealth-swatch" style="background:#f97316"></div><span>Cat 2 · Défavorisé</span></div>
          <div class="wealth-row"><div class="wealth-swatch" style="background:#ef4444"></div><span>Cat 1 · Très défavorisé</span></div>
        </div>
      </div>

      <!-- RAT Framework Legend -->
      <div class="sb-section">
        <div class="sb-label">RAT Framework</div>
        <div style="font-size:11px; color:#64748b; line-height:1.8;">
          <div>🟢 <strong style="color:#22c55e">Guardianship</strong> — deters crime (police, cameras)</div>
          <div>🔴 <strong style="color:#ef4444">Target</strong> — attracts crime (banks, luxury shops)</div>
          <div>🟠 <strong style="color:#f97316">Generator</strong> — produces offenders (transport)</div>
          <div>🟡 <strong style="color:#eab308">Attractor</strong> — draws offenders (markets)</div>
        </div>
      </div>
    </div>
  </div>

  <!-- ── MAP ──────────────────────────────────────────── -->
  <div style="position:relative; flex:1;">
    <div id="map" style="width:100%; height:100%;"></div>

    <!-- View controls -->
    <div class="map-fab">
      <button class="fab-btn on" id="fab-markers" onclick="toggleMarkers()">📍 Point Markers</button>
      <button class="fab-btn" id="fab-cluster" onclick="toggleCluster()">🔵 Density Mode</button>
    </div>

    <!-- Crime heatmap legend -->
    <div class="heat-legend" id="heat-legend">
      <div class="hl-title">Crime Intensity</div>
      <div class="heat-grad"></div>
      <div class="heat-labels"><span>Low</span><span>High</span></div>
    </div>
  </div>
</div>

<script>
// ═══════════════════════════════════════════════════════
//  DATA
// ═══════════════════════════════════════════════════════
const CRIME = {crime_js};
const RENT  = {rent_js};
const OSM   = {osm_js};
const SIM   = {sim_js};

// ═══════════════════════════════════════════════════════
//  STATE
// ═══════════════════════════════════════════════════════
const layerState = {{
  'crime-heat':    true,
  'crime-circles': true,
  'wealth':        true,
  'police':        true,
  'camera':        true,
  'bank':          true,
  'cafe':          false,
  'transport':     false,
  'market':        false,
  'sim-heat':      true,
  'sim-dots':      true,
}};

const leafletLayers = {{}};
let map, heatLayer, simHeatLayer;

// ═══════════════════════════════════════════════════════
//  INIT MAP
// ═══════════════════════════════════════════════════════
function initMap() {{
  map = L.map('map', {{ zoomControl: false, attributionControl: false }})
         .setView([33.572, -7.600], 12);

  const carto = L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
    subdomains: 'abcd', maxZoom: 19
  }});
  let fallbackDone = false;
  carto.on('tileerror', () => {{
    if (fallbackDone) return;
    fallbackDone = true;
    map.removeLayer(carto);
    const osm = L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{ maxZoom:19 }});
    osm.on('tileload', e => {{
      e.tile.style.filter = 'invert(1) hue-rotate(180deg) saturate(0.5) brightness(0.75)';
    }});
    osm.addTo(map);
  }});
  carto.addTo(map);

  L.control.zoom({{ position:'bottomright' }}).addTo(map);
  L.control.attribution({{ position:'bottomright', prefix:'© OSM · CartoDB · Casablanca ABM' }}).addTo(map);

  // Pre-create layer groups for all toggleable layers
  const keys = ['crime-circles','wealth','police','camera','bank','cafe','transport','market','sim-dots'];
  keys.forEach(k => {{ leafletLayers[k] = L.layerGroup(); }});

  // Build everything
  buildHeat();
  buildCrimeCircles();
  buildWealthZones();
  buildOSMLayers();
  buildSimulatedLayer();

  // Show active layers
  Object.entries(layerState).forEach(([k, on]) => {{
    if (on && leafletLayers[k]) leafletLayers[k].addTo(map);
    if (k === 'crime-heat' || k === 'sim-heat') {{ /* handled separately */ }}
  }});

  if (layerState['crime-heat']) heatLayer && heatLayer.addTo(map);
  if (layerState['sim-heat'])   simHeatLayer && simHeatLayer.addTo(map);

  updateStats();
}}

// ═══════════════════════════════════════════════════════
//  LAYER BUILDERS
// ═══════════════════════════════════════════════════════

function buildHeat() {{
  const pts = Object.values(CRIME).map(d => [d.lat, d.lng, Math.min(d.total / 45, 1)]);
  heatLayer = L.heatLayer(pts, {{
    radius:35, blur:28, maxZoom:14,
    gradient: {{ 0:'#1a1a2e', 0.2:'#16213e', 0.5:'#f97316', 0.8:'#ef4444', 1:'#ffffff' }}
  }});
}}

function buildCrimeCircles() {{
  const lg = leafletLayers['crime-circles'];
  lg.clearLayers();
  const zoneColor = {{
    red:'#ef4444', orange:'#f97316', yellow:'#eab308', green:'#22c55e'
  }};
  const maxTotal = Math.max(...Object.values(CRIME).map(d => d.total));
  Object.entries(CRIME).forEach(([name, d]) => {{
    const col = zoneColor[d.zone] || '#6366f1';
    const r   = 200 + (d.total / maxTotal) * 1600;
    const c   = L.circle([d.lat, d.lng], {{
      radius: r, color: col, fillColor: col,
      fillOpacity: 0.2, weight: 1.5
    }});
    c.bindPopup(`
      <div class="popup-name">🚨 ${{name}}</div>
      <div class="popup-grid">
        <div class="popup-cell"><div class="popup-cell-val" style="color:${{col}}">${{d.total}}</div><div class="popup-cell-lbl">Total</div></div>
        <div class="popup-cell"><div class="popup-cell-val" style="color:#818cf8">${{d.articles}}</div><div class="popup-cell-lbl">📰 Articles</div></div>
        <div class="popup-cell"><div class="popup-cell-val" style="color:#f472b6">${{d.youtube}}</div><div class="popup-cell-lbl">📺 Videos</div></div>
        <div class="popup-cell"><div class="popup-cell-val">${{d.zone.toUpperCase()}}</div><div class="popup-cell-lbl">Risk Zone</div></div>
      </div>
    `, {{ maxWidth:240 }});
    c.addTo(lg);
  }});
}}

function buildWealthZones() {{
  const lg = leafletLayers['wealth'];
  lg.clearLayers();
  Object.entries(RENT).forEach(([name, d]) => {{
    const opacity = d.median ? 0.32 : 0.16;
    const medianText = d.median ? `${{d.median.toLocaleString()}} DH / mois` : 'Données insuffisantes';
    const c = L.circle([d.lat, d.lng], {{
      radius: 1500,
      color: d.color, fillColor: d.color,
      fillOpacity: opacity, weight: 2,
      dashArray: d.median ? null : '6 4'
    }});
    c.bindPopup(`
      <div class="popup-name">💰 ${{name}}</div>
      <div class="popup-grid">
        <div class="popup-cell" style="grid-column:span 2">
          <div class="popup-cell-val" style="color:${{d.color}}">${{d.label}}</div>
          <div class="popup-cell-lbl">Wealth Category ${{d.cat}}</div>
        </div>
        <div class="popup-cell" style="grid-column:span 2">
          <div class="popup-cell-val">${{medianText}}</div>
          <div class="popup-cell-lbl">Median Rent</div>
        </div>
      </div>
    `, {{ maxWidth:220 }});
    c.addTo(lg);
  }});
}}

function buildSimulatedLayer() {{
  // Heatmap from simulated crimes
  if (SIM.length > 0) {{
    const pts = SIM.map(c => [c.lat, c.lng, 0.8]);
    simHeatLayer = L.heatLayer(pts, {{
      radius:22, blur:16, maxZoom:16,
      gradient: {{ 0:'#2e1065', 0.4:'#7c3aed', 0.7:'#a78bfa', 1:'#e9d5ff' }}
    }});
  }}

  // Dots coloured by victim wealth
  const lg = leafletLayers['sim-dots'];
  lg.clearLayers();
  const wealthColor = {{ 1:'#ef4444', 2:'#f97316', 3:'#eab308', 4:'#22c55e', 5:'#3b82f6' }};
  const wealthLabel = {{ 1:'Très défavorisé', 2:'Défavorisé', 3:'Classe moyenne', 4:'Classe aisée', 5:'Aisé/Chic' }};
  SIM.forEach(c => {{
    const col = wealthColor[c.victim_wealth] || '#a78bfa';
    const m = L.circleMarker([c.lat, c.lng], {{
      radius: 5, color: col, fillColor: col,
      fillOpacity: 0.8, weight: 0.5
    }});
    m.bindPopup(`
      <div class="popup-name">🤖 Simulated Crime</div>
      <div class="popup-grid">
        <div class="popup-cell"><div class="popup-cell-val" style="color:${{col}}">${{wealthLabel[c.victim_wealth] || 'Unknown'}}</div><div class="popup-cell-lbl">Victim Wealth</div></div>
        <div class="popup-cell"><div class="popup-cell-val">${{c.tick}}</div><div class="popup-cell-lbl">Simulation Tick</div></div>
      </div>
    `, {{ maxWidth:200 }});
    m.addTo(lg);
  }});

  const el_dots = document.getElementById('cnt-sim-dots');
  if (el_dots) el_dots.textContent = SIM.length;
}}

function buildOSMLayers() {{
  const colorMap = {{
    guardianship: {{ police:'#3b82f6', camera:'#8b5cf6', lighting:'#fbbf24' }},
    target:       {{ bank:'#ef4444',  cafe:'#f97316', shop:'#fb923c' }},
    generator:    {{ transport:'#f97316' }},
    attractor:    {{ market:'#eab308' }}
  }};

  Object.entries(OSM).forEach(([key, meta]) => {{
    const lg = leafletLayers[key];
    if (!lg) return;
    lg.clearLayers();
    const col = meta.color || '#6366f1';
    const r = 4;
    meta.nodes.forEach(n => {{
      const m = L.circleMarker([n.lat, n.lng], {{
        radius: r, color: col, fillColor: col,
        fillOpacity: 0.85, weight: 0.5
      }});
      m.bindPopup(`
        <div class="popup-name">${{meta.emoji}} ${{n.name || meta.label}}</div>
        <div style="font-size:11px; color:var(--muted)">
          <span style="text-transform:uppercase; font-size:9px; letter-spacing:1px; color:${{col}}">${{meta.rat.toUpperCase()}}</span>
          · ${{meta.label}}
        </div>
      `, {{ maxWidth:200 }});
      m.addTo(lg);
    }});

    // Update sidebar counts
    const el = document.getElementById(`cnt-${{key}}`);
    if (el) el.textContent = meta.count;
  }});
}}

// ═══════════════════════════════════════════════════════
//  TOGGLE LOGIC
// ═══════════════════════════════════════════════════════

function toggleLayer(key) {{
  layerState[key] = !layerState[key];
  const on = layerState[key];

  const li  = document.getElementById(`li-${{key}}`);
  const chk = document.getElementById(`chk-${{key}}`);
  if (li)  li.classList.toggle('active', on);
  if (chk) chk.textContent = on ? '✓' : '○';

  if (key === 'crime-heat') {{
    if (on) heatLayer && heatLayer.addTo(map);
    else    heatLayer && map.removeLayer(heatLayer);
  }} else if (key === 'sim-heat') {{
    if (on) simHeatLayer && simHeatLayer.addTo(map);
    else    simHeatLayer && map.removeLayer(simHeatLayer);
  }} else if (leafletLayers[key]) {{
    if (on) leafletLayers[key].addTo(map);
    else    map.removeLayer(leafletLayers[key]);
  }}
}}

let markersMode = true;
function toggleMarkers() {{
  const btn = document.getElementById('fab-markers');
  markersMode = !markersMode;
  btn.classList.toggle('on', markersMode);
}}

function toggleCluster() {{
  const btn = document.getElementById('fab-cluster');
  btn.classList.toggle('on');
}}

// ═══════════════════════════════════════════════════════
//  STATS
// ═══════════════════════════════════════════════════════

function updateStats() {{
  const totalCrime = Object.values(CRIME).reduce((s,d) => s + d.total, 0);
  const guardCount = (OSM.police?.count || 0) + (OSM.camera?.count || 0);
  const targetCount = (OSM.bank?.count || 0) + (OSM.cafe?.count || 0);

  document.getElementById('cnt-crime').textContent  = totalCrime;
  document.getElementById('cnt-guard').textContent  = guardCount;
  document.getElementById('cnt-target').textContent = targetCount;
  document.getElementById('cnt-rent').textContent   = Object.keys(RENT).length;
  document.getElementById('cnt-sim').textContent    = SIM.length;

  document.getElementById('s-incidents').textContent = totalCrime;
  document.getElementById('s-guard').textContent     = guardCount;
  document.getElementById('s-targets').textContent   = targetCount;
  document.getElementById('s-sim').textContent       = SIM.length;

  // OSM layer counts
  if (OSM.police) document.getElementById('cnt-police').textContent = OSM.police.count;
  if (OSM.camera) document.getElementById('cnt-camera').textContent = OSM.camera.count;
  if (OSM.bank)   document.getElementById('cnt-bank').textContent   = OSM.bank.count;
  if (OSM.cafe)   document.getElementById('cnt-cafe').textContent   = OSM.cafe.count;
  if (OSM.transport) document.getElementById('cnt-transport').textContent = OSM.transport.count;
  if (OSM.market)    document.getElementById('cnt-market').textContent    = OSM.market.count;
}}

// ═══════════════════════════════════════════════════════
//  BOOT
// ═══════════════════════════════════════════════════════
window.addEventListener('DOMContentLoaded', initMap);
</script>
</body>
</html>"""

    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ Dashboard written → {OUT_HTML}")
    print(f"   Crime neighborhoods : {len(CRIME_DATA)}")
    print(f"   Wealth zones        : {len(RENT_LOOKUP)}")
    print(f"   OSM layers          : {len(layers_js)}")
    total_osm = sum(v['count'] for v in layers_js.values())
    print(f"   OSM total nodes     : {total_osm:,}")
    print(f"   Simulated crimes    : {len(sim_crimes)}")

if __name__ == "__main__":
    build_dashboard()
