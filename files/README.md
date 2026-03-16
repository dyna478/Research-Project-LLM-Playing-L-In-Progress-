# 🕷️ Casablanca Crime Scraper & ABM Simulation

A Proof-of-Concept (POC) demonstrating AI-powered crime zone analysis and Agent-Based Modeling (ABM) in Casablanca, Morocco using public news data.

**Note:** The scraping component is specifically a tool to gather environmental and baseline data for the true simulation engine.

---

## 1. 🕷️ The Scraper Pipeline (Data Extraction)

Scrapes 20 Moroccan news websites for **سرقة** (theft/crime) articles, filters only those mentioning **الدار البيضاء** (Casablanca) and its neighborhoods, and enriches them using **Claude AI** (or Hugging Face APIs) to extract structured crime data.

### 📁 Pipeline Structure
```
casablanca_crime_scraper/
├── main.py                  ← Entry point — run this
├── requirements.txt
├── scraper/
│   └── news_scraper.py      ← 20 websites, scraping logic
├── nlp/
│   └── claude_extractor.py  ← Claude API enrichment
├── utils/
│   ├── helpers.py           ← Text cleaning, filtering
│   └── logger.py            ← Logging setup
└── output/                  ← Results saved here
    ├── casablanca_crimes_YYYYMMDD_HHMMSS.json
    ├── casablanca_crimes_YYYYMMDD_HHMMSS.csv
    └── casablanca_crimes_YYYYMMDD_HHMMSS_enriched.json
```

### ⚙️ Scraper Setup
**1. Install dependencies**
```bash
pip install -r requirements.txt
```
**2. Set your Hugging Face API key**
```bash
# Linux/Mac
export HUGGING_FACE_HUB_TOKEN=your_key_here
# Windows
set HUGGING_FACE_HUB_TOKEN=your_key_here
```

### 🚀 Scraper Usage
```bash
# Scrape everything (all 20 sources)
python main.py --mode scrape

# Quick test — Tier 1 sources only (7 sites, fastest)
python main.py --mode scrape --tier 1

# Scrape only one site
python main.py --mode scrape --source hespress

# Run Hugging Face NLP enrichment on existing scraped data
python main.py --mode enrich --input output/casablanca_crimes_20250101_120000.json

# Full pipeline: scrape + enrich
python main.py --mode full --tier 1
```

### ⚠️ Scripter Guidelines
- The scraper includes **1.5–4 second delays** to respect website limits.
- **Expected results:** Tier 1 yields ~500–1,500 articles in 2–4 hours. All 20 sites yield ~2,000–5,000 articles in 8–12 hours.
- **Rate Limits:** NLP enrichment processes ~1.5 articles/second due to API limits.

---

## 2. 🧠 The ABM Simulation Engine (Routine Activity Theory)

Once the historical data is scraped, we use it to construct a highly detailed digital twin of Casablanca based on **Routine Activity Theory** (RAT). RAT states a crime occurs when a *Motivated Offender* meets a *Suitable Target* in the *Absence of Guardianship*.

### 🗺️ The Three Environmental Layers
To simulate this, we built three layers around the scraped data:
1. **Historical Crime Layer (Our Scraper Data):** Baseline heatmap of reality.
2. **Socio-Economic Layer:** Custom scraper mapping Avito.ma apartment rents to estimate Wealth Zones (Cat 1–5).
3. **Guardianship & Targets (OpenStreetMap):** Extracted Overpass nodes for Police Stations (77), Cameras (26), Banks (815), and Cafes.

### ⚙️ Running the Simulation (`abm_engine.py`)
Built with Python's **Mesa** framework, `OffenderAgents` move across the environment and probabilistically commit crimes based on police proximity and target wealth.
```bash
# Run simulation (default 200 ticks)
python abm_engine.py

# Custom configuration
python abm_engine.py --steps 500 --offenders 60 --victims 120 --patrols 30
```

### 📊 The Unified Dashboard
The outputs of the Scraper and the ABM Simulation merge into an interactive Leaflet map.
```bash
# Generate the interactive dashboard
python build_abm_dashboard.py
```
Open `output/abm_dashboard.html` to view the **Historical Heatmap, Wealth Zones, Guardianship nodes, and Simulated ABM crimes** unified on one CartoDB interface.

---

## 🔜 Next Steps of Research (RAT Focus)

This project serves as a foundational layer for deeper urban criminology research. The immediate next steps include:

1. **Calibrating the RAT Engine:** Fine-tuning the mathematical weights of the Routine Activity Theory equation. This means adjusting the deterrence radius (`GUARDIAN_RADIUS`) of police versus cameras and weighing target attractiveness based on local wealth density.
2. **Integrating Crime Pattern Theory:** Expanding the `OffenderAgent` logic to utilize transit networks (Tramway and Bus stops) rather than random wandering. Offenders will travel along known pathways to "Activity Nodes", intersecting with victims.
3. **Temporal Dynamics:** Introducing day/night cycles where target availability (e.g., cafes close) and guardianship efficacy (e.g., street lighting vs. daytime visibility) fluctuate over a 24-hour simulation tick period.
4. **Statistical Validation:** Correlating the ABM-generated hotspot map (outputted in `simulated_crimes.json`) with the historical LLM-extracted map (`neighborhoods_extracted.json`) to prove the predictive value of the simulation framework.
