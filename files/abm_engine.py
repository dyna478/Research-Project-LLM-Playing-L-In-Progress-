"""
abm_engine.py
─────────────────────────────────────────────────────────────────────────────
Casablanca Crime ABM — Routine Activity Theory Simulation Engine
Uses Mesa 3.x (ContinuousSpace)

Theoretical Framework: Routine Activity Theory (Cohen & Felson, 1979)
A crime occurs when:
  1. Motivated Offender
  2. Suitable Target
  3. Absence of Capable Guardian
...converge in the same place at the same time.

Agent Types:
  - OffenderAgent  : Spawns in poor wealth zones, wanders toward targets
  - VictimAgent    : Moves between home / activity nodes (cafes, banks)
  - GuardianAgent  : Police patrol around police station nodes

Data Sources (loaded from JSON):
  - output/osm_environment_casablanca.json  → Police, Camera, Bank, Cafe, Transport nodes
  - output/rent_data.json                   → Neighborhood wealth categories
  - Crime coordinates hardcoded from casablanca_crime_map.html

Usage:
  python abm_engine.py                     # 200 steps (default)
  python abm_engine.py --steps 500         # run 500 ticks
  python abm_engine.py --steps 100 --seed 42
  python abm_engine.py --steps 200 --no-viz  # skip matplotlib, JSON only
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import math
import random
import sys
from pathlib import Path

import mesa
import mesa.space as mspace

# ── Optional matplotlib for visualisation ─────────────────────────────────────
try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# ═════════════════════════════════════════════════════════
#  CONFIG
# ═════════════════════════════════════════════════════════
BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
OSM_JSON   = OUTPUT_DIR / "osm_environment_casablanca.json"
RENT_JSON  = OUTPUT_DIR / "rent_data.json"
SIM_OUT    = OUTPUT_DIR / "simulated_crimes.json"

# Casablanca bounding box (used to normalise GPS into 0…WIDTH × 0…HEIGHT grid)
BBOX = {
    "lat_min": 33.46, "lat_max": 33.73,
    "lng_min": -7.75, "lng_max": -7.36,
}
GRID_W = 1000   # grid units (arbitrary, higher = more precision)
GRID_H = 800

# ── RAT Parameters ────────────────────────────────────────────────────────────
GUARDIAN_RADIUS     = 30    # grid units — police deterrence aura
CAMERA_RADIUS       = 15    # cameras have a smaller deterrence radius
CRIME_RADIUS        = 8     # offender must be within this range of victim to attack
BASE_CRIME_PROB     = 0.30  # base probability of crime attempt per tick when in range
GUARDIAN_DETERRENCE = 0.95  # how much being near a police reduces crime probability

OFFENDER_SPEED      = 6     # grid units per tick
VICTIM_SPEED        = 4     # grid units per tick
PATROL_SPEED        = 5     # police patrol speed

NUM_OFFENDERS       = 40
NUM_VICTIMS         = 80
NUM_PATROLS         = 20    # how many mobile patrol officers (beyond static guardians)


# ═════════════════════════════════════════════════════════
#  COORDINATE UTILITIES
# ═════════════════════════════════════════════════════════
def gps_to_grid(lat, lng):
    """Convert GPS (lat/lng) → (x, y) in the simulation grid."""
    x = (lng - BBOX["lng_min"]) / (BBOX["lng_max"] - BBOX["lng_min"]) * GRID_W
    y = (lat - BBOX["lat_min"]) / (BBOX["lat_max"] - BBOX["lat_min"]) * GRID_H
    # Clamp to valid grid range
    x = max(1.0, min(GRID_W - 1.0, x))
    y = max(1.0, min(GRID_H - 1.0, y))
    return x, y


def dist(a, b):
    """Euclidean distance between two (x, y) tuples."""
    return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)


def move_toward(pos, target, speed):
    """Move from pos toward target by at most `speed` units; return new position."""
    dx, dy = target[0] - pos[0], target[1] - pos[1]
    d = math.sqrt(dx * dx + dy * dy)
    if d <= speed:
        return target
    ratio = speed / d
    return pos[0] + dx * ratio, pos[1] + dy * ratio


def random_walk(pos, speed, rng, margin=5):
    """Take a random step of exactly `speed` units in some direction."""
    angle = rng.uniform(0, 2 * math.pi)
    nx = max(margin, min(GRID_W - margin, pos[0] + speed * math.cos(angle)))
    ny = max(margin, min(GRID_H - margin, pos[1] + speed * math.sin(angle)))
    return nx, ny


# ═════════════════════════════════════════════════════════
#  DATA LOADERS
# ═════════════════════════════════════════════════════════
def load_osm():
    with open(OSM_JSON, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("layers", data)


def load_rent():
    with open(RENT_JSON, encoding="utf-8") as f:
        return json.load(f)


def build_node_list(osm_layers, key):
    """Return a list of (x, y) grid coordinates for a given OSM layer key."""
    layer = osm_layers.get(key, {})
    nodes = layer.get("nodes", [])
    return [gps_to_grid(n["lat"], n["lng"]) for n in nodes if "lat" in n and "lng" in n]


# ═════════════════════════════════════════════════════════
#  AGENTS
# ═════════════════════════════════════════════════════════

class OffenderAgent(mesa.Agent):
    """
    Motivated Offender. Spawns in low-wealth zones (Cat 1/2).
    Wanders the city looking for Victim or Target nodes.
    Commits crime if target is close + guardian is far.
    """
    def __init__(self, model):
        super().__init__(model)
        self.pos = self._spawn_pos()
        self.crimes_committed = 0
        self._target = None
        self._cooldown = 0   # ticks to wait after committing a crime

    def _spawn_pos(self):
        # Spawn near one of the low-wealth zone centroids
        LOW_WEALTH_ORIGINS = [
            (33.590, -7.587),   # Hay Mohammadi
            (33.543, -7.591),   # Moulay Rachid
            (33.552, -7.570),   # Ben Msick
            (33.598, -7.554),   # Sidi Moumen
            (33.584, -7.601),   # Derb Sultan
            (33.557, -7.538),   # Ain Chock
        ]
        base_lat, base_lng = self.random.choice(LOW_WEALTH_ORIGINS)
        # Add small random jitter within ~0.01° (~1km)
        lat = base_lat + self.random.uniform(-0.015, 0.015)
        lng = base_lng + self.random.uniform(-0.015, 0.015)
        return gps_to_grid(lat, lng)

    def step(self):
        if self._cooldown > 0:
            self._cooldown -= 1
            return

        # Decide a target (victim or static node)
        if self._target is None or self.random.random() < 0.05:
            self._target = self._pick_target()

        # Move toward target
        if self._target:
            self.pos = move_toward(self.pos, self._target, OFFENDER_SPEED)

        # Check for crime opportunity
        self._check_crime()

    def _pick_target(self):
        """Head toward a random bank/cafe node (target) or a victim agent."""
        # 50% of the time, pick a static target (bank or cafe)
        if self.random.random() < 0.5 and self.model.target_nodes:
            return self.random.choice(self.model.target_nodes)

        # Other times, pick a victim agent if any exist
        victims = [a for a in self.model.agents if isinstance(a, VictimAgent)]
        if victims:
            v = self.random.choice(victims)
            return v.pos
        return None

    def _check_crime(self):
        """RAT Crime Equation: if near a victim and no guardian → commit crime."""
        if self._cooldown > 0:
            return

        # Find nearest victim
        nearest_victim = None
        nearest_victim_dist = float("inf")
        for a in self.model.agents:
            if isinstance(a, VictimAgent):
                d = dist(self.pos, a.pos)
                if d < nearest_victim_dist:
                    nearest_victim_dist = d
                    nearest_victim = a

        # Must be within crime radius of a victim
        if nearest_victim is None or nearest_victim_dist > CRIME_RADIUS:
            return

        # Calculate guardian deterrence
        crime_prob = BASE_CRIME_PROB
        for g_pos in self.model.guardian_positions:
            if dist(self.pos, g_pos) < GUARDIAN_RADIUS:
                crime_prob *= (1 - GUARDIAN_DETERRENCE)
        for c_pos in self.model.camera_positions:
            if dist(self.pos, c_pos) < CAMERA_RADIUS:
                crime_prob *= (1 - GUARDIAN_DETERRENCE / 2)

        # Mobile patrol agents also act as guardians
        for a in self.model.agents:
            if isinstance(a, GuardianAgent):
                if dist(self.pos, a.pos) < GUARDIAN_RADIUS:
                    crime_prob *= (1 - GUARDIAN_DETERRENCE)

        # Rational Choice: high target value raises probability
        target_value_boost = (nearest_victim.wealth_level - 1) * 0.05
        crime_prob = min(0.95, crime_prob + target_value_boost)

        # Roll the dice
        if self.random.random() < crime_prob:
            self.crimes_committed += 1
            self.model.record_crime(self.pos, nearest_victim)
            self._cooldown = 10   # wait 10 ticks before next attempt
            self._target = None


class VictimAgent(mesa.Agent):
    """
    Ordinary resident / target. Moves between home and activity nodes.
    Has a wealth_level (1-5) based on their home zone.
    Higher wealth = higher target value but also higher chance of being in guarded area.
    """
    def __init__(self, model, wealth_level=3):
        super().__init__(model)
        self.wealth_level = wealth_level
        self.home = self._pick_home()
        self.pos = self.home
        self._destination = None
        self._ticks_at_dest = 0

    def _pick_home(self):
        WEALTH_ORIGINS = {
            1: [(33.590, -7.587), (33.543, -7.591), (33.598, -7.554)],  # poor east
            2: [(33.552, -7.570), (33.584, -7.601), (33.557, -7.538)],
            3: [(33.569, -7.639), (33.549, -7.634), (33.573, -7.589)],  # middle
            4: [(33.585, -7.650), (33.580, -7.648), (33.582, -7.637)],  # rich west
            5: [(33.593, -7.697), (33.600, -7.690)],                    # chic
        }
        origins = WEALTH_ORIGINS.get(self.wealth_level, WEALTH_ORIGINS[3])
        base_lat, base_lng = self.random.choice(origins)
        lat = base_lat + self.random.uniform(-0.012, 0.012)
        lng = base_lng + self.random.uniform(-0.012, 0.012)
        return gps_to_grid(lat, lng)

    def step(self):
        self._ticks_at_dest += 1

        # Decide a destination
        if self._destination is None or (self._ticks_at_dest > 15 and self.random.random() < 0.3):
            self._destination = self._pick_destination()
            self._ticks_at_dest = 0

        if self._destination:
            self.pos = move_toward(self.pos, self._destination, VICTIM_SPEED)
            if dist(self.pos, self._destination) < 2:
                self._destination = None

    def _pick_destination(self):
        """Move to home, a transport hub, or a cafe/restaurant."""
        roll = self.random.random()
        if roll < 0.3:
            return self.home
        elif roll < 0.6 and self.model.transport_nodes:
            return self.random.choice(self.model.transport_nodes)
        elif self.model.cafe_nodes:
            return self.random.choice(self.model.cafe_nodes)
        return self.home


class GuardianAgent(mesa.Agent):
    """
    Mobile Police Patrol. Spawns near a police station and patrols in a radius.
    Acts as a deterrent to OffenderAgents within GUARDIAN_RADIUS.
    """
    def __init__(self, model, station_pos):
        super().__init__(model)
        self.station_pos = station_pos
        # Start near station with a small random offset
        offset_x = self.random.uniform(-10, 10)
        offset_y = self.random.uniform(-10, 10)
        self.pos = (station_pos[0] + offset_x, station_pos[1] + offset_y)
        self._patrol_target = None
        self._patrol_radius = 60   # grid units

    def step(self):
        # Stay within patrol radius of station; pick new patrol waypoints
        if self._patrol_target is None or dist(self.pos, self._patrol_target) < 3:
            angle = self.random.uniform(0, 2 * math.pi)
            r = self.random.uniform(5, self._patrol_radius)
            nx = self.station_pos[0] + r * math.cos(angle)
            ny = self.station_pos[1] + r * math.sin(angle)
            nx = max(1.0, min(GRID_W - 1.0, nx))
            ny = max(1.0, min(GRID_H - 1.0, ny))
            self._patrol_target = (nx, ny)

        self.pos = move_toward(self.pos, self._patrol_target, PATROL_SPEED)


# ═════════════════════════════════════════════════════════
#  MODEL
# ═════════════════════════════════════════════════════════

class CasablancaModel(mesa.Model):
    """
    The Casablanca ABM. Loads all spatial data, populates the city,
    and runs the Routine Activity Theory simulation.
    """

    def __init__(self, seed=None,
                 num_offenders=NUM_OFFENDERS,
                 num_victims=NUM_VICTIMS,
                 num_patrols=NUM_PATROLS):
        super().__init__(seed=seed)

        print("  Loading OSM data…")
        osm = load_osm()
        print("  Loading rent data…")
        rent = load_rent()

        # ── Static environment nodes ───────────────────────────────────────
        self.guardian_positions  = build_node_list(osm, "police")
        self.camera_positions    = build_node_list(osm, "camera")
        self.target_nodes        = build_node_list(osm, "bank") + build_node_list(osm, "chic_cafe")
        self.transport_nodes     = build_node_list(osm, "transport")
        self.cafe_nodes          = build_node_list(osm, "chic_cafe") + build_node_list(osm, "market")

        print(f"  Police stations : {len(self.guardian_positions)}")
        print(f"  Cameras         : {len(self.camera_positions)}")
        print(f"  Target nodes    : {len(self.target_nodes)}")
        print(f"  Transport nodes : {len(self.transport_nodes)}")

        # ── Crime log ─────────────────────────────────────────────────────
        self.crime_log = []      # list of dicts: {tick, x, y, victim_wealth}

        # ── Spawn Victim Agents (weighted by wealth category) ─────────────
        #   Real-world distribution: more poor residents than rich
        WEALTH_DIST = [1]*12 + [2]*12 + [3]*8 + [4]*5 + [5]*3  # 40 entries
        for _ in range(num_victims):
            wl = self.random.choice(WEALTH_DIST)
            VictimAgent(self, wealth_level=wl)

        # ── Spawn Offender Agents ──────────────────────────────────────────
        for _ in range(num_offenders):
            OffenderAgent(self)

        # ── Spawn Mobile Patrol Guardians ──────────────────────────────────
        if self.guardian_positions:
            for _ in range(num_patrols):
                station = self.random.choice(self.guardian_positions)
                GuardianAgent(self, station_pos=station)

        # ── DataCollector ──────────────────────────────────────────────────
        self.datacollector = mesa.DataCollector(
            model_reporters={
                "Total Crimes":    lambda m: len(m.crime_log),
                "Active Offenders": lambda m: sum(1 for a in m.agents if isinstance(a, OffenderAgent)),
            },
            agent_reporters={
                "Crimes": lambda a: a.crimes_committed if isinstance(a, OffenderAgent) else 0,
            }
        )

        self.step_count = 0

    def record_crime(self, pos, victim):
        self.crime_log.append({
            "tick":         self.step_count,
            "x":            round(pos[0], 2),
            "y":            round(pos[1], 2),
            "victim_wealth": victim.wealth_level,
            # Convert back to approx lat/lng for dashboard overlay
            "lat": round(BBOX["lat_min"] + pos[1] / GRID_H * (BBOX["lat_max"] - BBOX["lat_min"]), 5),
            "lng": round(BBOX["lng_min"] + pos[0] / GRID_W * (BBOX["lng_max"] - BBOX["lng_min"]), 5),
        })

    def step(self):
        self.step_count += 1
        self.datacollector.collect(self)
        self.agents.shuffle_do("step")


# ═════════════════════════════════════════════════════════
#  ANALYSIS & OUTPUT
# ═════════════════════════════════════════════════════════

def run_simulation(steps=200, seed=42, num_offenders=NUM_OFFENDERS,
                   num_victims=NUM_VICTIMS, num_patrols=NUM_PATROLS, verbose=True):
    print("=" * 60)
    print("  Casablanca Crime ABM Engine")
    print(f"  Steps: {steps} | Offenders: {num_offenders} | Victims: {num_victims}")
    print(f"  Patrols: {num_patrols} | Seed: {seed}")
    print("=" * 60)

    model = CasablancaModel(
        seed=seed,
        num_offenders=num_offenders,
        num_victims=num_victims,
        num_patrols=num_patrols
    )
    print(f"\n  Running {steps} simulation steps…")
    for i in range(steps):
        model.step()
        if verbose and i % 50 == 0:
            print(f"    Tick {i:>4d} / {steps} — Crimes so far: {len(model.crime_log)}")

    return model


def print_summary(model):
    crimes = model.crime_log
    total  = len(crimes)
    print("\n" + "=" * 60)
    print(f"  SIMULATION COMPLETE — {total} total crimes in {model.step_count} ticks")
    print("=" * 60)

    if not crimes:
        print("  No crimes occurred. Try increasing steps or offender count.")
        return

    # Crimes by victim wealth level
    from collections import Counter
    wc = Counter(c["victim_wealth"] for c in crimes)
    print("\n  Crimes by victim wealth category:")
    labels = {1: "Très défavorisé", 2: "Défavorisé", 3: "Classe moyenne", 4: "Classe aisée", 5: "Aisé/Chic"}
    for cat in sorted(wc.keys()):
        bar = "█" * min(wc[cat], 50)
        print(f"    Cat {cat} ({labels.get(cat,'?'):20s}): {bar}  {wc[cat]}")

    # Top offenders
    offenders = sorted(
        [a for a in model.agents if isinstance(a, OffenderAgent)],
        key=lambda a: -a.crimes_committed
    )
    print("\n  Top 5 offenders by crimes committed:")
    for i, a in enumerate(offenders[:5]):
        print(f"    {i+1}. Agent #{a.unique_id:3d} → {a.crimes_committed} crimes")

    # Geographic hotspot (quantise crimes to grid cells)
    hotspot_grid = Counter()
    for c in crimes:
        cell = (int(c["x"] // 50), int(c["y"] // 50))
        hotspot_grid[cell] += 1
    print("\n  Top 5 crime hotspot cells (grid 50×50 units):")
    for cell, cnt in hotspot_grid.most_common(5):
        cx, cy = cell
        approx_lat = round(BBOX["lat_min"] + (cy * 50) / GRID_H * (BBOX["lat_max"] - BBOX["lat_min"]), 3)
        approx_lng = round(BBOX["lng_min"] + (cx * 50) / GRID_W * (BBOX["lng_max"] - BBOX["lng_min"]), 3)
        bar = "█" * min(cnt, 40)
        print(f"    [{approx_lat}, {approx_lng}]  {bar}  {cnt}")


def save_output(model):
    crimes = model.crime_log
    model_df = model.datacollector.get_model_vars_dataframe()

    output = {
        "simulation": {
            "steps":           model.step_count,
            "total_crimes":    len(crimes),
            "num_offenders":   sum(1 for a in model.agents if isinstance(a, OffenderAgent)),
            "num_victims":     sum(1 for a in model.agents if isinstance(a, VictimAgent)),
            "num_patrols":     sum(1 for a in model.agents if isinstance(a, GuardianAgent)),
            "guardian_nodes":  len(model.guardian_positions),
            "camera_nodes":    len(model.camera_positions),
        },
        "crimes":    crimes,
        "crime_timeseries": model_df["Total Crimes"].tolist(),
        "rat_params": {
            "guardian_radius":     GUARDIAN_RADIUS,
            "camera_radius":       CAMERA_RADIUS,
            "crime_radius":        CRIME_RADIUS,
            "base_crime_prob":     BASE_CRIME_PROB,
            "guardian_deterrence": GUARDIAN_DETERRENCE,
        }
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(SIM_OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n  ✅ Saved → {SIM_OUT}")


def visualise(model):
    if not HAS_MATPLOTLIB:
        print("  [!] matplotlib not installed — skipping visualisation.")
        return

    crimes = model.crime_log
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.patch.set_facecolor("#0a0e1a")

    # ── Left: Spatial crime scatter ─────────────────────────────────────
    ax = axes[0]
    ax.set_facecolor("#0d1117")
    ax.set_title("Simulated Crime Locations", color="white", fontsize=12, pad=10)
    ax.set_xlim(0, GRID_W); ax.set_ylim(0, GRID_H)

    # Draw environment
    for px, py in model.guardian_positions:
        ax.plot(px, py, "b.", markersize=3, alpha=0.3, zorder=1)
    for px, py in model.camera_positions:
        ax.plot(px, py, "m.", markersize=2, alpha=0.2, zorder=1)
    for px, py in model.target_nodes[:200]:
        ax.plot(px, py, "y.", markersize=2, alpha=0.15, zorder=1)

    # Draw crimes coloured by wealth level
    cmap = {1:"#ef4444", 2:"#f97316", 3:"#eab308", 4:"#22c55e", 5:"#3b82f6"}
    if crimes:
        for c in crimes:
            ax.plot(c["x"], c["y"], "o",
                    color=cmap.get(c["victim_wealth"], "white"),
                    markersize=5, alpha=0.7, zorder=3)

    # Legend
    patches = [mpatches.Patch(color=v, label=f"Cat {k} victim") for k, v in cmap.items()]
    patches += [
        mpatches.Patch(color="blue",    label="Police station"),
        mpatches.Patch(color="magenta", label="Camera"),
        mpatches.Patch(color="yellow",  label="Target node"),
    ]
    ax.legend(handles=patches, loc="upper right", fontsize=7,
              framealpha=0.3, labelcolor="white", facecolor="#1a1a2e")
    ax.tick_params(colors="#64748b")
    for spine in ax.spines.values(): spine.set_edgecolor("#1e293b")

    # ── Right: Crime time-series ─────────────────────────────────────────
    ax2 = axes[1]
    ax2.set_facecolor("#0d1117")
    ax2.set_title("Cumulative Crimes Over Time", color="white", fontsize=12, pad=10)
    ts = model.datacollector.get_model_vars_dataframe()["Total Crimes"]
    ax2.plot(ts.index, ts.values, color="#ef4444", linewidth=2)
    ax2.fill_between(ts.index, ts.values, alpha=0.15, color="#ef4444")
    ax2.set_xlabel("Simulation Tick", color="#64748b")
    ax2.set_ylabel("Cumulative Crimes", color="#64748b")
    ax2.tick_params(colors="#64748b")
    ax2.grid(axis="y", color="#1e293b", linewidth=0.5)
    for spine in ax2.spines.values(): spine.set_edgecolor("#1e293b")

    plt.tight_layout()
    fig_path = OUTPUT_DIR / "abm_results.png"
    plt.savefig(str(fig_path), dpi=150, facecolor=fig.get_facecolor())
    print(f"  📊 Chart saved → {fig_path}")
    plt.show()


# ═════════════════════════════════════════════════════════
#  ENTRY POINT
# ═════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Casablanca Crime ABM — Routine Activity Theory")
    parser.add_argument("--steps",          type=int, default=200,          help="Number of simulation ticks")
    parser.add_argument("--seed",           type=int, default=42,           help="Random seed")
    parser.add_argument("--offenders",      type=int, default=NUM_OFFENDERS,help="Number of offender agents")
    parser.add_argument("--victims",        type=int, default=NUM_VICTIMS,  help="Number of victim agents")
    parser.add_argument("--patrols",        type=int, default=NUM_PATROLS,  help="Number of mobile police patrols")
    parser.add_argument("--no-viz",         action="store_true",            help="Skip matplotlib chart")
    args = parser.parse_args()

    model = run_simulation(
        steps=args.steps,
        seed=args.seed,
        num_offenders=args.offenders,
        num_victims=args.victims,
        num_patrols=args.patrols,
    )

    print_summary(model)
    save_output(model)

    if not args.no_viz:
        visualise(model)

    return model


if __name__ == "__main__":
    main()
