"""
patch_contract.py  <version>

Adds deployment fields to models/<version>/contract.json. Safe to run twice.
  python processing/person_b/patch_contract.py v20261010
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
version = sys.argv[1] if len(sys.argv) > 1 else sorted((ROOT / "models").glob("v*"))[-1].name
p = ROOT / "models" / version / "contract.json"

with p.open("r", encoding="utf-8") as f:
    c = json.load(f)

lag = int(c.get("persistence_lag_days", 30))
c.update({
    "evaluation_status": "GDELT improvement rules (stated before the run, committed after) were NOT met",
    "probability_status": "uncalibrated risk score for ranking; isotonic tested and rejected",
    "calibration_method": "none",
    "retraining": "monthly, after each UCDP Candidate release",
    "label_resolution": "UCDP release covering t+7; weeks, not days",
    "deployment_note": "prod_* models use no GDELT; shadow_clf must never alert",
    "ucdp_lag_observation": ("newest UCDP release (Aug 2026, covers through 2026-08-31) was 40 days "
                             "behind on 2026-10-10"),
})
c.setdefault("scoring", {}).update({
    "timezone": "UTC",
    "score_after": "23:45 batch of the day has landed",
    "never_score_partial_days": True,
    "gating": "prod_status needs only UCDP persistence; shadow_status also needs GDELT",
})
c.setdefault("history_required", {}).update({"gdelt_days": 14, "ucdp_days_before_t": lag + 27})

with p.open("w", encoding="utf-8") as f:
    json.dump(c, f, indent=2)
    f.write("\n")
check = json.load(p.open("r", encoding="utf-8"))
print("Contract updated and validated:", p)
print("persistence_lag_days:", check.get("persistence_lag_days"), "| calibration:", check.get("calibration_method"))