# Talos 2 Cleanup LOC Audit

Date: 2026-05-05
Workspace: Talos-2.0

## Baseline Audit

- Total LOC: 73,911
- Python LOC: 55,502
- Template LOC: 8,896
- CSS/JS LOC: 9,513

### Major Folder Breakdown

| Folder | LOC |
| --- | ---: |
| services | 30,966 |
| templates | 8,896 |
| static | 9,513 |
| tests | 15,779 |
| scripts | 1,449 |
| supabase | 0 |
| instance | 0 |

Excluded from this audit: `.git`, `.venv`, `__pycache__`, `.pytest_cache`, `runtime_backups`, `logs`, `artifacts`, and generated/runtime-only content under excluded paths.

## Final Audit

- Total LOC: 74,161
- Net LOC delta: +250
- Counted files: 165

This final audit uses the same exclusions as the baseline audit: `.git`, `.venv`, `__pycache__`, `.pytest_cache`, `runtime_backups`, `logs`, `artifacts`, and generated/runtime-only content under excluded paths.

Note: This reflects the current Talos 2 cleanup working tree after Research removal, Talos visibility changes, and public Kairos surface shutdown. Deeper Kairos service/runtime code is still present in the repository.

## Pre-Major Cleanup Snapshot

- Date: 2026-05-05
- Git branch state: `main...origin/main` with local modifications already present
- Pre-pass commit: `71921491fff22fd1e2d17a74ed30fe8dfeac4c3f`

### Current LOC Baseline For This Pass

- Total LOC: 74,167
- Python LOC: 55,202
- Template LOC: 8,762
- CSS/JS LOC: 9,513
- Counted files: 165

### Major Folder Breakdown

| Folder | LOC |
| --- | ---: |
| app.py | 6,980 |
| services | 30,878 |
| templates | 8,762 |
| static | 9,513 |
| tests | 15,751 |
| scripts | 1,571 |
| docs | 35 |
| README.md | 244 |
| HOSTED_CHANGELOG.md | 18 |
| config.py | 144 |
| delphi5_phase6_hosted_shell_note.md | 22 |
| supabase | 249 |

### Smoke Test Before Deletion

- `/apollo`: 200
- `/trades/real`: 200
- `/performance`: 200
- `/`: failed before this pass with `NameError: name 'MarketDataError' is not defined`

This pre-pass baseline uses the same exclusions as the original audit: `.git`, `.venv`, `__pycache__`, `.pytest_cache`, `runtime_backups`, `logs`, `artifacts`, and generated/runtime-only content under excluded paths.

## Post-Major Cleanup Snapshot

- Date: 2026-05-05
- Total LOC: 46,466
- Net LOC delta from pre-major snapshot: -27,701
- Counted files: 97

### Major Folder Breakdown

| Folder | LOC |
| --- | ---: |
| app.py | 6,888 |
| services | 22,608 |
| templates | 6,234 |
| static | 9,513 |
| tests | 26 |
| scripts | 702 |
| docs | 107 |
| README.md | 244 |
| config.py | 144 |

### Validation

- `python -m compileall .`: passed
- `pytest tests/test_core_smoke.py -q`: passed (`3 passed`)
- Local app smoke via Flask test client: `/` `200`, `/apollo` `200`, `/journal` `200`, `/performance` `200`, `/kairos` `404`

### Notes

- Kairos runtime/services, Kairos tests, Delphi4 sync code, and most legacy validation scripts were physically deleted in this pass.
- The repository is materially smaller, but it is still above the requested ~20k LOC target because large hosted/mobile/static/template surfaces remain in `app.py`, `templates`, and `static`.