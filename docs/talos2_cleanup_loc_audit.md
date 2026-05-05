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

## Talos 2.1 Pre-Prune Snapshot

- Date: 2026-05-05
- Pre-pass commit: `6fb4016bcbc70c8311733a3fb7669f2c2795d84b`

### Working Tree Status (Before Edits)

Modified:

- app.py
- config.py
- instance/apollo_last_run.json
- instance/horme_trades.db
- services/open_trade_manager.py
- services/runtime/service_composition.py
- static/styles.css
- templates/_delphi_header_macros.html

Deleted:

- services/talos_service.py
- static/hosted_shell.js
- templates/_hosted_mobile_bottom_nav.html
- templates/home.html
- templates/hosted_device_launch.html
- templates/hosted_login.html
- templates/hosted_login_mobile.html
- templates/hosted_mobile_apollo.html
- templates/hosted_mobile_shell.html
- templates/hosted_shell_access_error.html
- templates/hosted_shell_apollo.html
- templates/hosted_shell_base.html
- templates/hosted_shell_home.html
- templates/hosted_shell_journal.html
- templates/hosted_shell_manage_trades.html
- templates/hosted_shell_open_trades.html
- templates/hosted_shell_performance.html
- templates/hosted_shell_placeholder.html
- templates/performance_summary.html
- templates/talos.html

### LOC Baseline By Category

- Python LOC: 20,755
- Templates LOC: 3,552
- CSS LOC: 7,441
- JS LOC: 261
- Tests LOC: 95
- Static text/code assets LOC: 7,871

### Top 15 LOC Files

| LOC | File |
| ---: | --- |
| 7,441 | static/styles.css |
| 3,213 | app.py |
| 2,403 | services/trade_store.py |
| 1,852 | services/apollo_candidate_service.py |
| 1,573 | services/open_trade_manager.py |
| 1,530 | templates/performance.html |
| 1,482 | services/performance_dashboard_service.py |
| 948 | services/providers/schwab_provider.py |
| 929 | templates/trades.html |
| 818 | services/repositories/trade_repository.py |
| 718 | templates/index.html |
| 707 | services/apollo_structure_service.py |
| 534 | services/market_data.py |
| 496 | services/macro_service.py |
| 493 | services/repositories/management_state_repository.py |

## Talos 2.1.0 Post-Prune Snapshot

- Date: 2026-05-05
- Version: 2.1.0
- Validation: `pytest tests/ -q` → 6 passed

### LOC By Category

| Category | LOC |
| --- | ---: |
| Python | 20,361 |
| Templates | 3,475 |
| CSS | 6,077 |
| JS | 261 |
| Scripts / Docs | 616 |
| **Total source** | **30,790** |

### Net LOC Reduction vs Pre-Prune Baseline

| Metric | Value |
| --- | ---: |
| Pre-prune source LOC | 32,104 |
| Post-prune source LOC | 30,790 |
| Net delta (this pass) | −1,314 |

### Major Folder Breakdown

| Folder | LOC |
| --- | ---: |
| services | 16,707 |
| static | 6,338 |
| templates | 3,475 |
| app.py | 2,819 |
| scripts | 665 |
| supabase | 230 |
| README.md | 182 |
| docs | 139 |
| config.py | 133 |
| tests | 95 |

### Top 15 LOC Files

| LOC | File |
| ---: | --- |
| 6,077 | static/styles.css |
| 2,819 | app.py |
| 2,403 | services/trade_store.py |
| 1,852 | services/apollo_candidate_service.py |
| 1,573 | services/open_trade_manager.py |
| 1,530 | templates/performance.html |
| 1,482 | services/performance_dashboard_service.py |
| 948 | services/providers/schwab_provider.py |
| 929 | templates/trades.html |
| 818 | services/repositories/trade_repository.py |
| 718 | templates/index.html |
| 707 | services/apollo_structure_service.py |
| 534 | services/market_data.py |
| 496 | services/macro_service.py |
| 493 | services/repositories/management_state_repository.py |

### Files Deleted In This Pass

| File | Lines Removed |
| --- | ---: |
| services/talos_service.py | 3,724 |
| templates/talos.html | 649 |
| templates/hosted_mobile_shell.html | 585 |
| static/hosted_shell.js | 411 |
| templates/performance_summary.html | 147 |
| templates/hosted_login.html | 156 |
| templates/hosted_login_mobile.html | 154 |
| templates/hosted_mobile_apollo.html | 135 |
| templates/home.html | 117 |
| templates/hosted_shell_open_trades.html | 71 |
| templates/hosted_shell_manage_trades.html | 62 |
| templates/hosted_shell_journal.html | 64 |
| templates/hosted_shell_home.html | 64 |
| templates/hosted_shell_apollo.html | 86 |
| templates/notifications_settings.html | 82 |
| templates/hosted_shell_base.html | 48 |
| templates/hosted_device_launch.html | 49 |
| templates/hosted_shell_performance.html | 46 |
| templates/hosted_shell_access_error.html | 22 |
| templates/hosted_shell_placeholder.html | 19 |
| templates/_hosted_mobile_bottom_nav.html | 14 |

### Key Removals In This Pass

- **app.py**: ~160 lines removed — dead helpers (`build_mobile_performance_ui_filters`, `build_open_trade_action_payload`, `format_loss_range`, `coerce_trade_notification_input`, `coerce_global_notification_settings_input`), broken route reference fixed in `build_delphi_route_map()`, dead imports removed (`dataclass`, `PERFORMANCE_DEFAULT_FILTERS`, entire `trade_notifications` import block)
- **static/styles.css**: ~1,364 lines removed — hosted-shell CSS block, top-level kairos blocks, residual kairos selector stubs from media queries, dead utility classes (`.eyebrow`, `.provider-strip`, `.provider-value`, `.provider-sub`, `.full-width`, `.export-actions`)
- **config.py**: Version bumped `2.0.2` → `2.1.0`