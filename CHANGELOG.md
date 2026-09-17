# Changelog

All notable changes to this project are documented here.
Versioning follows [Semantic Versioning](https://semver.org/) (MAJOR.MINOR.PATCH).

## v2.0 - 2026-09
- Migrated the federated testbed from VirtualBox/Docker to a Kubernetes-native deployment (ONM, DTM, and N tenant-specific NDT instances as pods, with declarative orchestration and self-healing)
- Added XRM (Extended Reality Manager): a third agentic manager providing per-tenant XR service provisioning, deployed as XRM Core + per-NDT XRM distributed agents
- Fixed a real bug in `DTM/dtm.py`'s `AdmissionQueue`: dispatch order was a plain FIFO deque, so tenant priority only affected admission/drop decisions and never actual service order (priority and FIFO modes produced identical latency). Replaced with a `heapq` ordered by priority
- Added a non-destructive `AdmissionQueue.peek_ids()` so "QUEUED" status updates are correctly broadcast to xrm-agents (previously always empty due to a `drain_up_to(0)` no-op bug)
- Added `--tenant-bounds`: an optional per-tenant admission cap in `AdmissionQueue`, independent of the shared queue depth `K`, intended to bound how long an always-admitted (non-droppable) tenant can wait under sustained contention. **Included as a candidate mechanism, not yet validated** — with the current synchronous test client (`ndt_event_cli.py`), a tenant's in-flight request count never exceeds 1, so no bound value can actually engage the check. Validating this requires an open-loop load generator (tracked as future work)
- New analysis pipeline: `experiment_1_results.py`, `experiment_2_results.py`, `analyze_queue_operations.py`, `ndt_event_cli.py`
- New Kubernetes automation: `run_experiment2.sh`, `run_experiment2_bounded.sh`, updated `launch_testbed.sh`/`stop_testbed.sh`, NDT lifecycle scripts (`restart_ndt.sh`, `watch_restarts.sh`)

## v1.6 - 2026-05
- Kubernetes deployment (clean `on_ndt`/`onm`/`dtm` separation, dynamic topology), federated mode validated

## v1.5 - 2026-04
- Experiment 1 (ON-to-NDTs synchronization) analysis scripts added

## v1.4.1 - 2026-04
- New functions to synchronize ON changes to NDTs in federated mode (Experiment 1); VirtualBox-based deployment

## v1.3 - 2026-04
- VirtualBox-based federated prototype checkpoint

## v0 - 2026-01
- Initial relay nodes between ON and NDT
