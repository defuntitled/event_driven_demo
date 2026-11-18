# Current Goal

Rewrite demo per docs/DEMO_TECHNICAL_SPEC.md (v1.0), preserve presentation content from docs/PRESENTATION_DESCRIPTION.md.

## Decisions

- docs/plans/2026-11-18-go-demo-rewrite.md explicitly approved through Plannotator (decision=approved), followed by user “продолжай”. No review remains open.
- User explicitly said “не делай коммиты”; never commit or push. Work remains in original directory/master. Branch creation failed sandbox, escalation was interrupted; no branch change happened.
- New runtime uses exact spec Go1.26.11, franz-go1.22.3, go-sqlite3 1.14.53, Kafka4.1.2. Approval includes listed local-demo exceptions to Ozon conventions.
- Preserve old runtime/volumes and unrelated Docker workloads. Only new project last-ticket-go-acceptance at18080 running; new default project last-ticket-go-demo.

## Plan Status

Tasks1/2 verified including first real browser purchase; Task3 compensation/dedup tests green; Task4 diagnostics verified against actual Kafka/DBs. Task5 subprocess supervisor and crash73 implemented, task6 persisted controller implemented; currently validating actual pauses/fault via new tests/integration/scenarios_test.go.
- Added internal/workers supervisor, subprocess readiness logs, graceful stop/kill and Wait; unit subprocess pause test green.
- Common migration002 adds run_control_requests to preserve immutable params before INITIALIZE. ServiceControl phases and late-phase/idempotent tests green.
- Replaced temporary InitializeFirstRun/PrepareFirst/RunInitialWorkers; main now spawns real children and demoweb.ControlLoop executes durable phase operations.
- Public prepare/reset/actions/operations/retry and presenter state exist. Backend tests all green before integration check.
- Remaining: full dispatcher with race pair and restart recovery; presenter UI; rest of T01–T25 and race checks; review; archive/cutover README; clean-start/slides rehearsal.

## Evidence

- Exact toolchain downloaded and SHA256 checked: /tmp/last-ticket-toolchain/go/bin/go. Host Go1.26.1 must NOT be used.
- All Go commands: GOCACHE=/tmp/last-ticket-gocache GOMODCACHE=/tmp/last-ticket-gomod GOTOOLCHAIN=local /tmp/last-ticket-toolchain/go/bin/go ...
- First red tests observed missing new APIs; green contracts/storage/config, order creation (8 concurrent repeats), rollback charge/reserve, happy projection, inbox/attempt atomicity.
- Go module tidy downloaded kmsg1.15.1, compress1.20.3, lz4v4.1.33. No unauthorized version substitutions.
- Docker images downloaded; digests pinned in Dockerfile/compose.yaml. Kafka pull transient TLS timeout resolved after escalated retry.
- Initial Compose full build succeeded; 4 role servers and Kafka healthy. kafka-init proved all3 topics2 partitions, RF1, retention86400000, cleanup delete.
- Common build anchor initially caused concurrent image-tag export collision. Fixed: only orders has build, shared image and pull_policy never.
- Docker builds require escalation to write host buildx metadata; no buildx CLI available but Compose internal builder works. Do not inspect sensitive Docker config.
- Legacy Dockerfile/compose.yaml/.dockerignore saved under legacy/python; old runtime directories otherwise untouched.

## Active Commands

- exec session from latest compose up -d --no-build: need poll (read tool history for ID). Latest image includes child supervisor/control.
- Build session87312 completed successfully. Compiler cache mount now makes rebuild ~6 seconds.
- Initial integration compile failed due invalid Example identifier, fixed. TestPaymentCrash then failed expected405 against OLD running image. New image being started now; rerun full integration suite.

## Next Concrete Work

1. Finish compose start, then DEMO_BASE_URL=http://localhost:18080 [Go env below] go test -tags=integration ./tests/integration -count=1 -v. Four tests happy/pause/paymentcrash/outbox, serialized with stand mutex and t.Parallel.
2. Debug failures with concrete data; enhance crash test to assert newPID and offset advancement, outbox test to prove journal caught up before absence.
3. Task7 durable dispatcher: current savePurchase creates one persisted job and synchronously HTTP-delivers it; race scenario does NOT yet create competitor; pending jobs NOT recovered in background. Plan explicitly requires both. App.deliveryGate currently surrounds synchronous purchase; controller uses gate before canceling old jobs.
4. Task8 full presenter UI nonexistent (link currently404); buyer basic UI working. Need browser tests5 routes, race old response/version, storage error/backoff improvements.
5. Task9/10 broad acceptance, cleanup/archive old Python files, README, presentation notes, check_slides, fresh isolated start.

## Technical Notes

- Go commands: PATH=/tmp/last-ticket-toolchain/go/bin:$PATH GOCACHE=/tmp/last-ticket-gocache GOMODCACHE=/tmp/last-ticket-gomod GOTOOLCHAIN=local. goimports exists /Users/kibikeev/go/bin/goimports, requires this PATH/env to avoid host auto-download.
- Build with docker compose -p last-ticket-go-acceptance build orders, escalation prefix approved. Up: DEMO_PORT=18080 docker compose -p last-ticket-go-acceptance up -d --no-build (sandbox works). All images pinned.
- Playwright /tmp/last-ticket-browser/node_modules/playwright + Chrome binary /Applications/Google Chrome.app/Contents/MacOS/Google Chrome with temp profile only. node tests/go-happy.cjs requires escalation, prefix approved. It assumes a fresh happy run; now use prepare/reset first.
- Kafka real metadata verified: artifacts/go-diagnostics.json contains3 journal events and full group offsets with null on untouched partitions. APIs /api/presenter/databases/events/attempts/orders/kafka implemented; owner /internal/records/{table} allowlists and paginates.
- kgo Request handles ListOffsets/OffsetFetch routing. kgo lacks MaxPollInterval API; uses RebalanceTimeout60s, session6, heartbeat2. Full docs /tmp/last-ticket-kgo-doc.txt.
- Fault transaction changes remaining1->0, desiredFAULTED and attemptfault with charge/inbox/outbox; Consumer os.Exit73 BEFORE CommitRecords. Supervisor does NOT signal FAULTED child, waits actual exit.
- Supervisor snapshot mixes last child identity with desired state when stopped; readiness checks running generation. Need tests/final review for restart, concurrent phases and readiness.
- Controller loops a single persisted active operation. FAILED keeps admissionclosed and active operation; retry same ID/gen. Phase barriers20sec, requests3sec, child stop5sec. Old run retained; pending jobs canceled before QUIESCE.
- Initial startup/restart reconciliation for an already READY demo is still missing: ControlLoop currently does nothing if READY + no active op. Must verify all3 services on restart without making newrun.
- Public can_purchase currently uses DB READY only; must account for orders availability/admission per spec.
- Snapshot valid first purchase: run_a64c1cd04b8122ec24aaca6627f31435, ord_71343c5a20a5064c9e593bdc311ebbf8, all3topics partition1offset0. go-first-purchase.png/json evidence.
- All user project files untracked; no commits. Keep no-commit instruction. README remains old Python until final cutover; old dirs intact except backed-up Dockerfile/compose/.dockerignore under legacy/python.

# Previous Completed Goal

Presentation redesign complete per docs/PRESENTATION_DESCRIPTION.md; user will replace the demo separately.

## Decisions

- Plan docs/plans/2026-11-17-presentation-redesign.md revision2 explicitly approved through Plannotator 2026-11-17.
- New description is source of truth: keep Go1.26, concert, target buyer-triggered competitor and outbox demo. Do not adapt to current Python runtime.
- 12 logical slides /33 fixed graphic frames; final.md contains 12 final states. Frame changes use physical slides permitted by spec; visible footer stays N /12.
- Assets and Inter/IBM Plex Mono fonts are local; final PNG playback needs no fonts installed or network. Supported image terminal required.
- Current directory/master retained, no commits/pushes. Runtime and user demo data untouched.

## Plan Status

All presentation tasks completed. Notes/README/verification updated. No review session remains open. Future demo checks and human checks are documented, not falsely marked performed.

## Evidence

- python3 scripts/build_slides.py succeeded: 33 SVG/PNG frames, 12 finals, manifest.
- python3 scripts/check_slides.py succeeded: talk33 frames at120x40 and100x32; final12 frames at120x40; valid distinct image packets, Space/left/G/gg/numeric/q, no loading/overflow errors. Report artifacts/presentation-check.json.
- SVG reveal groups unchanged across frames, all paths valid and PNG dimensions1600x900.
- /root/presentation_review inspected all33 PNGs and SVGs plus notes/README. One minor slide10 arrow/text overlap fixed and rechecked; no outstanding findings.
- PTY checks verify emitted iTerm2 packets, not physical graphical terminal rendering. All images independently inspected.
- rsvg-convert2.62.3; presenterm0.16.1. Need PANGOCAIRO_BACKEND=fc on macOS to honor bundled fonts. dark theme margin override uses percent0 to avoid merging different enum variants.
- Main source files: scripts/build_slides.py, scripts/check_slides.py, slides/talk.md, slides/final.md, slides/speaker-notes.md, slides/verification.md, slides/presenterm.yaml, slides/assets/.

## Open Issues

- EVENT_DRIVEN_TALK.md absent; references to its sections use the new description's mapping.
- Updated Go demo not available; four target browser routes, optional outbox route and new clean-start acceptance pending. Existing Python tests not rerun for documentation work.
- Physical graphical terminal/projector and full spoken25-minute rehearsal pending.

## Previous Runtime Context

Separate customer localhost8000/presenter8001, UI reset and six auto-cases were previously implemented and verified on Python. Preserve runtime files; see previous accepted plans and project-map.md.
Old artifacts/acceptance-run.json, scenarios-run.json, browser-run.json, presenterm-check.json and replay-example.txt are previous-runtime/nine-slide evidence only, not new Go demo proof.
