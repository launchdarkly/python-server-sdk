# Async Python SDK — Remaining Follow-ups

**Status:** the async implementation shipped **experimentally in `launchdarkly-server-sdk` 9.17.0**.
Docs (SDK-2879) and the distinct async User-Agent (SDK-2658) have landed. The async client's only
diff vs the sync client is intentional and documented.

This doc is the forward-looking punch list only. Full history lives in git, Jira **SDK-60 / SDK-2513**,
and the `CONTRIBUTING.md` sync/async parity note. Nothing below is a regression or a blocker for the
experimental release — it's the pre-GA backlog + parked items.

---

## Pre-GA decisions (settle before the async surface GAs)

- **SDK-2766 — async flag-listener misses updates during subscription.** `AsyncFlagValueChangeListener`
  captures its baseline in `create()` (`await eval_fn`) *before* registering, so a change during that
  `await` window is missed. Mirrors sync's accepted behavior; the `await` widens it. Decide: accept
  (sync parity) or register-then-capture-baseline under the listener's lock (watch for a spurious first
  notification). `impl/async_flag_tracker.py`.
- **SDK-62 — sync `FeatureStore.upsert -> bool`.** The async store already returns `bool` (fires change
  events only on a real write); sync stays `-> None` with a `TODO(SDK-62)` until this breaking change.
  Next major.
- **SDK-2773 (under SDK-34) — remove `is_alive`/`BackgroundOperation` from the sync interfaces.** A 2015
  Twisted fossil; nothing calls `update_processor.is_alive()`. The async interfaces already omit it.
  Minor breaking; next major.
- **SDK-34 breaking-change review.** Walk the SDK-34 epic + SDK-62 before async GA to bundle related
  breaks into the major that GAs async, rather than spreading them.
- **Standardize async teardown (architectural).** Decision settled: single-event-loop / not thread-safe;
  cooperative atomicity; `asyncio.Lock` only where a critical section spans an `await`; always let
  `CancelledError` propagate. **Remaining:** now that genuine async persistent stores exist (Redis #488,
  DynamoDB #490), make the FDv2 availability poller an `AsyncRepeatingTask` to remove the last
  loop↔thread boundary and its thread-safe primitives.
- **Async hook environment-ID parity (#484).** When the async client runs hooks with the environment ID,
  add an `environment_id` property + record calls to `AsyncFDv2` and the abstract property to
  `AsyncDataSystem`, mirroring sync #484.

## Hooks & plugins async support (SDK-3077)

`AsyncLDClient` accepts `AsyncHook`/`AsyncPlugin` only and **silently drops** sync ones; the OTel
(`ldotel`) and observability (`ldobserve`) integrations ship sync `Hook`/`Plugin`. Decide + implement:
thin async variants per integration, or have the async SDK accept the existing sync `Hook`/`Plugin`
under a non-blocking contract. Hooks are client-agnostic (their methods take only shared types) and
safe; plugins carry `register(client)` coupling. Also fix the inconsistency: `AsyncConfig` silently
drops sync hooks/plugins (`async_config.py:282`) while `add_hook` raises `TypeError` (`async_client.py:646`).
Design + dotnet reference (dotnet hooks/plugins are fully sync): `async-integrations-plan.md` (scratchpad).

## Flaky tests (each a small PR off `main`)

- **`test_variation[_detail]_when_feature_store_throws_error`** — a leaked sync polling thread from an
  earlier test logs into this test's `caplog`. `RepeatingTask.stop()` (`repeating_task.py:34-38`) never
  `join()`s the worker. Fix: assert *membership* not full-list equality (`test_ldclient_evaluation.py:137,148`);
  optional product fix: join the worker on `stop()`/`close()`.
- **`test_async_migrator.py::TestTrackingLatency`** (+ sync `test_migrator.py`) — Windows timer granularity
  lets an `asyncio.sleep(0.1)` wake <100ms. Loosen the lower-bound assertion (~80ms).
- **Sync `DefaultEventProcessor` shutdown bugs** — the same two the async processor fixed in #472
  (guard the stop branch; drain the outbox before stopping workers). Apply to `impl/events/event_processor.py`;
  reasonable to bundle with the flaky-test sync fix.
- **Environmental flakes (shared `gh-actions` repo — helps every SDK):** the `persistent-stores` Consul
  setup and the `contract-tests` harness download both fail transiently before tests run. Add
  retry/backoff or cache the binaries.

## Small cleanups / investigations

- **`AsyncFeatureStore` docstring** (`interfaces.py:341`) still says items are "simply a dict of arbitrary
  data" — wrong now that async stores hold decoded model objects. Fix.
- **`is_available`/`close` on the store-core protocols** — `FeatureStoreCore`/`AsyncFeatureStoreCore`
  leave these duck-typed, forcing `# type: ignore`. Decide: promote as optional/default-noop members, or
  leave.
- **Share a base between `AsyncDataSystem` and `DataSystem`** for the common members (`store`, status
  providers, `data_availability`, `target_availability`).
- **Data-source status SINK helpers** (~40 lines) still duplicated in `status.py`/`async_status.py` —
  extract or accept.
- **`AsyncExitStack`** — investigate replacing the hand-rolled async close logic (getattr/isawaitable
  branching) with a shared exit stack.
- **`ItemDescriptor` model like .NET** — investigate a first-class versioned-item + tombstone type.
- **Revisit `AsyncStore` locking** — single-loop asyncio may not need the inherited `threading.RLock`.
- **Use `.items()`** in the FDv2 store loops (`store.py`/`async_store.py`) — cosmetic.
- **Store-error kind overwritten** by streaming's broad `except` (both sync + async) — if corrected, fix
  in both to preserve parity.

## Cross-SDK

- **Decode contract test** (from #489, which fixed the async transport substituting U+FFFD on an
  undecodable body). A shared `sdk-test-harness` test could catch this across SDKs; needs an opt-in
  capability flag + a **spec decision** ("an SDK must not apply data from a payload it cannot decode;
  reject and keep last-known-good"). ~½ day harness-side.
