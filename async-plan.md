# Async Python Server SDK — Plan

**Epic:** SDK-60. **Implementation branch:** `jb/sdk-60/async-python-sdk` (single squash commit).

This is the single canonical plan: the architecture, the implemented-behavior reference, and the
stacked-PR extraction plan. The implementation is complete and validated; what remains is slicing it
into reviewable PRs.

**Status (current):** the full async implementation lives on the impl branch and is validated —
**1345 unit tests pass, mypy + isort clean, both contract suites 4670**. **Phase 0 is complete** —
Phase 0a (sync fixes) merged via #447; Phase 0b (impl-branch reverts) done. The impl branch is a
single squash commit on latest `main`, and eventsource 1.7.1 (async client + SDK-2600 pool fix) is
released and pinned. Extraction of the async slices is underway.

---

## Architecture — "drop-codegen"

Hand-maintained parallel sync/async code over shared sans-I/O logic. **No generator.**

- **Shared sans-I/O `_common` modules** hold the pure logic imported by *both* the sync file (already
  in `main`) and its async sibling: `impl/client_common.py`, `impl/datasystem/fdv2_common.py`,
  `impl/events/event_processor_common.py`.
- **Sibling pairs** for the I/O-bearing components: `foo.py` (sync) + `async_foo.py` (async), e.g.
  `evaluator.py` / `async_evaluator.py`, `big_segments.py` / `async_big_segments.py`. The evaluator
  is a sibling pair, **not** folded into a `_common`.
- **Async support classes** (`impl/aio/`): `concurrency.py` (async concurrency primitives — locks,
  queues, task runner, repeating task, worker pool, callback scheduler), `transport.py`
  (`AsyncHTTPTransport` / `AsyncSSEFactory` over aiohttp), `transport_types.py` (the one shared
  `TransportResponse`). These have **no** sync twin — sync code uses stdlib / urllib3 / `SSEClient`
  directly. This is a genuine async support library, not a compatibility shim: the earlier
  sync-or-async duck-typing bridges (`resolve`, `iterate`, `store_get`/`store_all`, the no-op
  `AsyncRWLock`) were **removed** once the interfaces were properly async-typed (see the data-store
  read contract below), so every async call site is a plain `await` / `async for`.
- **Uniform async store read** (`AsyncReadOnlyStore` in `interfaces.py`): the async analog of
  `ReadOnlyStore`, with `async def get`/`all`. Both async data systems expose their active store
  through it — FDv1's `AsyncFeatureStore` satisfies it directly; FDv2 wraps its synchronous
  in-memory active store in a tiny `_AsyncStoreView` (the async analog of the sync side's
  `FeatureStoreClientWrapper`). The client reads uniformly with `await ds.store.get(...)`, no
  `isinstance`. Similarly, async-typed null objects (`AsyncNullEventProcessor`,
  `AsyncNullUpdateProcessor` in `stubs.py`, and `_NotStartedDataSystem`) make every `stop()` awaitable
  so the client/data-system teardown is uniform `await`, no coroutine sniffing.
- **Drift protection:** the `CONTRIBUTING.md` sync/async parity note + both contract suites + the
  async support tests (`test_aio.py`) + `test_sync_async_parity.py` (public-surface guard; allowlists document intentional
  one-sided members — client `postfork`/`start`, feature_store async-only `close`). There is **no**
  codegen `--check` gate (no generator exists) and, by decision, no broader public-method-parity test.
- **Experimental marking (sdk-specs 1.2.1.2):** the async surface is a non-GA feature in a GA SDK, so
  every async **public** class/entry point carries a `.. caution::` "experimental … not ready for
  production … no backwards-compat guarantees" block (verbatim 1.2.1 wording), mirroring
  eventsource 1.7.1's `AsyncSSEClient`. Done on the impl branch for `AsyncLDClient`,
  `AsyncInMemoryFeatureStore`, `AsyncHook`, `AsyncPlugin`, `AsyncMigrator`/`AsyncMigratorBuilder`,
  the async interface ABCs (`AsyncFeatureStore`, `AsyncBigSegmentStore`, `AsyncDataSourceUpdateSink`,
  `AsyncInitializer`, `AsyncSynchronizer`), and `Redis.async_big_segment_store()`. Each async PR must
  keep the block on any public class it introduces. The README also carries an "Async support
  (experimental)" section with the matching `> [!CAUTION]` block (extract with the client slice, PR 10).
- **Config split (IMPLEMENTED):** the async client takes a dedicated **`AsyncConfig`** class in its
  own file (`async_config.py`), **not** the sync `Config`. Its component fields are async-typed
  (`AsyncFeatureStore`, `AsyncBigSegmentStore` via `AsyncBigSegmentsConfig`, `AsyncHook`/`AsyncPlugin`,
  async `*_class` factories), so sync-vs-async is enforced by the type system — **no runtime
  `isinstance` checks in either client** (the old `big_segments` `ValueError`, `feature_store`
  `NotImplementedError`, and `*_class` misuse paths were removed). Fields are **duplicated** (not a
  shared base) since the two configs may diverge; extract a base later if that proves unnecessary.
  `HTTPConfig` and `DataStoreMode` are pure data and shared as-is. The data-system plumbing
  (`DataSystemConfig` + `DataSourceBuilder`) stays **shared/generic**, and the config surface is
  carved into **role-specific layered protocols** (interface segregation) rather than one broad one:
  - `SdkIdentityConfig` — the SDK identity fields (`sdk_key`, `wrapper_name`, `wrapper_version`,
    `application`); used by the shared `get_environment_metadata`/`secure_mode_hash` helpers.
  - `DataSourceBuilderConfig(SdkIdentityConfig)` — adds the transport/endpoint fields (`base_uri`,
    `stream_base_uri`, `http`, `initial_reconnect_delay`, `poll_interval`, `payload_filter_key`,
    `_instance_id`); the parameter type of `DataSourceBuilder.build()`.
  - `PrivateAttributesConfig` — `all_attributes_private` + `private_attributes`; the parameter type
    of `EventOutputFormatter` (removed the last sync/async `type: ignore`).

  `Config` and `AsyncConfig` **explicitly inherit `DataSourceBuilderConfig`** (so they satisfy
  `SdkIdentityConfig` transitively; conformance is visible + mypy-enforced) and satisfy
  `PrivateAttributesConfig` structurally. `get_plugin_hooks` takes the plugin list directly. Superseded
  the earlier ideas of union-typing the shared `Config` + per-client validation, and of overloading a
  single broad config protocol.

---

## Design reference (implemented behavior)

Concise record of the non-obvious behavioral decisions. The **code + contract tests are the
authoritative source**; this is orientation for reviewers.

- **Loop ownership** — `AsyncLDClient` owns no event loop; it runs on the caller's running loop and
  is used via `async with AsyncLDClient(config) as client:` (explicit `await client.start()` /
  `await client.close()`).
- **Shared `aiohttp.ClientSession`** — one session per client, created lazily and owned/closed by the
  client (or transport) when it created it; an injected session stays owned by the caller.
- **TLS & timeouts** — derived from `Config.http` (connect/read timeouts, proxy, cert verification),
  mapped onto `aiohttp.ClientTimeout` / connector settings.
- **Streaming** — a single consume loop driven by `ld_eventsource`'s `AsyncSSEClient`
  (library-driven reconnect/backoff); the data source reacts to `Start` / `Event` / `Fault`.
- **`flush()`** — blocking delivery: `await flush()` resolves after the batch has been sent.
- **Ready / startup-hang prevention** — the ready event is set on success *and* on unrecoverable
  error, so `start()` never hangs.
- **Double-start / concurrent-close guards** — `start()` and `close()` are idempotent.
- **Bounded shutdown** — background tasks are awaited with a timeout on close.
- **Bounded event queue** — backpressure with drop-and-warn when full.
- **Feature store** — `AsyncFeatureStore` only. `AsyncConfig.feature_store` is typed `AsyncFeatureStore`,
  so a sync store can't be configured; `None` defaults to `AsyncInMemoryFeatureStore`. Reads route
  through `_get_store_item` so the legacy `kind.decode(...)`-on-dict step is preserved. (There is no
  sync-store `ThreadPoolExecutor` fallback — it was never implemented and is now precluded by the type.)
- **`AsyncFeatureStore.upsert` returns `bool`** — the async store reports whether it actually wrote
  (new or newer version), so `AsyncDataSourceUpdateSinkImpl` fires flag-change events only on a real
  write — no read-before-write, no spurious events on a version-rejected upsert. This is the SDK-62
  contract ("data stores return a boolean"), adopted on the async side now because the interface is
  new (non-breaking). The released **sync** `FeatureStore.upsert` stays `-> None` until **SDK-62**
  makes the breaking change; until then sync keeps main's behavior with a `TODO(SDK-62)`. A justified
  sync/async divergence that converges at the next major version.
- **Big segments** — fully async (`AsyncBigSegmentStoreManager` + async Redis adapter).
- **Hooks & plugins** — the async client accepts **async hooks/plugins only** (`AsyncHook`,
  `AsyncPlugin`); `add_hook` raises `TypeError` on a sync `Hook`. Starting strict is deliberate:
  adding sync support later is a non-breaking addition, whereas removing it later would be breaking.
- **Flag-change listeners** — async evaluation, sync callbacks.
- **FDv2** — async data system (`async_fdv2` coordinator + async FDv2 sources), selected when
  `config.datasystem_config is not None`.
- **Lazy imports** — no `aiohttp` at import time; `AsyncLDClient` is exposed via a lazy
  `ldclient.__getattr__`. `pytest-asyncio` runs in `strict` mode.

---

## Phase 0 — Sync cleanup ✅ COMPLETE

The architecture work touched *released sync production code*. Two audits (sync-diff vs `main`;
async-parity) classified every change; the cleanup is now resolved. Standing principle: **minimize
changes to released sync code; where sync/async should align, move the new async side, never the
released sync side; diverge only when it makes sense.**

### Phase 0a — "Fix existing sync issues" PR ✅ MERGED (#447)

A standalone PR to `main` with only the genuine bugs that exist independent of async. Reviewable on
its own; after it merges the impl branch rebases and absorbs it. Contents (currently held on the impl
branch as minimal additions over `main`):

- **F1** — `datasource/polling.py`: `time.time` → `time.time()` (the UNKNOWN-error branch passed the
  uncalled function object as the timestamp).
- **F2** — `datasource/status.py`: version-gate the post-`upsert` dependency / flag-change
  notification (resolves the `sc-212471` TODO; stops spurious change events on version-rejected
  upserts). Minimal change to `upsert()` only — changes flag-change-event behavior, so review it.
- **F3** — `datasourcev2/polling.py`: `f"HTTP error {response}"` → `{response.status}` (two sites).
- **R7 (fix portion)** — `datasource/polling.py`: set the ready event on an unrecoverable error so
  `init()` can't hang.

### Phase 0b — Impl-branch reverts (✅ done, not a separate PR)

Reverted the gratuitous churn so the foundation PR's sync footprint is *only* the shared-`_common`
extraction. **Validated: 1345 unit, mypy/isort clean, both contract suites 4670.** Outcome:

- **Byte-for-byte `main`:** `evaluator.py`, `big_segments.py`, `feature_store.py`,
  `feature_requester.py`, `flag_tracker.py`, `datasource/streaming.py`, `datasourcev2/streaming.py`,
  `integrations/redis/redis_big_segment_store.py`.
- **Extraction-only (no churn):** `client.py`, `event_processor.py`, `fdv2.py` — each just routes to
  its shared `_common` module; `__start_up`/`__register_plugins` keep `main`'s double-underscore names
  (and the new async side was aligned to those names).
- **`main` + a kept fix:** `polling.py` (+F1 +R7-fix), `status.py` (+minimal F2),
  `datasourcev2/polling.py` (+F3).
- **Async dead-code removed:** `close_sse_pool`/`SSEPool`/`AsyncSSEFactory.pool` chain, the
  accept-and-ignore `AsyncHTTPTransport(target_base_uri=…)` and `.request(retries=…)` params, async
  `_running`, and the async `BigSegmentStoreManager.close()` alias (tests now call `stop()`).
- **Notable findings:** the `_run_guarded`/`_spawn_guarded` wrappers were redundant (`main`'s event
  processor and FDv2 thread targets already log their own exceptions); `feature_store.close()`,
  `feature_requester` injectable `http_client`, and `big_segments.close()` were all gratuitous
  (unused / hasattr-guarded / not in the ABC). Redis `decode_responses=True` (a user `redis_opts`
  escape hatch `main`'s sync never supported) → both stores stay bytes-only; str/bytes tolerance can
  be added to both sides later as a non-breaking feature if requested.

---

## Stacked-PR extraction plan

Phase 0a is merged (#447). Next the **foundation tier** lands as two PRs — the async shim layer, and
the `_common` extraction + sync routing. After that the async siblings slice cleanly behind the
interfaces.

### PR 0 — Release `python-eventsource` ✅ DONE (prerequisite, different repo)
Published as **1.7.1** — adds `AsyncSSEClient` (marked Experimental) and includes the SDK-2600
pool-ownership / synchronous-close fix. The SDK pin bump to `launchdarkly-eventsource =
">=1.7.1,<2.0.0"` already landed with Phase 0a (#447), and the impl branch carries no
`[tool.uv.sources]` path, so nothing remains to extract here.

### PR 1 — Foundation tier ⚠ FOUNDATIONAL (split into two PRs)
The architecture both the sync code (in `main`) and every async sibling depend on. ~1,700 lines as
one PR, so it's split along its natural seam — **pure-additive infra** vs **released-code refactor**.
The two are independent (the async support classes don't import the cores; the cores are sans-I/O and
don't import them), so they can land in either order. Both are sync-side-only and precede all async
work (everything from PR 2 onward depends on this tier). **Actual PR titles use conventional-commit
subjects, not "1a/1b" labels.**

**1a — Async support classes** (`impl/aio/`; ~560 new lines; touches no released code; low-risk review)
Files: `impl/aio/__init__.py`, `concurrency.py`, `transport.py`, `transport_types.py` + a new
`testing/test_aio.py` — these currently ship **untested**, so this PR adds their unit tests. The
package is dormant until its first consumer lands, so it adds no runtime surface to `main` beyond the
new (as-yet-unimported) modules. The PR description should summarize what will consume them later:
- `concurrency.py` — async concurrency primitives (`AsyncEvent`, `AsyncLock`, `AsyncQueue`,
  `AsyncRepeatingTask`, `AsyncWorkerPool`, `AsyncCallbackScheduler`, `AsyncTaskRunner`,
  `spawn_handle`/`join_handle`). First consumed by async big segments (PR 4) and async flag tracker
  (PR 5); then the event processor (PR 8), the FDv1/FDv2 polling + streaming sources, and the
  FDv1/FDv2 data systems.
- `transport.py` — async HTTP/SSE transport (`AsyncHTTPTransport`, `AsyncSSEFactory`,
  `make_client_session`). First consumed by async FDv1 streaming (PR 6) and FDv1 polling (PR 7);
  then the event processor (PR 8) and the FDv2 sources (PR 11).
- `transport_types.py` — shared transport response types.
This PR also carries the async packaging in `pyproject.toml` — the `[async]` aiohttp extra plus
`aiohttp` / `pytest-asyncio` dev deps and `asyncio_mode="strict"` — because the tests import aiohttp
and run under pytest-asyncio, so they can't pass in CI without it. (The `redis` bump is deferred to
the async-redis slice, PR 4; **no** `[tool.uv.sources]` path; **no** version downgrade.)
Open as **PR #451** (was titled "async shim layer"; re-title/-describe to "async support classes"
and drop the `resolve`/`iterate`/`store_get`/`store_all`/`AsyncRWLock` references — those bridges no
longer exist).
Dependencies: none.

**1b — `_common` extraction + sync routing + config read-protocols ✅ MERGED (#450)**
Files: `impl/client_common.py`, `impl/datasystem/fdv2_common.py`, `impl/events/event_processor_common.py`;
the sync refactors `client.py`, `impl/events/event_processor.py`, `impl/datasystem/fdv2.py` routing
through those cores (extraction-only — sync behavior identical); the config read-protocols
(`SdkIdentityConfig`/`DataSourceBuilderConfig`/`PrivateAttributesConfig`) + `DataSourceBuilder.build()`
widening; `CONTRIBUTING.md` sync/async parity note.
Dependencies: none.

### PR 2 — `AsyncFeatureStore` interface + `AsyncInMemoryFeatureStore` + async test utilities
- `interfaces.py` (async ABCs/protocols: `AsyncFeatureStore`, `AsyncReadOnlyStore`,
  `AsyncDataSourceUpdateSink`, `AsyncBigSegmentStore`, `AsyncInitializer`, `AsyncSynchronizer`)
- `async_feature_store.py` (no lock — the event loop makes its non-awaiting critical sections
  atomic), `testing/mock_async_components.py`, `testing/async_feature_store_test_base.py`,
  `testing/test_async_in_memory_feature_store.py`
- Async-typed null objects in `impl/stubs.py` (`AsyncNullEventProcessor`, `AsyncNullUpdateProcessor`)
  so client/data-system teardown is uniform `await`.
- Async TestData source: `impl/integrations/test_datav2/async_test_data_sourcev2.py` +
  `TestDataV2.async_builder`, so the async data-system tests drive a genuinely-async source.
- Depends on PR 1.

### PR 3 — `AsyncEvaluator`
- `impl/async_evaluator.py`, `testing/impl/test_async_evaluator.py`. Depends on PR 2.
- Also introduced `impl/evaluator_common.py`: moved the pure, I/O-free evaluator internals that
  both evaluators shared verbatim — `EvalResult`, `EvaluationException`, the module constants, and
  the 14 stateless helper functions (bucketing, clause/target matching, variation resolution). Both
  `evaluator.py` and `async_evaluator.py` now import them (touches released sync `evaluator.py`).

## Deferred sync/async dedup — REVIEW BEFORE PROCEEDING PAST PR 3
An audit of all sibling pairs (after the evaluator dedup) found two more genuine pure-duplication
cases. Both are real but need a mixin/base-class refactor rather than a straight move, so they were
deferred to land with their consuming slices — revisit these before/when those PRs are built:
- **Client event methods** — `client.py` ↔ `async_client.py`: `track`, `identify`,
  `track_migration_op` are byte-identical and pure (~55 lines) but are instance methods reading
  `self._config` / `self._event_factory_default` / the event processor. Sharing needs a
  `_ClientEventMixin` (in `client_common.py`), not a move. Natural fit with **PR 10** (`AsyncLDClient`).
- **Data-source status helpers** — `impl/datasource/status.py` ↔ `async_status.py`:
  `__update_dependency_for_single_item`, `__reset_tracker_with_new_data`, `__send_change_events`,
  `__compute_changed_items_for_full_data_set` are near-identical and pure (~40 lines), and the
  provider class differs only in name + `update_sink` type. But they are name-mangled `self`-bound
  privates → would need converting to free functions or a shared base. Fits with **PR 7**
  (async data-source status). More invasive; decide deliberately.
- Audit rejected as NOT shareable (divergent types / `await` / async-removed): migrator builders,
  `config`/`async_config` (already shares via imports), datasourcev2 streaming/polling constants,
  and the data-source/store/event-processor bodies.

### PR 4 — `AsyncBigSegmentStoreManager` + async Redis adapter
- `impl/async_big_segments.py`, `impl/integrations/redis/async_redis_big_segment_store.py`,
  `integrations/__init__.py` (`Redis.async_big_segment_store(...)`), + tests. Depends on PR 1, PR 2.

### PR 5 — `AsyncHook`, `AsyncPlugin`, `AsyncFlagTracker`
- `hook.py` (`AsyncHook`), `plugin.py` (`AsyncPlugin`), `impl/async_flag_tracker.py`, + tests.
  Depends on PR 2 (uses `shims/aio.py` from PR 1).

### PR 6 — Async FDv1 streaming data source
- `impl/datasource/async_streaming.py` + test. (PR 0 done; eventsource `>=1.7.1` pin already in
  `main`. The data source builds its session via `make_client_session` for proper certs/SSL/proxy
  and owns its close, mirroring the FDv2 async source and SDK-2600's ownership model.)
  Depends on PR 1 (transport shims), PR 2.

### PR 7 — Async FDv1 polling + async data-source status
- `impl/datasource/async_feature_requester.py`, `async_polling.py`, `async_status.py` (async twin of
  the refactored sync `status.py`) + tests. Depends on PR 1, PR 2. (`async_status` could move to PR 2
  if both streaming and polling need it first.)

### PR 8 — Async event processor
- `impl/events/async_event_processor.py` + test (`event_processor_common` already in PR 1).
  Depends on PR 1, PR 2.

### PR 9 — Async migrations
- `migrations/async_migrator.py` (`AsyncMigrator`/`AsyncMigratorBuilder`/`AsyncMigratorFn`),
  `migrations/__init__.py` eager export, + test. Depends on PR 3; sequence before PR 10 so
  `AsyncLDClient.migration_variation()` can call into it.

### PR 10 — `AsyncLDClient` + FDv1 wiring + public API + contract service
- `async_client.py`, `impl/datasystem/async_fdv1.py`, `impl/datasystem/__init__.py`
  (`AsyncDataSystem` Protocol), `ldclient/__init__.py` (lazy `__getattr__`),
  `contract-tests/async_service.py` (FDv1 + hooks + migrations), `testing/test_async_client.py`.
  Depends on PRs 1–9. `migration_variation()` is real, not a stub.

### PR 11 — Async FDv2 data system  (largest — recommend splitting)
- `impl/datasourcev2/async_streaming.py`, `async_polling.py`, `impl/datasystem/async_fdv2.py`,
  `async_client.py` `start()` FDv2 selection, contract FDv2 handlers, + tests (~1,900 LOC).
  Depends on PR 1 (`fdv2_common`), PR 10. **Split** into PR 11a (sources) + PR 11b (coordinator +
  client wiring + contract handlers).

### Dependency graph
```
PR 1 foundation tier (2 PRs: shim layer; _core extraction + sync routing)  ← critical path, lands first
  └─ PR 2 (async store iface + in-mem + test utils)
       ├─ PR 3 (async evaluator) ──────────────┐
       ├─ PR 4 (async big segments + redis)     │
       ├─ PR 5 (async hook/plugin/flagtracker)  │
       ├─ PR 6 (async FDv1 streaming)  (PR 0 done — eventsource 1.7.1 shipped)
       ├─ PR 7 (async FDv1 polling + async_status)
       ├─ PR 8 (async event processor)          │
       └─ PR 9 (async migrations) ──────────────┤
                                                 ▼
                              PR 10 (AsyncLDClient + FDv1 + contract svc)
                                                 ▼
                              PR 11 (async FDv2)  [split 11a sources / 11b coordinator]
```

---

## Before GA — breaking-change review (SDK-34)

Once all the PRs above are created, review the **SDK-34** epic ("Python Server SDK Breaking") for
additional breaking-change / cleanup items worth bundling **before the async work ships**. The async
surface is brand new, so the major version that GAs it is the natural place to also land related
breaking changes rather than spreading them across releases. Concretely: walk SDK-34's children
(data-store / internal-API redesign, removing deprecated things, breaking DB integrations into
packages, etc.) and **SDK-62** (sync `FeatureStore.upsert -> bool`, to align sync with the async
contract already adopted here) and decide what to fold in. Output: a short list of in-scope items
appended to this plan, then sliced like the rest.

---

## Branch state — extraction notes

The dev-only state earlier flagged here is **already resolved** (no extraction-time surgery needed):
`version.py` / `pyproject.toml` are at `9.16.0`, matching `main` (release-please owns the bump — don't
touch it in PRs), and there is **no** `[tool.uv.sources]` local path. The `pyproject.toml` diff vs
`main` is entirely real, mergeable packaging for the async feature — eventsource `>=1.7.1,<2.0.0`,
the `[async]` aiohttp extra, the `redis` bump, `pytest-asyncio`, and `asyncio_mode="strict"` — and
belongs in PR 1 as-is.

The only branch-only artifact is **this plan file** (`async-plan.md`); never merge it to `main`.
Everything else on the branch is production code/tests/docs to be extracted verbatim — **PRs pull
code out, they don't author new work.**

## Contract test commands
```
# sync  (service on 8001, harness reporting port 8111)
uv run --group contract-tests python contract-tests/service.py 8001
sdk-test-harness/main/bin/sdktest -url http://localhost:8001 -port 8111
# async (service on 8002, harness reporting port 8112)
uv run --group contract-tests python contract-tests/async_service.py 8002
sdk-test-harness/main/bin/sdktest -url http://localhost:8002 -port 8112
```
Both currently report: 4681 total, 11 skipped, **4670 ran, all passed**.
