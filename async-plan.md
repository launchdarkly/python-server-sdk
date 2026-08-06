# Async Python Server SDK — Plan

**Epic:** SDK-60. **Implementation branch:** `jb/sdk-60/async-python-sdk` (single squash commit).

This is the single canonical plan: the architecture, the implemented-behavior reference, and the
stacked-PR extraction plan. The implementation is complete and validated; what remains is slicing it
into reviewable PRs.

**Status (current):** the full async implementation lives on the impl branch and is validated —
mypy + isort + pycodestyle clean, unit suite green, both contract suites pass. **Phase 0 complete**
(0a sync fixes merged #447; 0b reverts done). Extraction is well underway:

- ✅ **Merged to `main`:** PR 1 foundation (1a support classes #451, 1b `_common` + sync routing
  #450), PR 2 async feature store (#457), PR 3 async evaluator (#460), PR 4 async big segments
  (#462, SDK-2729), PR 5 async hook/plugin/flag tracker (#463, SDK-2737), `AsyncConfig` (#471,
  SDK-2768), **PR 6 async FDv1 streaming + data-source status (#464, SDK-2743)**, **PR 9 async
  migrations (#470, SDK-2767)**, + dead-`__BUILTINS__` cleanup (#461), the import-safety guard
  (#478), the async-redis comment cleanup (#479), and infra chores (Renovate #469, checkout #467,
  setup-uv #477).
- 🔎 **Open (in review):**
  - PR 7 async FDv1 polling + feature requester (#475, SDK-2825) — was stacked on PR 6/#464; now that
    #464 is merged, rebased `--onto origin/main <old-#464-tip>` so it shows only its own commits.
  - PR 8 async event processor (#472, SDK-2769) — rebased onto latest `main` (plain rebase; branches
    off `main` directly).
- ⏳ **To create:** PR 10 (`AsyncLDClient` + client event methods + README async section +
  `flush_and_wait` client), PR 11 (FDv2, split 11a/11b), + a small standalone evaluator
  `check_targets` cleanup (home PR #460 already merged, so it needs its own PR).
- **Impl is rebased onto `main` after every slice merge.** Rebases have absorbed the release version
  drift, eventsource `1.7.2`, and **#456**'s removal of the SDK-side FDv1/FDv2 streaming pool close,
  all cleanly (the merged slices' changes drop out via 3-way). **Needs a fresh rebase now that
  #464/#470/#478/#479 have merged.**
- **Stacked-rebase gotcha:** slices are **squash-merged**, so a child stacked on a merged parent must
  be rebased with `git rebase --onto origin/main <old-parent-tip>` — GitHub's "rebase stack" button
  and a plain `git rebase main` both try to replay the parent's now-squashed commits and conflict.
  PR 7/#475 hit this (stacked on the now-merged #464); PR 8/#472 branches off `main` directly, so a
  plain rebase suffices for it.
- **Deferred follow-ups:**
  - **Flaky CI test (leaked sync polling thread).** `test_ldclient_evaluation.py::test_variation[_detail]_when_feature_store_throws_error`
    flakes (mostly `windows (3.14)`; also seen on other jobs) from a sync polling `RepeatingTask`
    that outlives its test — `RepeatingTask.stop()`/`close()` set the stop event but never **join**
    the daemon worker, so an orphaned poller hits a torn-down `start_server` port and its
    connection-error log lands in the next test's process-wide `caplog`, breaking its exact-match
    `errlog` assertion. Pre-existing (on `main` since 2026-07-06; **not** caused by the async work).
    **Chosen fix (deferred):** make sync teardown synchronous — `stop()`/`close()` join the worker
    with a bounded timeout (the async side already has `AsyncRepeatingTask.wait_stopped()`). Rejected:
    loosening the assertion (masks the leak) and per-client loggers (too big). Meanwhile: rerun the
    job to unblock. Own change off `main`, no Jira yet.
  - **Sync event processor: same two shutdown bugs as async (fixed in PR 8/#472).** The sync
    `DefaultEventProcessor` (`impl/events/event_processor.py`) has the identical dispatcher shape, so
    it shares both: (A) if `_do_shutdown` raises, the loop logs and continues without setting the stop
    reply or returning → `stop()` waits on that reply forever; (B) `_do_shutdown` never drains the
    outbox, so buffered events are lost on shutdown when the pre-stop flush is dropped (inbox full) or
    left buffered (workers saturated). #472 fixes both on the async side (guard the stop branch; drain
    the outbox with retry before stopping workers). **Deferred:** apply the same two fixes to the sync
    processor in its own PR off `main` — reasonable to bundle with the flaky-test sync fix above.
  - **#464 transport review (sooner-than-later).** Matthew's multi-agent review of the merged PR 6/#464
    (`sdk-scratchpad/research/multi-review-python-server-sdk-464.md`) flags three `impl/aio/transport.py`
    findings that surfaced via #475 but belong to #464: `errors='replace'` silent body corruption,
    missing `allow_redirects=False`, and no response body-size cap. #464 is merged → **fix PR off
    `main`**. Read the scratchpad, triage each vs the sync transport, then fix. (The #475 review itself is
    largely already addressed; only its M4 VALID-gate + a few Low nits remained, fixed in PR 7/#475.)
  - **Flaky CI test #2 (Windows short-sleep latency).** `test_async_migrator.py::TestTrackingLatency::test_writes[dualwrite]`
    (and its sibling `test_reads`, plus the sync `test_migrator.py` equivalents) flakes on Windows — seen
    on `windows (3.11)`, run `31032057940`. The test asserts a migration op's measured latency is
    `>= timedelta(milliseconds=100)` around an `asyncio.sleep(0.1)`, but latency is a wall-clock
    `datetime.now()` delta and the Windows event-loop timer resolution (~15.6ms) lets the sleep wake early,
    so the measured value dips just under 100ms (saw ~86ms). Distinct from flaky test #1 above. Added by
    PR 9/#470, pre-existing on `main` (not caused by #472). **Chosen fix (deferred):** loosen the lower-bound
    assertion to absorb Windows timer granularity (e.g. `>= 80ms`) in `test_async_migrator.py` and the sync
    `test_migrator.py`. Own change off `main`, no Jira yet. Meanwhile: rerun the job to unblock.
  - **Standardize async teardown across components (pre-GA, architectural).** Most of the async
    teardown bugs we have hit (swallowed `CancelledError` in `wait_stopped`, the `BoundedTaskSet`
    livelock, close-before-drain, close-while-in-flight, leaked aiohttp session) come from each
    component hand-rolling its own multi-step `stop()`/`_do_shutdown()` and re-deriving the ordering
    and cancellation-safety every time. The count of resources per component is not the problem
    (a data source legitimately owns a task + a transport); the ad-hoc-ness is. Even async streaming's
    `stop()` is only leak-safe by luck: `_close_owned_session()` runs in the run task's `_run` finally
    (so a cancelled `stop()` still releases the session), but the parts that live only in `stop()`'s
    second await — the redundant `sse.close()` and the status update to `OFF` — are skipped if `stop()`
    is cancelled. Polling had the same hole (fixed interim with Option A: `try: wait_stopped() finally:
    requester.close()` in PR 7/#475).
    **DECISION (settled).** The async client is **single-event-loop / not-thread-safe** — the ecosystem
    contract (aiohttp / asyncpg / httpx / redis.asyncio, and Python's own asyncio docs). Cooperative
    atomicity (code between `await`s is not preempted) replaces most locks; use `asyncio.Lock` only where
    a critical section genuinely spans an `await`; always let `CancelledError` propagate. **One real
    exception:** the FDv2 persistent-store **availability poller** runs on a real OS thread — it's the
    shared sync `FeatureStoreClientWrapper` (`fdv2_common.py`) polling a *blocking sync* store's
    `is_available()` — so the store / data-store-status / listeners primitives on that path stay
    thread-safe. **Deployment:** `__init__` is loop-free, so construct once (pre-fork / preloaded app) and
    `await start()` per worker loop (ASGI lifespan) → all workers share one `_instance_id` (per-application,
    by design); no `postfork` API needed; uWSGI is not ASGI and is out of scope; public docs = SDK-2879.
    **Evaluate-before-ready** returns `default` + `CLIENT_NOT_READY` (the spec norm across sync Python /
    Go / .NET / Ruby / PHP + the sdk-test-harness client-not-ready contract) — accepted as-is, just documented.
    **Applied on impl (this pass, commit on `jb/sdk-60`):** dropped the two **async-only** locks
    (`async_client` hooks → lockless `add_hook` + `list(self.__hooks)` snapshot; async FDv1 `async_status`
    lock); **left the sync-shared locks** (`fdv2_common`, `datastore/status`, `listeners`) because the sync
    SDK drives them from background threads. Also folded in the #480 client review fixes and the #481
    capability trim. **Remaining (future / PR 11):** make the FDv2 availability poller an `AsyncRepeatingTask`
    once a genuine **async persistent store** exists — that removes the last loop↔thread boundary and its
    thread-safe primitives. (Portability note: `requires-python >=3.10`, so `asyncio.TaskGroup`/`asyncio.timeout()`
    are unavailable; `AsyncExitStack` + `gather(..., return_exceptions=True)` are the portable tools.)

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
  every async **public class, entry point, and method** carries a `.. caution::` "experimental … not
  ready for production … no backwards-compat guarantees" block (verbatim 1.2.1 wording), mirroring
  eventsource 1.7.1's `AsyncSSEClient`. **Rule: a class-level caution block covers all of that class's
  methods — do not repeat it per method.** A public **async method added to a non-experimental (sync)
  class** carries its own **method-level** caution block, since the class itself has none (e.g.
  `Redis.async_big_segment_store()`, `TestDataV2.async_builder`). Done on the impl branch for
  `AsyncLDClient`, `AsyncInMemoryFeatureStore`, `AsyncHook`, `AsyncPlugin`, `AsyncFlagTracker`,
  `AsyncMigrator`/`AsyncMigratorBuilder`, the async interface ABCs (`AsyncFeatureStore`,
  `AsyncBigSegmentStore`, `AsyncDataSourceUpdateSink`, `AsyncInitializer`, `AsyncSynchronizer`), and
  the standalone methods `Redis.async_big_segment_store()` / `TestDataV2.async_builder`. Each async PR
  must keep the block on any public class or method it introduces. The README also carries an "Async
  support (experimental)" section with the matching `> [!CAUTION]` block (extract with the client
  slice, PR 10).
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

**1a — Async support classes ✅ MERGED (#451)** (`impl/aio/`; ~560 new lines; touches no released code)
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

### PR 2 — `AsyncFeatureStore` interface + `AsyncInMemoryFeatureStore` + async test utilities ✅ MERGED (#457)
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

### PR 3 — `AsyncEvaluator` ✅ MERGED (#460)
- `impl/async_evaluator.py`, `testing/impl/test_async_evaluator.py`. Depends on PR 2.
- Also introduced `impl/evaluator_common.py`: moved the pure, I/O-free evaluator internals that
  both evaluators shared verbatim — `EvalResult`, `EvaluationException`, the module constants, and
  the 14 stateless helper functions (bucketing, clause/target matching, variation resolution). Both
  `evaluator.py` and `async_evaluator.py` now import them (touches released sync `evaluator.py`).
- **Post-merge follow-up:** a later dedup pass extracted the `check_targets` helper into
  `evaluator_common.py` too (both evaluators now share it). Since #460 already merged, this rides on
  impl and needs its **own small standalone cleanup PR** against `main`.

## Sync/async dedup — status (pre-PR-3 gate actioned)
The "review before PR 3" gate has been worked. A follow-up **class-level re-audit** found ~380–400
lines of duplication the file-level pass missed; that batch landed on impl (big-segment status
provider → `big_segments_common`, evaluator `check_targets`, `EventDispatcherBase` →
`event_processor_common`, datasourcev2 polling/streaming + fdv2 conditions, datasource-v1
`sink_or_store`/`parse_path` → `datasource_common`, migrations config + byte-identical builder
setters). Status of the originally-deferred items:

- **Data-source status PROVIDER — ✅ DONE (PR 6 / #464).** `DataSourceStatusProviderImpl` is now
  shared: the async data system reuses the sync class, whose `update_sink` param was widened to a
  `status`-only `_DataSourceStatusSource` `Protocol` (interface-segregation, mirroring the config
  read-protocols). No async provider class exists.
- **Data-source status SINK helpers — ⏳ STILL DUPLICATED (open decision).**
  `__update_dependency_for_single_item`, `__reset_tracker_with_new_data`, `__send_change_events`,
  `__compute_changed_items_for_full_data_set` remain name-mangled `self`-bound privates in *both*
  `status.py` and `async_status.py` (~40 lines). Both files are now in **PR 6 (#464)**, so extracting
  them (free functions or a shared base) could land there — or be left as acceptable duplication.
  **Decide deliberately.**
- **Client event methods — ⏳ STILL DEFERRED → PR 10.** `track`/`identify`/`track_migration_op` are
  still byte-identical in `client.py`/`async_client.py`; no `_ClientEventMixin` yet. Fold into PR 10.
- Audit rejected as NOT shareable (divergent types / `await` / async-removed): migrator
  `read`/`write`/`build`, `config`/`async_config`, datasourcev2 streaming/polling constants, and the
  data-source/store/event-processor bodies. (The migrator *config* class + byte-identical builder
  *setters* WERE shared — see the batch above; only `read`/`write`/`build` stayed divergent.)

### PR 4 — `AsyncBigSegmentStoreManager` + async Redis adapter ✅ MERGED (#462, SDK-2729)
- `impl/async_big_segments.py`, `impl/integrations/redis/async_redis_big_segment_store.py`,
  `integrations/__init__.py` (`Redis.async_big_segment_store(...)`), + tests. Depends on PR 1, PR 2.
- Also carries (from review): the `redis` pin decision (published `[redis]` floor kept at `>=2.10.5`,
  only the dev/test dep bumped to `>=4.2`; async-redis `>=4.2` documented in README + enforced by the
  adapter's guarded import — floor bump deferred pending a team decision before GA); `get_status`
  simplified to `self.__last_status or BigSegmentStoreStatus(False, False)`; async redis `stop()`
  supports redis-py `<5.0.1` (`aclose`/`close` fallback); the `DataSourceStatusProviderImpl` dedup;
  and the `BigSegmentStoreStatusProvider.status` sync-vs-async docstring note (`interfaces.py`).

### PR 5 — `AsyncHook`, `AsyncPlugin`, `AsyncFlagTracker` ✅ MERGED (#463, SDK-2737)
- `hook.py` (`AsyncHook`), `plugin.py` (`AsyncPlugin`), `impl/async_flag_tracker.py`, + tests.
  Depends on PR 2 (uses `impl/aio/` from PR 1). `AsyncFlagValueChangeListener._on_flag_change` widened
  its lock to serialize overlapping re-evaluations (review fix).

### PR 6 — Async FDv1 streaming + data-source status ✅ MERGED (#464, SDK-2743)
- **Scope grew during extraction** beyond "streaming only": now `impl/datasource/async_streaming.py`
  + `impl/datasource/async_status.py` (the streaming source pushes into its update sink, so they're
  runtime-coupled) + the shared sans-I/O `impl/datasource/datasource_common.py`
  (`STREAM_ALL_PATH`/`sink_or_store`/`parse_path`) with the behavior-preserving routing of sync
  `streaming.py`/`polling.py` + the `DataSourceStatusProviderImpl` provider dedup + tests. The source
  builds its session via `make_client_session` and owns its close (mirrors the FDv2 async source /
  SDK-2600). Depends on PR 1, PR 2. Review fixes that landed on the branch: streaming `stop()` cancels
  the run task before teardown (startup race); `AsyncFeatureStore.delete` returns `bool` and stale
  deletes no longer notify listeners; and `init` monitors the prior-data read for store errors (async
  `__monitor_store_update`, mirroring sync).

### PR 7 — Async FDv1 polling + feature requester ✅ MERGED (#475, SDK-2825)
- `impl/datasource/async_feature_requester.py`, `async_polling.py` + tests. **`async_status` and
  `datasource_common` are in PR 6 (#464); `AsyncConfig` was split out and merged separately (#471)**,
  so PR 7 carried only polling + the feature requester. Review fixes that landed on the branch:
  `AsyncFeatureRequester` interface added (with `close()`); polling reports `VALID` on every
  successful poll (M4); `wait_stopped()` uses `asyncio.wait({task})` so it does not swallow the
  caller's cancellation; and `stop()` closes the transport in a `finally` so a cancelled `stop()`
  does not leak the owned transport.

### PR 8 — Async event processor ✅ MERGED (#472, SDK-2769)
- `impl/events/async_event_processor.py` + test (`event_processor_common` already in PR 1). Also adds
  `flush_and_wait(timeout) -> bool` on the concrete (the interface method rode along in the
  `AsyncConfig` PR #471, since it owns `interfaces.py`). Depends on PR 1, PR 2, `AsyncConfig` (#471 ✓).
  Unblocked and mergeable.

### PR 9 — Async migrations ✅ MERGED (#470, SDK-2767)
- `migrations/async_migrator.py` (`AsyncMigrator`/`AsyncMigratorBuilder`/`AsyncMigratorFn`),
  `migrations/__init__.py` eager export, + test. Depends on PR 3; sequence before PR 10 so
  `AsyncLDClient.migration_variation()` can call into it.

### PR 10 — `AsyncLDClient` + FDv1 wiring + public API + contract service — SPLIT into a stacked pair
Depends on PRs 1–9 (all merged). `migration_variation()` is real, not a stub. Split along the
SDK-vs-test-harness seam into two **stacked** PRs (10b depends on 10a's client code):
- **PR 10a — `AsyncLDClient` + FDv1 data system + public API — SDK-2867 (Ready for Dev).** `async_client.py`
  (FDv1 only — FDv2 `start()` branch held back for PR 11), `impl/datasystem/async_fdv1.py`,
  `impl/datasystem/__init__.py` (`AsyncDataSystem` Protocol), `ldclient/__init__.py` (lazy `__getattr__`),
  async null-object stubs, the `track`/`identify`/`track_migration_op` client event-mixin dedup,
  `testing/test_async_client.py`. Base `main`.
- **PR 10b — async contract-test service (FDv1) — SDK-2868 (Ready for Dev).** `contract-tests/async_service.py`
  (FDv1 + hooks + migrations) + `contract-tests/hook.py`; FDv2 handlers held back for PR 11. **Stacked on
  10a** (base = 10a's branch); rebase onto `main` after 10a merges.

### PR 11 — Async FDv2 data system (largest) — SPLIT into a stacked pair
~1,900 LOC total. Split into sources + coordinator:
- **PR 11a — async FDv2 data sources — SDK-2869 (Ready for Dev).** `impl/datasourcev2/async_streaming.py`,
  `async_polling.py`, sans-I/O `polling_common.py`/`streaming_common.py` + behavior-preserving sync
  routing, + tests. Base `main` (depends on the merged async transport/store, not on PR 10).
- **PR 11b — async FDv2 coordinator + client wiring — SDK-2870 (Ready for Dev).** `impl/datasystem/async_fdv2.py`,
  `fdv2_common.py` + sync `fdv2.py` routing, `async_client.py` `start()` FDv2 selection (held back from 10a),
  contract-test FDv2 handlers (held back from 10b), + tests. Depends on PR 10a (SDK-2867) and PR 11a (SDK-2869).

### Cleanup — Deduplicate shared sync/async evaluator + async test setup — SDK-2871 (Ready for Dev)
Independent of the PR 10/11 stack (touches only files already on `main`): extract `check_targets` into
`evaluator_common.py` (shared by sync + async evaluators) and remove duplicated setup from the async test
files. Own PR off `main`, not stacked.

### Dependency graph
```
PR 1 foundation (1a support classes #451 ✓ · 1b _common + sync routing #450 ✓)      ← merged
  └─ PR 2 async store iface + in-mem + test utils (#457 ✓)                            ← merged
       ├─ PR 3 async evaluator (#460 ✓)                                               ← merged
       ├─ PR 4 async big segments + redis (#462 ✓)                                    ← merged
       ├─ PR 5 async hook/plugin/flag tracker (#463 ✓)                                 ← merged
       ├─ PR 6 async FDv1 streaming + data source status (#464 ✓)                       ← merged
       └─ PR 9 async migrations (#470 ✓)                                                ← merged
                       AsyncConfig (#471 ✓ merged)
                          ├─ PR 8 async event processor (#472 ✓)                        ← merged
                          └─ PR 7 async FDv1 polling + feature requester (#475 ✓)       ← merged
                                                 ▼
                              PR 10 (AsyncLDClient + FDv1 wiring + client event methods + contract svc)  ← NEXT (PRs 1–9 all merged)
                                                 ▼
                              PR 11 (async FDv2)  [split 11a sources / 11b coordinator]  ← needs PR 10
```

**Blocker status — cleared. PRs 1–9 are all merged** (foundation #450/#451, store #457, evaluator
#460, big segments #462, hook/plugin/tracker #463, streaming #464, event processor #472, migrations
#470, polling #475, plus `AsyncConfig` #471). **PR 10 is now unblocked and is the next slice to
extract** — it depends only on PRs 1–9. Impl is rebased on the post-#475 `main` (`347e564`); its diff
vs `main` is now just the unmerged PR 10 + PR 11 surface (async client, FDv1/FDv2 data systems,
FDv2 sources, contract service, client event methods) plus a one-line `AsyncRepeatingTask._run`
cleanup.

---

## Parked follow-ups (not blocking the async slices)

- **Flaky sync test `test_variation_when_feature_store_throws_error`** — pre-existing on `main`,
  surfaced by #464's Windows-3.14 CI. Root cause: the test asserts *exact equality* on **all**
  captured ERROR log records (`get_log_lines` filters by level only), and a background polling thread
  from an *earlier* test finishes a failing connect seconds later — leaking its
  `"Exception encountered when updating flags … connection refused"` log into this test's caplog.
  `RepeatingTask.stop()` (`repeating_task.py:34-38`) sets an event but never `join()`s the worker and
  can't interrupt an in-flight blocking `get_all_data()`, so `close()` can leave the poll thread
  lingering. **Fix:** assert *membership* of the expected record instead of full-list equality
  (`test_ldclient_evaluation.py:137-138` and the sibling at `:148-149`). Its own small PR against
  `main` (the test is unchanged on the async branch — #464 just surfaced it). Optional broader fix:
  make `RepeatingTask.stop()` join the thread / add a socket timeout so `close()` leaves no lingering
  poll thread (a minor product-side lifecycle wart; separate ticket).

- **Async flag-value-change-listener "missed update during subscription"** (SDK-2766, from #463
  review). `AsyncFlagValueChangeListener` captures its baseline in `create()` (`await eval_fn`) before
  registering, so a flag change during that `await` window is missed by the not-yet-registered
  listener. Mirrors the sync `FlagValueChangeListener` (accepted behavior), but the async `await`
  widens the window. **Decide before GA:** accept (sync parity) or close it (register first, then
  capture the baseline under the listener's lock — careful of a spurious first notification). Left
  as-is for now; the async flag tracker is experimental.

- **Store-error kind overwritten by streaming's broad handler** (Bugbot on #464). When the update
  sink reports `STORE_ERROR` and re-raises, the streaming processor's broad `except Exception` calls
  `update_status(INTERRUPTED, UNKNOWN)`, replacing the more specific `STORE_ERROR` kind. **This is not
  async-specific** — the sync sink's `__monitor_store_update` also re-raises and sync `run()` has the
  identical broad handler, so both SDKs overwrite the kind. Left as-is on the async side to preserve
  sync parity; if worth correcting it should be fixed in **both** sync and async as its own change
  (e.g. have the streaming handler skip the generic status update when the sink already recorded a
  more specific error). Replied on the #464 thread.

- **Async persistent feature stores not yet supported.** The only async store on the branch is
  `AsyncInMemoryFeatureStore` (plus test subclasses). The sync SDK ships persistent stores — Redis,
  DynamoDB, Consul — built on `CachingStoreWrapper` + `FeatureStoreCore` for daemon-mode / Relay-Proxy
  / shared-across-process setups. The async side has no equivalent: no async `CachingStoreWrapper`, no
  async `FeatureStoreCore`, no async persistent adapters. This was missed during scoping; needed
  before GA for parity. **Follow-up:** design and add the async persistent-store stack. While doing
  so, fix the `AsyncFeatureStore` interface docstring (`interfaces.py:341`) — it was copied verbatim
  from the sync `FeatureStore` and still says objects are "simply a dict of arbitrary data," which is
  wrong now that async stores hold decoded model objects (the `_get_store_item` dict shim was dropped).

- **Remove `is_alive` / `BackgroundOperation` from the sync interfaces** (SDK-2773, under SDK-34).
  `is_alive` is a 2015 fossil of the removed Twisted async implementation — nothing calls
  `update_processor.is_alive()` (the only live `.is_alive()` calls are stdlib `Thread.is_alive()` in
  FDv2). The new `AsyncUpdateProcessor`/`AsyncEventProcessor` interfaces already omit it; dropping it
  from the public sync `UpdateProcessor`/`BackgroundOperation` is a minor breaking change deferred to
  the next major.

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
