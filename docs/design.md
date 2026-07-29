# Design: Zig client SDK for Azure Event Hubs (`eventhubs-zig`)

## Understanding

The task is to design a Zig 0.16.0 client SDK for Azure Event Hubs and to break the work into GitHub issues. The design document is the source for one root tracking issue plus child issues in the repository https://github.com/j7nw4r/eventhubs-zig. A different agent implements each issue without access to this conversation.

The repository ships **two publishable libraries as two Zig modules in one package**:

- `amqp`: a standalone, general-purpose AMQP 1.0 client library. It has no Event Hubs dependency and no Event Hubs concepts in its API. A third party can use it alone with `@import("amqp")`.
- `eventhubs`: the Event Hubs SDK, built on the `amqp` module. Consumers write `@import("eventhubs")`.

The layout is one repository, one CI, one release tag, one issue tree:

```
j7nw4r/eventhubs-zig
|-- src/amqp/        -> b.addModule("amqp")
|-- src/eventhubs/   -> b.addModule("eventhubs")   (imports the amqp module)
`-- build.zig
```

The package name in `build.zig.zon` is `eventhubs`.

"GA" means version 1.0.0 with this surface.

For the `eventhubs` module:

- Connect with a connection string (SAS, HMAC-SHA256) or with an Entra ID token credential.
- TLS on port 5671. Plain TCP on port 5672 for the local emulator only.
- Producer: build size-checked batches, send to the hub, to a partition, or with a partition key.
- Consumer: a per-partition receiver with event position filters, prefetch credit, owner level (epoch), and last-enqueued-event tracking.
- Management operations: get hub properties and get partition properties.
- Retry policy, error classification, and automatic recovery of connections and links.

For the `amqp` module (the 1.0.0 scope; non-goals are listed in Approach):

- Client (initiator) role over TCP or TLS. SASL ANONYMOUS and SASL PLAIN.
- Connection, session, sender link, receiver link, request/response link pair.
- Full message section codec, multi-frame transfer in both directions, credit-based flow control, idle-timeout heartbeats.
- Receiver settlement: pre-settled deliveries, and unsettled deliveries with accept, release, reject, and modify dispositions.
- Caller-supplied link properties, capabilities, and source filters, so any vendor extension can ride on top.

Acceptance condition, both modules: the v1.0.0 tag exists; `zig build test` passes; `zig build test-interop` passes against a non-Azure AMQP 1.0 broker (Apache ActiveMQ Artemis) in Docker; `zig build test-live` passes against a real Event Hubs namespace; a fresh project can consume each module through `zig fetch` and build against `dep.module("amqp")` alone and against `dep.module("eventhubs")`.

Versioning policy: **lockstep**. One tag and one `.version` in `build.zig.zon` cover both modules. Reason: the Zig package manager fetches and versions a package, not a module; one `build.zig.zon` carries exactly one version, so independent module versions have no mechanism. A future repository split can introduce independent versions if the `amqp` module grows its own audience.

Explicitly out of scope for GA (deliberate deferrals):

- The event processor and its load balancer.
- The blob checkpoint store (needs an Azure Storage client).
- The buffered producer.
- Idempotent publishing and producer owner levels.
- Web sockets transport and proxy support.
- Geo-DR failover handling beyond the capability symbol.
- Kafka surface and ARM management-plane operations.
- Windows support (macOS and Linux only at GA).
- The `amqp` module non-goals listed in Approach (transactions, broker role, SASL EXTERNAL, exactly-once settlement, delivery resumption, dynamic nodes).

## Findings

All paths are relative to `/Users/johnathan/.claude/jobs/7440d8b3/tmp/net-main/sdk/eventhub/` unless marked otherwise.

**Transport and connection.** The .NET client connects with TLS, then runs a SASL ANONYMOUS handshake, then the AMQP open (`Azure.Messaging.EventHubs/src/Amqp/AmqpConnectionScope.cs:1215-1229`, `:502-569`). CBS replaces real SASL authentication; the SASL layer only carries ANONYMOUS (`:34-35`). Defaults: max frame size 64 KiB, ports 5671/5672 (`/Users/johnathan/Dev/azure-amqp/Microsoft.Azure.Amqp/Amqp/AmqpConstants.cs:66-73`), connection idle timeout 60 s (`src/EventHubConnectionOptions.cs:23`). The open frame carries client library properties (`AmqpConnectionScope.cs:1317-1335`).

**Entity paths.** Producer path: `{hub}` or `{hub}/Partitions/{pid}`. Consumer path: `{hub}/ConsumerGroups/{group}/Partitions/{pid}` (`AmqpConnectionScope.cs:47-50`).

**Consumer link attach.** Role receiver, settle mode `SettleOnSend`, `TotalLinkCredit` = prefetch, source filter set with one entry (`AmqpConnectionScope.cs:681-694`). The filter is a described value named `apache.org:selector-filter:string` whose value is a SQL-like string such as `amqp.annotation.x-opt-offset > '...'` (`src/Amqp/AmqpFilter.cs:20-32,56-78`). Earliest is offset `-1`; latest is offset `@latest` (`src/Consumer/EventPosition.cs:20-23`). Link properties: `com.microsoft:entity-type` = 8, `com.microsoft:epoch` (owner level), `com.microsoft:receiver-name`; desired capability `com.microsoft:enable-receiver-runtime-metric` for last-enqueued tracking (`AmqpConnectionScope.cs:696-715`, `src/Amqp/AmqpProperty.cs:20-50`). Default prefetch is 300 (`src/Primitives/PartitionReceiverOptions.cs:30`). Received events are accepted locally; the receive loop tracks the last offset for recovery (`src/Amqp/AmqpConsumer.cs:284-303`).

**Producer link attach.** Role sender, `com.microsoft:entity-type` = 7, `com.microsoft:timeout` property, desired capability `com.microsoft:georeplication` (`AmqpConnectionScope.cs:804-823`). The remote attach supplies max-message-size; the client caches it as the batch size limit (`src/Amqp/AmqpProducer.cs:279-328,607`). A send awaits the disposition outcome; anything other than `accepted` maps to an error (`AmqpProducer.cs:493-505`). Idempotent publishing adds link properties `com.microsoft:producer-id`, `com.microsoft:producer-epoch`, `com.microsoft:producer-sequence-number` (`AmqpConnectionScope.cs:825-833`); it is a bounded extra feature on top of the base producer.

**Batch envelope.** A multi-event batch is one AMQP message with message-format `0x80013700`; each event is a fully encoded AMQP message wrapped in a `Data` section. A single-event batch sends the event message directly. The partition key goes on the envelope as message annotation `x-opt-partition-key` (`src/Amqp/AmqpMessageConverter.cs:305-361`; format constant at `/Users/johnathan/Dev/azure-amqp/.../AmqpConstants.cs:66`). The message-format field is a standard AMQP transfer field; only the constant is vendor-specific.

**System properties on received events.** Message annotations `x-opt-sequence-number`, `x-opt-offset` (a string), `x-opt-enqueued-time`, `x-opt-partition-key`; last-enqueued data arrives in delivery annotations `last_enqueued_sequence_number`, `last_enqueued_offset`, `last_enqueued_time_utc`, `runtime_info_retrieval_time_utc` (`src/Amqp/AmqpProperty.cs:80-123`).

**CBS.** Authorization flows over a request/response link pair on node `$cbs`: application properties `operation=put-token`, `type=servicebus.windows.net:sastoken` or `jwt`, `name={audience}`, and the token string as the body; status-code 202 means success (`/Users/johnathan/Dev/azure-amqp/Microsoft.Azure.Amqp/Amqp/Cbs/CbsConstants.cs:22-73`, `src/Amqp/CbsTokenProvider.cs:23-26`). Refresh happens 7 minutes before expiry, with 0-30 s jitter and a 3-minute floor (`AmqpConnectionScope.cs:73-114,985-1006`).

**SAS token.** `signature = base64(HMACSHA256(key, "{url-encoded audience}\n{unix expiry}"))`; the token string is `SharedAccessSignature sr={enc audience}&sig={enc sig}&se={expiry}&skn={key name}`; default validity 30 minutes (`Azure.Messaging.EventHubs.Shared/src/Authorization/SharedAccessSignature.cs:51,337-354`). Connection string keys: `Endpoint`, `EntityPath`, `SharedAccessKeyName`, `SharedAccessKey`, `SharedAccessSignature`, `UseDevelopmentEmulator` (`src/EventHubsConnectionStringProperties.cs:33` and parser). The Entra scope is `https://eventhubs.azure.net/.default` (`Azure.Messaging.EventHubs.Shared/src/Authorization/EventHubTokenCredential.cs:19`).

**Management operations.** A request/response pair on node `$management`. Request application properties: `operation=READ`, `type=com.microsoft:eventhub` or `com.microsoft:partition`, `name`, `partition`, `security_token` (`src/Amqp/AmqpMessageConverter.cs:121-135,188-205`). The response body is a map with keys `name`, `created_at`, `partition_ids`, `georeplication_factor`, `begin_sequence_number`, `last_enqueued_sequence_number`, `last_enqueued_offset`, `last_enqueued_time_utc`, `is_partition_empty` (`src/Amqp/AmqpManagement.cs:51-118`). The call sits inside a retry loop with error translation (`src/Amqp/AmqpClient.cs:227-309`).

**Errors and retry.** Failure reasons and their transience live in `src/EventHubsException.cs:180-255`. AMQP condition mapping (for example `com.microsoft:server-busy`, `com.microsoft:timeout`, `amqp:link:stolen`, `amqp:resource-limit-exceeded`, `amqp:not-found`) is in `src/Amqp/AmqpError.cs:30-92,174-292`. Defaults: 3 retries, base delay 0.8 s, max delay 60 s, try timeout 60 s, exponential mode (`src/EventHubsRetryOptions.cs:17-27`); delay = `2^attempt * base + random * 0.08 * base` (`Azure.Messaging.EventHubs.Shared/src/Core/BasicRetryPolicy.cs:46,106,209`).

**Live testing in .NET.** Tests read `EVENTHUB_NAMESPACE_CONNECTION_STRING` (`Azure.Messaging.EventHubs.Shared/src/Testing/EventHubsTestEnvironment.cs:25`) and create one Event Hub per test through ARM (`EventHubScope.cs:104-180`); `test-resources.json` stands up a namespace plus a storage account. This ARM-based model is too heavy for a personal repo; the Rust SDK instead points tests at one pre-created hub via env vars (`/Users/johnathan/Dev/azure-sdk-for-rust/sdk/eventhubs/azure_messaging_eventhubs/tests/eventhubs_round_trip.rs:23-24,158`).

**Available live namespace.** A live Event Hubs namespace exists and is reachable on AMQPS port 5671. The namespace is an Event Hubs Dedicated cluster. The connection string lives in 1Password; the CLI reference is `op read "op://Personal/Azure Test Event Hub Namespace Connection String/Connection String"` (item id `56fpr72dhdb6o4xmeppxu5pvem`, vault `Personal`). The connection string must never appear in code, commits, logs, or test output. It reaches the tests through an environment variable only.

**Interoperability broker.** Apache ActiveMQ Artemis states: "The AMQP 1.0 specification is supported."; "By default, there are acceptor elements configured to accept AMQP connections on ports 61616 and 5672."; "The PLAIN, ANONYMOUS, and GSSAPI SASL mechanisms are accepted." Source: https://artemis.apache.org/components/artemis/documentation/latest/amqp.html (fetched 2026-07-29). The Docker image `apache/activemq-artemis` exists on Docker Hub with current tags (2.44.0 at the time of writing); the Artemis Docker documentation (https://artemis.apache.org/components/artemis/documentation/latest/docker.html) documents `ARTEMIS_USER` and `ARTEMIS_PASSWORD` (default `artemis`/`artemis`) and also references an `apache/artemis` image name after the move to a top-level Apache project. The implementing step must pin the exact image tag named by the then-current Artemis docs and record it in the compose file.

**Sizing.** go-amqp, a client-only AMQP 1.0 library without TLS code, is about 12,700 lines of Go. fe2o3-amqp is about 50,000 lines of Rust across three crates, but it implements both peers and full transactions. The Rust Event Hubs client (without AMQP) is about 11,800 lines. The Zig `amqp` module should land near 9,000-13,000 lines (the public bar and dispositions add to the earlier private-subset estimate), and the `eventhubs` module near 5,000-7,000 lines. The AMQP module is the largest single work item, roughly half the project.

**Zig 0.16.0 facts, confirmed against the installed std at `/opt/homebrew/Cellar/zig/0.16.0_1/lib/zig/std/`:**

- Two public modules in one package: `std.Build.addModule` registers a named public module (`Build.zig:909-913`); `Module.CreateOptions` has `imports: []const Import` for `@import` wiring between modules, and `Module.addImport` can add one later (`Build/Module.zig:216-226`). Consumers reach each module with `dependency(...).module("name")`.
- `std.crypto.tls.Client` exists, speaks TLS 1.2 and 1.3, runs over `std.Io.Reader`/`std.Io.Writer`, and supports CA-bundle plus hostname verification (`crypto/tls/Client.zig:87-195`).
- `std.crypto.Certificate.Bundle.rescan` loads system roots, including on macOS (`crypto/Certificate/Bundle.zig:75-91`).
- HMAC-SHA256 is `std.crypto.auth.hmac.sha2.HmacSha256` (`crypto/hmac.zig:11`).
- `std.Io` is the new I/O interface: `io.async`, `io.concurrent`, `Io.Group`, `Io.Queue(T)`, `Io.Mutex`, `Io.Condition`, sleep and timestamps (`Io.zig:1218,1587,2184,2326-2397`). `std.Io.Threaded` is the standard blocking-thread implementation.
- Networking is `std.Io.net`: `IpAddress.connect(io, ...)` returns a `Stream`; `HostName` handles resolution (`Io/net.zig:7,339,1243`).
- Tests get an Io and the environment: `std.testing.io` and `std.testing.environ` (`testing.zig:34-37`); `std.process.Environ.getPosix/getAlloc` read variables (`process/Environ.zig:606,689`). `error.SkipZigTest` skips a test.
- `std.Build.addTest` accepts `filters`; the init templates show the current `addModule`/`createModule` build graph and the `.fingerprint`/`.minimum_zig_version` fields in `build.zig.zon` (`Build.zig:858-887`, `lib/zig/init/`).
- `std.testing.fuzz` exists for in-tree fuzzing (`testing.zig:1227`).

**Emulator.** The Event Hubs emulator image is `mcr.microsoft.com/azure-messaging/eventhubs-emulator`, exposes plain AMQP on port 5672, and depends on Azurite. Source: Microsoft Learn, "Test locally with the Event Hubs emulator" (https://learn.microsoft.com/en-us/azure/event-hubs/test-locally-with-event-hub-emulator) and the MCR artifact page (https://mcr.microsoft.com/en-us/artifact/mar/azure-messaging/eventhubs-emulator/about).

## Approach

**Two public libraries, one package.** `build.zig` declares two modules:

```zig
const amqp_mod = b.addModule("amqp", .{
    .root_source_file = b.path("src/amqp/root.zig"),
    .target = target,
});
const eventhubs_mod = b.addModule("eventhubs", .{
    .root_source_file = b.path("src/eventhubs/root.zig"),
    .target = target,
    .imports = &.{.{ .name = "amqp", .module = amqp_mod }},
});
```

This mechanism is confirmed against the installed std (`Build.zig:909`, `Build/Module.zig:226`). One `build.zig.zon` with package name `eventhubs` and one version covers both modules (lockstep versioning; see Understanding).

**Layer purity rule (mandatory, enforced by CI).** `src/amqp/` must not contain any Event Hubs or Azure concept. Concretely: no `com.microsoft:*` symbol, no `x-opt-*` name, no `$cbs` or `$management` constant, no "Event Hubs" naming, no Azure endpoint knowledge. The `eventhubs` layer passes link properties, desired capabilities, source filters, message annotations, and node addresses into the `amqp` API as caller-supplied values. CI runs a purity check that greps `src/amqp/` for the forbidden strings (`com.microsoft`, `x-opt-`, `eventhub`, `servicebus`, `azure`, case-insensitive) and fails the build on a match. The implementing agent must not take the shortcut of hard-coding a vendor symbol in `src/amqp/`; any vendor constant belongs in `src/eventhubs/`.

**Public bar for the `amqp` module.** The module is a deliverable of its own. Every public declaration carries a doc comment. Public names are stable from the API review step onward. The error model is a clean Zig error set plus remote errors surfaced as data (condition symbol string plus description), with no vendor mapping. Every public function documents its allocator and ownership rules (who allocates, who frees, lifetime of returned slices). Samples use `@import("amqp")` only.

**`amqp` module scope at 1.0.0.** Supported: client (initiator) role; TCP and TLS transports; SASL ANONYMOUS and SASL PLAIN; connection open/close with idle-timeout heartbeats; sessions with window flow control; sender links with credit handling, unsettled transfers, and multi-frame sends; receiver links with credit top-up, multi-frame reassembly, pre-settled deliveries, and unsettled deliveries with accept/release/reject/modify dispositions (rcv-settle-mode first); request/response link pairs; the full message section codec; caller-supplied properties, capabilities, and filters on attach. Explicit non-goals at 1.0.0, each with the reason: transactions (large, no consumer in scope needs them); the broker/listener role (this is a client library); SASL EXTERNAL (needs client TLS certificates, which `std.crypto.tls.Client` does not support); exactly-once settlement (rcv-settle-mode second; rarely implemented, not needed by Artemis or Event Hubs defaults); delivery resumption via the unsettled map (complex, recovery re-sends instead); dynamic source/target nodes (temporary queues; deferred until a consumer needs them); web sockets. These non-goals are stated in the `amqp` README so the library does not present an Event-Hubs-shaped subset as complete.

**Concurrency model.** Every client takes an `std.Io` at construction and stores it. The connection starts one demultiplexer task with `io.concurrent`; it owns the socket read side and routes frames to per-session `Io.Queue` instances. A second task sends heartbeat (empty) frames. Writers share the socket behind an `Io.Mutex`. Public client objects are not thread-safe; one client per task is the documented rule. Rejected alternative: a caller-pumped, task-free design. It cannot keep heartbeats alive while the caller is idle, and it breaks when a producer and a receiver share one connection.

**TLS.** Primary choice: `std.crypto.tls.Client` with `Certificate.Bundle` system roots and explicit hostname verification. Reasons: zero dependencies, it matches the new `std.Io` reader/writer model, and it supports the TLS 1.2/1.3 suites Azure offers. Fallback, if interop fails against the real endpoint: vendor mbedTLS as a C dependency built by `build.zig`, behind the same internal `Transport` interface. The transport step includes an early live handshake test so this risk resolves in week one of that step.

**Authentication order.** SAS from a connection string is the first milestone. It has no HTTP dependency, it matches the credential already stored in 1Password, and it unlocks live tests earliest. Entra ID lands pre-GA as a `Credential` implementation (`client_credentials` grant over `std.http.Client`, token type `jwt` at CBS, scope `https://eventhubs.azure.net/.default`).

**Error model.** Zig errors carry no payload. The `amqp` module surfaces remote failures as data: an `amqp.Error` value holding the condition symbol and the description. The `eventhubs` module classifies conditions into an `EventHubsError` error set that mirrors the .NET `FailureReason` values, plus an optional `*Diagnostic` out-parameter on public operations. Judgement call (accepted on review); an alternative was a `last_error` field on each client.

**Testing design.** Four tiers, one code path:

- Unit (`zig build test`): codec golden vectors and fuzzing, framing, SAS vectors, retry math, error mapping, and full handshake tests against a scripted in-memory mock transport. Covers both modules.
- Interop (`zig build test-interop`): the `amqp` module alone against Apache ActiveMQ Artemis in Docker on plain TCP port 5672 with SASL PLAIN. This tier is the evidence that the `amqp` module is a real AMQP 1.0 client and not an Event Hubs transport with a general name. It uses no `eventhubs` code.
- Live (`zig build test-live`): reads `EVENTHUB_CONNECTION_STRING` (and optional `EVENTHUB_NAME`) through `std.testing.environ`; every live test returns `error.SkipZigTest` when the variable is absent. The variable is filled locally from 1Password with `op read "op://Personal/Azure Test Event Hub Namespace Connection String/Connection String"`; the secret never appears in code, commits, logs, or test output, and the harness must never print it. No ARM resource creation: tests target one pre-created hub on the Dedicated-cluster namespace. Tests must not depend on Dedicated-only limits or quotas, so the same suite runs on the emulator. Isolation from concurrent runs: each test captures the partition's `last_enqueued_sequence_number` first, reads from that position only, and stamps every published event with a UUIDv7 run id in application properties, filtering received events by that id. Cleanup is the hub's retention policy; nothing to delete.
- Emulator: the same `test-live` binary pointed at the emulator's `UseDevelopmentEmulator=true` connection string, run in CI inside Docker with Azurite. This gives an Event Hubs integration tier without cloud secrets on every push.

**Judgement calls.** Accepted on review and now settled: no processor and no checkpoint store at GA; emulator support in scope; no idempotent publishing, buffered producer, web sockets, or proxy; macOS and Linux only; the `*Diagnostic` error model; std TLS first with mbedTLS as the named fallback; SAS before Entra ID. Decided by Johnathan: both layers ship as public libraries; one repository with two modules. New judgement calls Johnathan may want to overturn:

1. **Lockstep versioning.** One tag and one version for both modules. The alternative (independent versions) has no mechanism inside one Zig package.
2. **Apache ActiveMQ Artemis as the interop broker.** RabbitMQ 4.x also speaks native AMQP 1.0; Artemis wins because its docs state full AMQP 1.0 support, a default 5672 acceptor, and SASL PLAIN plus ANONYMOUS, which matches the transport exactly.
3. **SASL PLAIN is in scope for the `amqp` module.** Artemis authentication needs it; it is one frame.
4. **Receiver dispositions are in scope.** A general client must handle unsettled deliveries; Event Hubs alone would not have needed them.
5. **The `amqp` non-goal list at 1.0.0** (transactions, broker role, SASL EXTERNAL, exactly-once settlement, delivery resumption, dynamic nodes, web sockets).

## Risks

- **`std.crypto.tls.Client` interop is unproven against Event Hubs.** The handshake, session tickets, or record handling could fail against Azure's TLS stack. Mitigation: the transport step tests the handshake against the real endpoint immediately; the mbedTLS fallback is named and isolated behind the `Transport` interface. This is the single largest uncertainty.
- **The public `amqp` surface raises the cost of every AMQP step.** Doc comments, stable names, and generic APIs take longer than a private subset, and the interop tier can surface broker behaviors (mixed settle modes, unexpected optional fields, large-message chunking differences) that a single-service transport never sees. Mitigation: the interop step sits inside milestone 1, directly after the links land, so general-client gaps surface before the Event Hubs layer builds on them.
- **Zig 0.16 `std.Io` is new.** `io.concurrent` semantics, cancellation, and `Io.Queue` behavior may hold surprises, and 0.17 may break APIs. Mitigation: pin `minimum_zig_version = "0.16.0"` and keep all Io usage behind thin internal wrappers.
- **AMQP scope creep.** A public library invites feature requests. The performative list and the non-goal list in this document are the contract; anything outside them needs a new issue.
- **Service behavior is only observable live.** Filter string quoting, geo-DR offset strings, and disposition timing can differ from the spec reading. Mitigation: the live smoke test lands mid-project (step 17), not at the end.
- **Sizing estimate.** 9,000-13,000 lines for the `amqp` module is an estimate anchored on go-amqp; if it runs 2x, milestone 1 absorbs the overrun, not the API surface.
- **The `Environ`/test wiring detail.** `std.testing.environ` is confirmed present; if the test runner does not populate it on some platform, the fallback is a build option that forwards the variable name (not the secret) and a `std.posix` read. Flagged, not blocking.
- **Artemis image naming is in transition.** Docker Hub hosts `apache/activemq-artemis`; the Artemis docs also reference `apache/artemis` after the top-level-project move. The interop step pins the exact image and tag from the then-current docs.

## Issue tree

Root: **GA the Zig Event Hubs SDK and the Zig AMQP 1.0 client** (tracking issue; links every child; states the GA definition from Understanding).

Milestone 0, bootstrap:
- Scaffold the repository, the two modules, and CI (step 1). No dependencies.

Milestone 1, AMQP 1.0 core (`amqp` module):
- Implement the AMQP primitive type codec (step 2). Depends: 1.
- Implement the AMQP performatives and the message codec (step 3). Depends: 2.
- Implement the AMQP frame layer (step 4). Depends: 3.
- Implement the transport with TCP, TLS, and SASL (step 5). Depends: 4.
- Implement the AMQP connection (step 6). Depends: 5.
- Implement the AMQP session (step 7). Depends: 6.
- Implement the sender link (step 8). Depends: 7.
- Implement the receiver link and dispositions (step 9). Depends: 8.
- Implement the request/response link pair (step 10). Depends: 8, 9.
- Add the AMQP interoperability tier against Artemis (step 11). Depends: 9.

Milestone 2, authentication (`eventhubs` module):
- Implement the connection string parser and SAS signer (step 12). Depends: 1.
- Implement CBS authorization and token refresh (step 13). Depends: 10, 12.

Milestone 3, clients and live tests (`eventhubs` module):
- Implement the error model and retry policy (step 14). Depends: 1.
- Implement EventData and the batch envelope (step 15). Depends: 3.
- Implement the management client (step 16). Depends: 13, 14.
- Build the client facade, the live test harness, and the smoke test (step 17). Depends: 16.
- Implement the producer client (step 18). Depends: 8, 14, 15, 17.
- Implement the partition receiver (step 19). Depends: 9, 14, 15, 17.
- Implement connection and link recovery (step 20). Depends: 18, 19.
- Add the emulator CI tier (step 21). Depends: 18, 19.
- Implement the Entra ID credential (step 22). Depends: 13, 17.

Milestone 4, GA:
- Harden the live test suite (step 23). Depends: 20.
- Review and document the AMQP public API, with samples (step 24). Depends: 11.
- Write the Event Hubs samples and documentation (step 25). Depends: 18, 19.
- Cut the GA release (step 26). Depends: 21, 22, 23, 24, 25.

Dependency order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → {10, 11} → 13 → 16 → 17 → {18, 19} → 20 → 23 → 26. Steps 12, 14, 15 run concurrently with the core chain once their dependencies land; 21, 22, 24, 25 run concurrently after their parents.

Note on `Parallel-safe`: under the strict definition (`yes` only when the step shares no file with any other step and depends on nothing unfinished), no step qualifies. Every step except step 1 has dependencies, and step 1 shares `build.zig`, `ci.yml`, and `README.md` with later steps. Every step below is therefore marked `no`. The concurrency guidance above says which issues can be picked at the same time once their listed dependencies are finished.

## Steps

### Step 1: Scaffold the repository, the two modules, and CI
- **Files**: `build.zig`, `build.zig.zon`, `src/amqp/root.zig`, `src/eventhubs/root.zig`, `.github/workflows/ci.yml`, `scripts/check-amqp-purity.sh`, `README.md`, `LICENSE`, `.gitignore`.
- **Change**: Create the two-module skeleton for Zig 0.16.0 in the existing `j7nw4r/eventhubs-zig` repository. In `build.zig.zon`, set `.name = .eventhubs`, a generated `.fingerprint`, and `.minimum_zig_version = "0.16.0"`. In `build.zig`, declare both public modules exactly as shown in Approach: `b.addModule("amqp", ...)` with root `src/amqp/root.zig`, and `b.addModule("eventhubs", ...)` with root `src/eventhubs/root.zig` and `.imports = &.{.{ .name = "amqp", .module = amqp_mod }}` (mechanism per `std/Build.zig:909` and `std/Build/Module.zig:216-226`). Add a `test` step that runs `b.addTest` on each module, a `fmt` step that runs `zig fmt --check .`, and a purity step that runs `scripts/check-amqp-purity.sh`. The purity script greps `src/amqp/` case-insensitive for `com.microsoft`, `x-opt-`, `eventhub`, `servicebus`, and `azure`, and exits nonzero on a match. Add one placeholder test in each root file; `src/eventhubs/root.zig` contains `@import("amqp")` so the module wiring is exercised from day one. The CI workflow installs Zig 0.16.0 on `ubuntu-latest` and `macos-latest` and runs the test, fmt, and purity steps on every push and pull request. The README states the two deliverables, the supported Zig version, and a status table of milestones.
- **Depends on**: none.
- **Parallel-safe**: no.
- **Done when**: `zig build test` passes locally with both module test binaries; the purity check fails when a `com.microsoft` string is planted in `src/amqp/` and passes without it; CI is green on both platforms.

### Step 2: Implement the AMQP primitive type codec
- **Files**: `src/amqp/types.zig`, `src/amqp/codec.zig`.
- **Change**: Implement encode and decode for every AMQP 1.0 primitive encoding (spec section 1.6): null, boolean (including true/false compact forms), ubyte, ushort, uint (uint0/smalluint/uint), ulong (ulong0/smallulong/ulong), byte, short, int (smallint), long (smalllong), float, double, char, timestamp (i64 milliseconds since the Unix epoch, distinct from long), uuid, binary (vbin8/vbin32), string (str8/str32, UTF-8), symbol (sym8/sym32), list (list0/list8/list32), map (map8/map32), array (array8/array32). Define `pub const Value = union(enum) { ... }` covering all of these plus `described: struct { descriptor: *Value, value: *Value }`. The encoder writes to a `*std.Io.Writer`. The decoder reads from a byte slice with strict bounds checks and returns `error.Malformed` on truncation, bad constructors, or invalid UTF-8. Allocation uses a caller-supplied allocator. This is public library surface: every public declaration carries a doc comment, and the ownership rule (decode allocates, caller frees via `Value.deinit`) is documented on the type.
- **Depends on**: 1.
- **Parallel-safe**: no.
- **Done when**: round-trip tests pass for every encoding; golden byte vectors from the AMQP 1.0 spec decode correctly; a `std.testing.fuzz` test on the decoder runs clean under `zig build test --fuzz` for a bounded time; `zig build test` passes.

### Step 3: Implement the AMQP performatives and the message codec
- **Files**: `src/amqp/performatives.zig`, `src/amqp/message.zig`.
- **Change**: Define typed structs with encode/decode for: `open` (0x10), `begin` (0x11), `attach` (0x12), `flow` (0x13), `transfer` (0x14), `disposition` (0x15), `detach` (0x16), `end` (0x17), `close` (0x18), `error` (0x1d), `source` (0x28), `target` (0x29), delivery states `received` (0x23), `accepted` (0x24), `rejected` (0x25), `released` (0x26), `modified` (0x27), and SASL frames `sasl-mechanisms` (0x40), `sasl-init` (0x41), `sasl-outcome` (0x44). Each encodes as a described list with the ulong descriptor. Omit optional trailing fields when null. Decode must accept lists longer than the known field count (ignore extras) and shorter (missing fields become null). In `message.zig`, implement a public `Message` type with encode/decode over the standard sections: header (0x70), delivery-annotations (0x71), message-annotations (0x72), properties (0x73), application-properties (0x74), data (0x75), amqp-sequence (0x76), amqp-value (0x77), footer (0x78). Annotations and properties are generic maps; no vendor key names appear in this module.
- **Depends on**: 2.
- **Parallel-safe**: no.
- **Done when**: golden vector round-trip tests pass for each performative and for a message with every section present, including one case with omitted optional fields and one with extra fields.

### Step 4: Implement the AMQP frame layer
- **Files**: `src/amqp/framing.zig`.
- **Change**: Implement the 8-byte frame header: size (u32 big-endian), doff (u8, 2 for no extended header), type (u8: 0x00 AMQP, 0x01 SASL), channel (u16 big-endian). Provide `writeFrame(writer, channel, performative, payload)` and `readFrame(reader, buf) !Frame` where `Frame` exposes the channel, the decoded performative, and the raw payload slice. Support the empty frame (size 8, no body) for heartbeats in both directions. Enforce the negotiated max frame size on read and write; return `error.FrameTooLarge`. Provide the protocol header constants `"AMQP\x03\x01\x00\x00"` (SASL) and `"AMQP\x00\x01\x00\x00"` (AMQP) and a helper that exchanges and validates them.
- **Depends on**: 3.
- **Parallel-safe**: no.
- **Done when**: golden byte tests pass for a full frame, an empty heartbeat frame, and both protocol headers; the fuzz test covers `readFrame`.

### Step 5: Implement the transport: TCP, TLS, SASL ANONYMOUS and PLAIN
- **Files**: `src/amqp/transport.zig`.
- **Change**: Implement `Transport.connect(gpa, io, host, port, tls_mode)`. Resolve the host with `std.Io.net.HostName` and connect with `IpAddress.connect` (`std/Io/net.zig:339`). For `tls_mode == .required`, wrap the stream in `std.crypto.tls.Client` (`std/crypto/tls/Client.zig:195`) with: `host = .{ .explicit = host }`, `ca = .{ .bundle = ... }` from `Certificate.Bundle.rescan`, fresh entropy, and `realtime_now` from the Io clock. For `.disabled`, use the plain stream. Then run SASL with a caller-selected mechanism: `.anonymous` sends `sasl-init` with mechanism `ANONYMOUS`; `.plain` sends mechanism `PLAIN` with the initial response `authzid NUL authcid NUL password` (authzid empty). In both cases: send the SASL protocol header, read and compare the server header, read `sasl-mechanisms`, make sure the chosen mechanism is offered, send `sasl-init`, read `sasl-outcome`, and make sure the code is 0. SASL EXTERNAL is a documented non-goal (no client certificates in std TLS). Then exchange the AMQP protocol header. Expose `reader()` and `writer()` as `*std.Io.Reader` / `*std.Io.Writer` regardless of TLS. Provide a `MockTransport` test double driven by scripted byte exchanges; it lives in this file behind `builtin.is_test`. Credentials must never appear in logs or error text.
- **Depends on**: 4.
- **Parallel-safe**: no.
- **Done when**: unit tests pass for the ANONYMOUS and PLAIN exchanges and header validation against `MockTransport`; a live test (env-gated, added here, run manually until step 17 wires the harness) completes a TLS handshake and SASL exchange against the real Event Hubs endpoint. This live check is the go/no-go gate for the std TLS choice; if it fails for TLS reasons, open the mbedTLS fallback issue before proceeding.

### Step 6: Implement the AMQP connection
- **Files**: `src/amqp/connection.zig`.
- **Change**: Implement `Connection.open(gpa, io, transport, options)`. Send `open` with: container-id (short unique id), hostname (caller-supplied), max-frame-size 65536, channel-max, idle-time-out 60000 ms, and a caller-supplied properties map (the `eventhubs` layer passes `product`, `version`, `platform`, and `user-agent`). Await the remote `open`; record the remote max-frame-size and idle-time-out. Start one demultiplexer task with `io.concurrent`: it reads frames and pushes them to per-channel `std.Io.Queue(Frame)` instances registered by sessions; unknown channels raise a connection error. Start one heartbeat task: it sends an empty frame every `remote_idle_timeout / 2` (skip when the remote sends no idle-time-out). Serialize all frame writes behind an `std.Io.Mutex`. Implement the close handshake and terminal-error propagation: on `close` with an error, on socket failure, or on decode failure, close every queue so blocked callers wake with the error, and store the remote error condition and description for callers to read. `deinit` cancels the tasks through an `Io.Group` and frees resources.
- **Depends on**: 5.
- **Parallel-safe**: no.
- **Done when**: mock tests pass for open/close negotiation, heartbeat emission under a scripted remote idle-time-out, frame routing to two channels, and error propagation to a blocked reader.

### Step 7: Implement the AMQP session
- **Files**: `src/amqp/session.zig`.
- **Change**: Implement `Session.begin(connection)`: allocate a channel, send `begin` (next-outgoing-id 0, incoming-window 5000, outgoing-window 5000), await the remote `begin`, and register the channel queue. Track next-incoming-id/next-outgoing-id and both windows; send `flow` to replenish the incoming window when it drops below half. Route `attach`, `flow`, `transfer`, `disposition`, and `detach` to links by handle; allocate output handles; hold a handle-to-link table. Implement `end` with error propagation to child links.
- **Depends on**: 6.
- **Parallel-safe**: no.
- **Done when**: mock tests pass for begin/end, window replenishment, and routing to two links.

### Step 8: Implement the sender link
- **Files**: `src/amqp/link.zig` (shared attach/detach plumbing), `src/amqp/sender.zig`.
- **Change**: In `link.zig`, implement the shared attach state machine: send `attach`, await the remote `attach`, surface a remote `detach`-with-error as an `amqp.Error` value (condition symbol plus description), and support local detach/close. In `sender.zig`, implement `Sender.attach(session, opts)` with: role sender, snd-settle-mode unsettled (0), rcv-settle-mode first (0), `source.address` = the link name, `target.address` = the caller-supplied node address, a caller-supplied properties map, and caller-supplied desired capabilities. No vendor symbol is named in this file. Read `max-message-size` from the remote attach and expose it. Track link credit from incoming `flow` frames; `send` blocks on an `Io.Condition` until credit is available. `send(payload, message_format, timeout)`: assign delivery-id and delivery-tag, split the payload into multiple `transfer` frames when it exceeds the max frame size (all but the last with `more = true`), send unsettled, and await the `disposition`; return the outcome (`accepted`, or `rejected`/`released`/`modified` with their payloads). `message_format` is a caller-supplied u32 with default 0.
- **Depends on**: 7.
- **Parallel-safe**: no.
- **Done when**: mock tests pass for the attach handshake, credit blocking and release, a multi-frame transfer split (compare the exact frame sequence), and the accepted and rejected outcomes.

### Step 9: Implement the receiver link and dispositions
- **Files**: `src/amqp/receiver.zig`, touches `src/amqp/link.zig`.
- **Change**: Implement `Receiver.attach(session, opts)` with: role receiver, a caller-selected requested snd-settle-mode (settled or unsettled), rcv-settle-mode first, `source.address` = the caller-supplied node address, `source.filter` = a caller-supplied map of symbol to described value (the module provides a generic helper to build a described filter entry from a descriptor symbol and a string value; no filter name is hard-coded), `target.address` = the link name, caller-supplied properties and desired capabilities. Implement credit: `flow` with `link-credit = prefetch` at attach; deliver reassembled messages (joining `more` transfers) into an internal `Io.Queue`; top the credit up to the prefetch value whenever consumed credit reaches half the window. `receive(timeout)` pops one delivery or returns null on timeout; a `Delivery` exposes the raw message bytes, the parsed `Message`, the delivery-id, and whether it arrived settled. Dispositions: settled deliveries need no action; for unsettled deliveries provide `accept(delivery)`, `release(delivery)`, `reject(delivery, error)`, and `modify(delivery, opts)`, each sending a `disposition` with `settled = true`; an `auto_accept` option (default on) accepts on receipt. Both paths are required: Event Hubs sends settled, Artemis sends unsettled by default.
- **Depends on**: 8.
- **Parallel-safe**: no.
- **Done when**: mock tests pass for: golden bytes of the attach frame including a filter map, credit top-up timing, multi-frame reassembly, timeout behavior, auto-accept dispositions, and one explicit reject.

### Step 10: Implement the request/response link pair
- **Files**: `src/amqp/rpc.zig`.
- **Change**: Implement `RpcLink.open(session, node_address)`: one sender with `target.address = node_address` and one receiver with `source.address = node_address` and `target.address` = a unique client reply address; the receiver attaches with auto-accept and credit. `request(message, timeout)`: set `properties.message-id` from a counter and `properties.reply-to` = the reply address, send, then await the response whose `correlation-id` equals the request's message-id; a table of pending requests keyed by message-id supports concurrent callers. Timeouts remove the pending entry and return `error.RequestTimeout`. The node address is always caller-supplied; `$cbs` and `$management` appear only in `src/eventhubs/`.
- **Depends on**: 8, 9.
- **Parallel-safe**: no.
- **Done when**: mock tests pass for correlation of two interleaved requests and for a request timeout.

### Step 11: Add the AMQP interoperability tier against Artemis
- **Files**: `docker/interop/docker-compose.yml`, `test/interop/root.zig`, `test/interop/harness.zig`, `test/interop/connection_test.zig`, `test/interop/send_receive_test.zig`, `test/interop/large_message_test.zig`, `build.zig`, `.github/workflows/ci.yml`.
- **Change**: Add a compose file running Apache ActiveMQ Artemis with `ARTEMIS_USER=artemis` and `ARTEMIS_PASSWORD=artemis` and port 5672 published. Artemis supports AMQP 1.0 on a default 5672 acceptor with SASL PLAIN and ANONYMOUS (https://artemis.apache.org/components/artemis/documentation/latest/amqp.html); the Docker Hub image is `apache/activemq-artemis`, and the Artemis Docker docs (https://artemis.apache.org/components/artemis/documentation/latest/docker.html) also reference `apache/artemis` after the project move. Pin the exact image and tag named by the then-current Artemis docs and record the choice in the compose file. Add a `test-interop` build step whose root is `test/interop/root.zig` and which imports only the `amqp` module; the purity rule extends to `test/interop/`. The harness reads `AMQP_INTEROP_ADDR`, `AMQP_INTEROP_USER`, and `AMQP_INTEROP_PASSWORD` via `std.testing.environ` and skips with `error.SkipZigTest` when unset. Tests: connect with SASL PLAIN, open and cleanly close a connection; attach a sender to a queue address, send 50 messages with application properties, attach a receiver to the same address, receive all 50 with auto-accept, and assert bodies and properties; explicit reject on one delivery; send and receive one message larger than the negotiated max frame size (multi-frame both directions); an idle period longer than the negotiated idle timeout followed by a successful send (heartbeats keep the connection alive). Add a Linux CI job that starts the compose stack and runs `zig build test-interop` on every push.
- **Depends on**: 9.
- **Parallel-safe**: no.
- **Done when**: the interop CI job is green on push; a planted vendor symbol in `test/interop/` fails the purity check.

### Step 12: Implement the connection string parser and SAS signer
- **Files**: `src/eventhubs/connection_string.zig`, `src/eventhubs/auth/credential.zig`, `src/eventhubs/auth/sas.zig`.
- **Change**: Parse semicolon-delimited `key=value` pairs with keys `Endpoint` (scheme `sb://`), `EntityPath`, `SharedAccessKeyName`, `SharedAccessKey`, `SharedAccessSignature`, `UseDevelopmentEmulator` (see `EventHubsConnectionStringProperties.cs`). Enforce: key/key-name pair XOR precomputed signature; emulator implies TLS off and default port 5672. In `sas.zig`, implement the token builder per `SharedAccessSignature.cs:337-354`: URL-encode the audience (lowercase host, path, uppercase percent hex), compute `HmacSha256(key, "{encoded_audience}\n{unix_expiry}")` with `std.crypto.auth.hmac.sha2.HmacSha256`, base64-encode, and format `SharedAccessSignature sr={enc}&sig={enc}&se={expiry}&skn={name}`. Default validity 30 minutes. In `credential.zig`, define the interface: `Credential` as a vtable struct with `getToken(audience: []const u8, gpa) !Token` where `Token = { value: []u8, expires_at: i64, kind: enum { sas, jwt } }`. Implement `SasKeyCredential` (signs fresh tokens) and `SasTokenCredential` (returns the precomputed string).
- **Depends on**: 1.
- **Parallel-safe**: no.
- **Done when**: parser tests cover all key combinations, the emulator string, and rejection cases; a pinned test vector (audience, key, fixed expiry, expected signature, generated once out-of-band and committed as a comment-documented constant) matches the signer output byte for byte.

### Step 13: Implement CBS authorization and token refresh
- **Files**: `src/eventhubs/auth/cbs.zig`.
- **Change**: On first use per connection, open an `amqp.RpcLink` to `$cbs` (the node name lives here, not in `src/amqp/`). `putToken(audience, token)`: application properties `operation = "put-token"`, `type = "servicebus.windows.net:sastoken"` for SAS or `"jwt"` for Entra (`CbsTokenProvider.cs:23-26`), `name = audience`, `expiration` = timestamp; body = amqp-value string with the token. Treat response `status-code` 202 (any 2xx) as success; otherwise map `status-code`/`status-description` through the error table (step 14). Implement the refresh manager: a registry of authorized audiences, one refresh task per connection via `io.concurrent`, next refresh at `expires_at - 7 min - jitter(0..30 s)` with a 3-minute floor (constants from `AmqpConnectionScope.cs:73-114`). Expose `authorize(audience)` for link creation and `deauthorize` on link close. The token value must never appear in logs or error text.
- **Depends on**: 10, 12.
- **Parallel-safe**: no.
- **Done when**: mock tests pass for the exact put-token request shape (golden), the 202 and error paths, and a pure-function test of the refresh interval math at the boundary values.

### Step 14: Implement the error model and retry policy
- **Files**: `src/eventhubs/errors.zig`, `src/eventhubs/retry.zig`.
- **Change**: Define `pub const EventHubsError = error{ GeneralError, ClientClosed, ConsumerDisconnected, ProducerDisconnected, ResourceNotFound, MessageSizeExceeded, QuotaExceeded, ServiceBusy, ServiceTimeout, ServiceCommunicationProblem, InvalidClientState, Unauthorized, ArgumentError }` (mirrors `EventHubsException.cs:221-255` plus the non-EventHubsException .NET mappings). Define `Diagnostic = { condition: ?[]const u8, description: buffer }` filled from `amqp.Error` values; public operations accept `diag: ?*Diagnostic`. Implement the condition table from `AmqpError.cs:30-92,174-292`: `com.microsoft:timeout` → ServiceTimeout; `com.microsoft:server-busy` → ServiceBusy; `com.microsoft:entity-disabled` and `amqp:not-found` (entity text) → ResourceNotFound; other `amqp:not-found` → ServiceCommunicationProblem; `amqp:link:stolen` → ConsumerDisconnected; `com.microsoft:producer-epoch-stolen` → ProducerDisconnected; `amqp:resource-limit-exceeded` → QuotaExceeded; `amqp:unauthorized-access` → Unauthorized; `com.microsoft:argument-error` and `:argument-out-of-range` → ArgumentError; `amqp:internal-error` → ServiceCommunicationProblem. `isTransient` is true for ServiceBusy, ServiceTimeout, ServiceCommunicationProblem (matches `EventHubsException.cs:185-192`) and for socket-level errors. Implement `RetryOptions { mode: enum { fixed, exponential } = .exponential, max_retries: u8 = 3, delay_ms: u32 = 800, max_delay_ms: u32 = 60_000, try_timeout_ms: u32 = 60_000 }` and `calculateDelay(options, err, attempt, random)` per `BasicRetryPolicy.cs:209`: `2^attempt * base + random * 0.08 * base`, capped at max delay; return null when the error is terminal or attempts are exhausted. Pure code, no I/O.
- **Depends on**: 1.
- **Parallel-safe**: no.
- **Done when**: table-driven tests cover every condition mapping and the delay formula with an injected deterministic random.

### Step 15: Implement EventData and the batch envelope
- **Files**: `src/eventhubs/event_data.zig`.
- **Change**: Build on the generic `amqp.Message` type from step 3. Define `EventData { body: []const u8, properties: map, message_id/content_type: optionals }` for publishing, and `ReceivedEventData` adding system fields parsed from annotations: `sequence_number` (i64, `x-opt-sequence-number`), `offset` ([]const u8, `x-opt-offset`), `enqueued_time` (i64 ms, `x-opt-enqueued-time`), `partition_key` (?[]const u8), plus `LastEnqueuedProperties` parsed from delivery annotations (`last_enqueued_sequence_number`, `last_enqueued_offset`, `last_enqueued_time_utc`, `runtime_info_retrieval_time_utc`; names from `AmqpProperty.cs:80-123`). All vendor annotation names live in this file. Implement the batch envelope builder per `AmqpMessageConverter.cs:305-361`: for N > 1 events, encode each event as a complete message, wrap each in a `data` section, and mark the batch for send with message-format `0x80013700` (passed to `Sender.send` as the caller-supplied format); for N = 1, use the event message directly with format 0; copy `x-opt-partition-key` onto the envelope annotations when set.
- **Depends on**: 3.
- **Parallel-safe**: no.
- **Done when**: round-trip tests pass; a golden fixture (bytes of one event and one 3-event envelope captured once from the .NET library and committed) decodes and re-encodes byte for byte.

### Step 16: Implement the management client
- **Files**: `src/eventhubs/management.zig`, `src/eventhubs/models.zig`.
- **Change**: In `models.zig`, define `EventHubProperties { name, created_at, partition_ids, geo_replication_enabled }` and `PartitionProperties { event_hub_name, id, is_empty, begin_sequence_number, last_enqueued_sequence_number, last_enqueued_offset, last_enqueued_time }`. In `management.zig`, open an `amqp.RpcLink` to `$management` on demand. Build requests per `AmqpMessageConverter.cs:121-135,188-205`: application properties `operation = "READ"`, `type = "com.microsoft:eventhub"` or `"com.microsoft:partition"`, `name = hub`, `partition = id` (partition case), `security_token` = a fresh token from the credential. Parse the amqp-value map response with the keys from `AmqpManagement.cs:51-118`; `georeplication_factor > 1` means geo-replication is on. Wrap both operations in the retry loop pattern of `AmqpClient.cs:227-309`: classify, consult `calculateDelay`, sleep via `io`, recreate the rpc link on transient failure.
- **Depends on**: 13, 14.
- **Parallel-safe**: no.
- **Done when**: mock tests pass with golden request bytes and a scripted map response for both operations, plus one retry-then-succeed script.

### Step 17: Build the client facade, the live test harness, and the smoke test
- **Files**: `src/eventhubs/client.zig`, `src/eventhubs/root.zig`, `test/live/harness.zig`, `test/live/root.zig`, `test/live/management_test.zig`, `build.zig`.
- **Change**: Implement `Client.fromConnectionString(gpa, io, conn_str, options)` and `Client.init(gpa, io, fqdn, event_hub, credential, options)`: parse, connect the transport (TLS, or emulator plain TCP), open the connection with the Event Hubs properties map, start CBS, and expose `getEventHubProperties` / `getPartitionProperties` plus factories used by later steps. Re-export the public types from `src/eventhubs/root.zig`. In `build.zig`, add a `test-live` step: a third `addTest` whose root is `test/live/root.zig` and which imports the `eventhubs` module, run by `zig build test-live`. In `harness.zig`, read `EVENTHUB_CONNECTION_STRING` and optional `EVENTHUB_NAME` with `std.testing.environ` (`std/testing.zig:37`, via `Environ.getPosix`); when the connection string is absent, return `error.SkipZigTest`. The harness supplies `std.testing.io`, a UUIDv7 run id, and a helper that resolves the hub name from `EntityPath` or `EVENTHUB_NAME`. The harness must never print the connection string or any token. The smoke test connects, fetches hub properties, asserts `partition_ids.len > 0`, and fetches properties for every partition.
- **Depends on**: 16.
- **Parallel-safe**: no.
- **Done when**: with the 1Password connection string exported as `EVENTHUB_CONNECTION_STRING`, `zig build test-live` passes against the real namespace; without it, the step reports skipped tests and exits zero; `zig build test` still passes.

### Step 18: Implement the producer client
- **Files**: `src/eventhubs/producer.zig`, `test/live/producer_test.zig`.
- **Change**: Implement `ProducerClient` created from `Client`. Maintain one `amqp.Sender` per destination path: `{hub}` for automatic routing, `{hub}/Partitions/{pid}` for a fixed partition (`AmqpConnectionScope.cs:47-50`); attach with caller-side properties `com.microsoft:timeout` (try timeout ms) and `com.microsoft:entity-type = 7` passed into the generic attach options, after `authorize(endpoint)` via CBS. `createBatch(options { partition_id: ?[]const u8, partition_key: ?[]const u8, max_size: ?u64 })` returns `EventDataBatch`; `tryAdd(event)` measures the serialized size including envelope overhead against the link max-message-size (pattern from `AmqpProducer.cs:279-328`) and returns false when the event does not fit; an oversized first event returns `error.MessageSizeExceeded`. `send(batch)` builds the envelope (step 15), sends through the sender with the batch message-format, and maps a `rejected` outcome through the error table; `sendEvents(events, options)` is a convenience wrapper. Wrap send in the standard retry loop. Partition id and partition key are mutually exclusive.
- **Depends on**: 8, 14, 15, 17.
- **Parallel-safe**: no.
- **Done when**: unit tests cover batch sizing edge cases against a mocked max-message-size; live tests pass: send 100 stamped events in batches to partition 0 and assert, via `getPartitionProperties`, that `last_enqueued_sequence_number` advanced by exactly 100 from the captured start; send one partition-key batch and one auto-routed batch without error.

### Step 19: Implement the partition receiver
- **Files**: `src/eventhubs/consumer.zig`, `test/live/consumer_test.zig`.
- **Change**: Implement `PartitionReceiver.init(client, consumer_group, partition_id, starting_position, options { prefetch: u32 = 300, owner_level: ?i64, track_last_enqueued: bool })`. Define `EventPosition = union(enum) { earliest, latest, offset: struct { value: []const u8, inclusive: bool }, sequence_number: struct { value: i64, inclusive: bool }, enqueued_time: i64 }`; build the filter expression per `AmqpFilter.cs:56-78`: offset → `amqp.annotation.x-opt-offset >[=] {value}`, sequence → `amqp.annotation.x-opt-sequence-number >[=] {value}`, time → `amqp.annotation.x-opt-enqueued-time > {ms}`; earliest is offset `-1` exclusive, latest is offset `@latest` exclusive (`EventPosition.cs:20-23`). Attach an `amqp.Receiver` (settled mode, auto-accept) to `{hub}/ConsumerGroups/{group}/Partitions/{pid}` with the filter entry built through the generic helper using descriptor `apache.org:selector-filter:string`, and caller-side properties `com.microsoft:entity-type = 8`, `com.microsoft:receiver-name = identifier`, and `com.microsoft:epoch = owner_level` when set; add desired capability `com.microsoft:enable-receiver-runtime-metric` when tracking. All of these symbols live in `src/eventhubs/`. `receiveBatch(max_count, max_wait_ms)` drains up to max_count events, blocking up to max_wait for the first; decode with step 15; update the tracked current position from each received offset (exclusive) for recovery; surface `last_enqueued` properties when tracking is on. An empty result after max_wait is a normal return, not an error (matches `AmqpConsumer.cs:313-319`).
- **Depends on**: 9, 14, 15, 17.
- **Parallel-safe**: no.
- **Done when**: unit tests cover filter-expression building for every position variant; live tests pass: publish stamped events with the step 18 producer, read from the captured start position, and assert bodies, order, and system properties; open an owner-level 2 receiver on the same partition and group, and make sure the owner-level 1 receiver fails with `ConsumerDisconnected`.

### Step 20: Implement connection and link recovery
- **Files**: `src/eventhubs/recoverable.zig`, touches `src/eventhubs/client.zig`, `src/eventhubs/producer.zig`, `src/eventhubs/consumer.zig`.
- **Change**: Implement fault-tolerant holders modeled on .NET `FaultTolerantAmqpObject` and the Rust `recoverable` module: a generic `Recoverable(T)` that owns a factory closure, returns the live object, and recreates it (with the retry policy governing delay and attempt count) when a caller reports a transient failure or the object is closed. Apply it at three levels: transport+connection, session, and link. On connection recreation, CBS re-authorizes every registered audience before links reattach. The producer retries an unsettled batch after link recreation; document at-least-once semantics (duplicates are possible on retry). The receiver reattaches with `EventPosition.offset(current, false)` from its tracked position (pattern from `AmqpConsumer.cs:300-303`), so no events are lost or re-read. Terminal errors (`ConsumerDisconnected`, `Unauthorized`, `ResourceNotFound`, `ArgumentError`) do not trigger recreation. This layer lives in `src/eventhubs/`; the `amqp` module stays a single-shot protocol library.
- **Depends on**: 18, 19.
- **Parallel-safe**: no.
- **Done when**: mock tests pass: a scripted connection drop mid-receive recovers and resumes at the next offset; a scripted link detach during send recovers and the batch lands once in the script; a terminal detach surfaces without retry. The live owner-level test from step 19 still passes with recovery active (the stolen receiver must not silently reattach).

### Step 21: Add the emulator CI tier
- **Files**: `.github/workflows/ci.yml`, `docker/emulator/docker-compose.yml`, `docker/emulator/config.json`, `docs/testing.md`.
- **Change**: Add a compose file with `mcr.microsoft.com/azure-messaging/eventhubs-emulator` plus Azurite, `ACCEPT_EULA=Y`, port 5672 published, and a mounted config declaring one hub `eh1` with 4 partitions and consumer group `$Default` (image and port per the Microsoft Learn emulator page). Add a CI job (Linux only) that starts the compose stack, waits for readiness, exports `EVENTHUB_CONNECTION_STRING` with the emulator default key and `UseDevelopmentEmulator=true` plus `EVENTHUB_NAME=eh1`, and runs `zig build test-live`. Document in `docs/testing.md` how to run the same stack locally, next to the interop stack from step 11. Skip, inside the harness, any live test that needs a capability the emulator lacks (mark such tests with a harness flag; geo-replication assertions are the known case).
- **Depends on**: 18, 19.
- **Parallel-safe**: no.
- **Done when**: the emulator CI job is green on push, running the producer, consumer, and management live tests without Azure secrets.

### Step 22: Implement the Entra ID credential
- **Files**: `src/eventhubs/auth/client_secret_credential.zig`, touches `src/eventhubs/auth/credential.zig`, `test/live/entra_test.zig`.
- **Change**: Implement `ClientSecretCredential { tenant_id, client_id, client_secret }` as a `Credential`: POST `client_credentials` form data to `https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token` with scope `https://eventhubs.azure.net/.default` (`EventHubTokenCredential.cs:19`) using `std.http.Client`; parse `access_token` and `expires_in` with `std.json`; cache the token and refresh when less than 5 minutes remain. The token kind is `jwt`, which makes CBS send `type = "jwt"` (step 13 already branches on kind). Add `EnvironmentCredential` that reads `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`. The live test builds a client with `EnvironmentCredential` plus `EVENTHUBS_FQDN`/`EVENTHUB_NAME` and fetches hub properties; it skips when the variables are absent.
- **Depends on**: 13, 17.
- **Parallel-safe**: no.
- **Done when**: unit tests parse a fixture token response and exercise cache expiry with a fake clock; the live Entra test passes when service-principal variables are present and skips cleanly otherwise.

### Step 23: Harden the live test suite
- **Files**: `test/live/round_trip_test.zig`, `test/live/limits_test.zig`, `test/live/stress_test.zig`, touches `test/live/harness.zig`.
- **Change**: Add: a round-trip test that sets application properties of each supported value type and asserts exact recovery; a near-limit test that sends an event close to the link max-message-size and asserts acceptance, and one over the limit asserting `MessageSizeExceeded` locally; a `tryAdd` overflow test that fills a batch to rejection and sends it; a quiet-partition test asserting `receiveBatch` returns empty after max_wait without error; a last-enqueued tracking test comparing tracked values against `getPartitionProperties`; a reconnect soak (10 minutes of send/receive with periodic idle gaps to exercise heartbeats) gated on `EVENTHUB_STRESS=1` so normal runs stay fast. All limit tests read the max-message-size from the attach frame, never from a hard-coded tier constant, because the live namespace is a Dedicated cluster while the emulator is not. All tests use the run-id stamping and captured-position isolation from the harness.
- **Depends on**: 20.
- **Parallel-safe**: no.
- **Done when**: `zig build test-live` passes three consecutive runs against the real namespace with zero flakes, and the stress mode passes once.

### Step 24: Review and document the AMQP public API, with samples
- **Files**: `src/amqp/root.zig`, `docs/amqp/README.md`, `docs/amqp/design.md`, `samples/amqp/send.zig`, `samples/amqp/receive.zig`, `build.zig`.
- **Change**: Do the public API review for the `amqp` module: `src/amqp/root.zig` re-exports the stable surface (`Value`, `Message`, `Transport`, `Connection`, `Session`, `Sender`, `Receiver`, `Delivery`, `RpcLink`, `Error`, options types); every public declaration carries a doc comment; each function documents allocator and ownership rules; internal helpers become private. `docs/amqp/README.md` states what the library is, the 1.0.0 scope, and the explicit non-goal list from Approach with reasons. `docs/amqp/design.md` documents the concurrency model and the layering rule. The samples are `main`-bearing programs that import only the `amqp` module and no `eventhubs` code: `send.zig` connects to a broker address from argv with SASL PLAIN and sends one message; `receive.zig` receives and accepts messages from a queue. Wire the samples into `build.zig` under a `samples` step; CI compiles them; the interop CI job additionally runs both samples against the Artemis container.
- **Depends on**: 11.
- **Parallel-safe**: no.
- **Done when**: `zig build samples` compiles both AMQP samples in CI; the interop job runs them against Artemis successfully; a review pass confirms every `pub` declaration in `src/amqp/` has a doc comment (spot-checked by a grep-based CI rule that rejects `pub fn`/`pub const` in `root.zig`-exported files without a preceding `///` line).

### Step 25: Write the Event Hubs samples and documentation
- **Files**: `README.md`, `docs/authentication.md`, `docs/testing.md`, `docs/design.md`, `samples/eventhubs/send.zig`, `samples/eventhubs/receive.zig`, `build.zig`.
- **Change**: Write the README quickstart for both modules: install via `zig fetch`; `@import("amqp")` for the AMQP client alone, `@import("eventhubs")` for the SDK; connect with a connection string, send a batch, receive from a partition; a support matrix (Zig 0.16.0, macOS/Linux, features in/out per this design); and a name mapping table from .NET types to Zig types. `docs/authentication.md` covers both credential paths. `docs/testing.md` covers the four tiers, the env variables, and the 1Password invocation shown in Validation. `docs/design.md` is a condensed version of this document's Approach section. The Event Hubs samples are `main`-bearing programs wired into the `samples` step next to the AMQP samples; CI compiles them.
- **Depends on**: 18, 19.
- **Parallel-safe**: no.
- **Done when**: `zig build samples` is green in CI for all four samples; a fresh clone can follow the README from zero to a successful send against the emulator.

### Step 26: Cut the GA release
- **Files**: `build.zig.zon`, `CHANGELOG.md`, `.github/workflows/release.yml`, `scripts/consumption-check.sh`, touches `src/amqp/root.zig`, `src/eventhubs/root.zig`.
- **Change**: Do a final API review pass over both modules: every public declaration matches the naming in this document, carries a doc comment, and the `eventhubs` module leaks no `src/amqp/` internals beyond the intentional re-export of `amqp` message types; run `zig fmt`; delete dead code. Set the version to `1.0.0` in `build.zig.zon` (lockstep: this one version covers both modules) and write the changelog with separate `amqp` and `eventhubs` sections. Add a release workflow that, on tag `v*`, runs all CI tiers and creates the GitHub release. Add `scripts/consumption-check.sh` to CI: it creates two temporary projects outside the repo tree; the first runs `zig fetch --save https://github.com/j7nw4r/eventhubs-zig/archive/refs/tags/v1.0.0.tar.gz`, imports only `dependency("eventhubs", ...).module("amqp")`, and builds a copy of the AMQP send sample; the second imports `module("eventhubs")` and builds the Event Hubs send sample, then runs it against the emulator. Document the SemVer policy (one version, both modules; a breaking change in either module bumps the major) in the README.
- **Depends on**: 21, 22, 23, 24, 25.
- **Parallel-safe**: no.
- **Done when**: the `v1.0.0` tag exists; the release workflow is green; both consumption checks pass; `zig build test`, `zig build test-interop`, and `zig build test-live` pass at the tag.

## Validation

Unit tier (no network, every push, both modules):

```
zig build test
zig fmt --check .
zig build purity
```

Passing looks like: all test steps exit zero on macOS and Linux; the format check reports no diffs; the purity check finds no vendor string in `src/amqp/`. Optional fuzzing: `zig build test --fuzz` runs the codec fuzz tests for a bounded time without a crash.

Interop tier (`amqp` module only, no secrets, CI on push, or local Docker):

```
docker compose -f docker/interop/docker-compose.yml up -d
AMQP_INTEROP_ADDR=localhost:5672 AMQP_INTEROP_USER=artemis AMQP_INTEROP_PASSWORD=artemis \
zig build test-interop
```

Passing looks like: all interop tests exit zero against the Artemis container; without the variables, every interop test reports skipped and the build exits zero.

Emulator tier (`eventhubs` module, no secrets, CI on push, or local Docker):

```
docker compose -f docker/emulator/docker-compose.yml up -d
EVENTHUB_CONNECTION_STRING='Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true' \
EVENTHUB_NAME=eh1 zig build test-live
```

Live tier (real Dedicated-cluster namespace; the secret comes from 1Password and passes through the environment only):

```
EVENTHUB_CONNECTION_STRING="$(op read 'op://Personal/Azure Test Event Hub Namespace Connection String/Connection String')" \
EVENTHUB_NAME='<hub>' zig build test-live
```

The connection string must never appear in code, commits, logs, or test output. Passing looks like: zero failures and zero skips in the live suite (skips indicate missing variables). Without the variables, `zig build test-live` exits zero and reports every live test as skipped. CI runs the unit, interop, and emulator tiers on every push; the live tier runs on manual dispatch and nightly, with `EVENTHUB_CONNECTION_STRING` as a repository secret.

Consumer build check (each module independently, automated in step 26):

```
zig fetch --save https://github.com/j7nw4r/eventhubs-zig/archive/refs/tags/v1.0.0.tar.gz
```

Then, in the consumer `build.zig`, either `dependency("eventhubs", .{...}).module("amqp")` for the AMQP client alone, or `.module("eventhubs")` for the SDK. Passing looks like: both scratch projects build, and the `eventhubs` sample runs against the emulator.

## Docs to update

Inside the repository (created by the steps themselves): `README.md`, `docs/amqp/README.md`, `docs/amqp/design.md`, `docs/authentication.md`, `docs/testing.md`, `docs/design.md`, `CHANGELOG.md`; each step that changes behavior updates these in the same change.

Outside the repository, in the same session that starts the implementation: add `~/.claude/docs/infrastructure/eventhubs-zig.md` (repo URL, the two module names, live-test env variables, the 1Password reference `op://Personal/Azure Test Event Hub Namespace Connection String/Connection String`, the emulator and interop compose invocations) and a one-line pointer in `~/.claude/docs/infrastructure/index.md`. The existing memory note `eventhubs-live-test-credentials.md` already points at the 1Password connection string and stays valid.
