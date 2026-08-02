# eventhubs-zig

Two Zig libraries in one package.

- **`amqp`**: a general-purpose AMQP 1.0 client. It implements the client
  (initiator) role of the AMQP 1.0 specification. It names no broker and no
  cloud service, so it works against any AMQP 1.0 peer.
- **`eventhubs`**: an Azure Event Hubs client SDK. It builds on the `amqp`
  module and adds the Event Hubs protocol details.

## Status

This package is under construction. The scaffold builds, and a consumer can
import both modules. The `amqp` module encodes and decodes the AMQP 1.0
primitive types through `amqp.Value`, `amqp.encode`, and `amqp.decode`. It also
encodes and decodes the performatives, the delivery states, and the message
sections through `amqp.performatives` and `amqp.message`. It reads and writes
AMQP and SASL frames, and it exchanges the protocol headers, through
`amqp.framing`. It connects over TCP or TLS, and it runs the SASL dialog,
through `amqp.Transport`. It runs the open handshake, the begin handshake, and
the attach handshake through `amqp.Connection`, `amqp.Session`, and
`amqp.Sender`, and `amqp.Sender.send` puts one message on the wire and reports
the outcome. The receiver link does not exist yet. Do not use this package in
production until version 1.0.0.

Track the work in [issue #1](https://github.com/j7nw4r/eventhubs-zig/issues/1).
Read [`docs/design.md`](docs/design.md) for the full design.

| Milestone | Scope | Status |
|---|---|---|
| M0 | Bootstrap: the two modules, the test steps, and CI | Complete |
| M1 | AMQP 1.0 core: codec, framing, transport, connection, session, links, interoperability tier | In progress |
| M2 | Authentication: connection strings, SAS signing, CBS token refresh | Not started |
| M3 | Clients and live tests: producer, receiver, management, recovery, emulator tier | Not started |
| M4 | GA: API review, samples, documentation, the 1.0.0 release | Not started |

## Requirements

- Zig 0.16.0. The package pins it with `.minimum_zig_version` in
  `build.zig.zon`.
- macOS or Linux. Windows is out of scope at 1.0.0.

## Install

```
zig fetch --save git+https://github.com/j7nw4r/eventhubs-zig
```

Then import one module, or both, in your `build.zig`:

```zig
const eh = b.dependency("eventhubs", .{
    .target = target,
    .optimize = optimize,
});

// The Event Hubs SDK.
exe.root_module.addImport("eventhubs", eh.module("eventhubs"));

// Or the AMQP 1.0 client alone.
exe.root_module.addImport("amqp", eh.module("amqp"));
```

## Versions

The two modules share one version, because a Zig package carries exactly one
version. A breaking change in either module increases the major version.

## The layer purity rule

`src/amqp/` must contain no Event Hubs concept and no Azure concept. It names
no `com.microsoft:*` symbol, no `x-opt-*` annotation, and no management node
address. The `eventhubs` layer passes link properties, desired capabilities,
source filters, message annotations, and node addresses into the `amqp` API as
caller-supplied values.

`scripts/check-amqp-purity.sh` enforces the rule, and CI runs it on every push.
This keeps the `amqp` module honest as a general-purpose library.

## Development

```
zig build test      # run the unit tests of both modules
zig build fmt       # make sure the source is formatted
zig build purity    # make sure the amqp module stays vendor free
```

CI runs all three steps on `ubuntu-latest` and on `macos-latest`.

### The live tests

`zig build test` runs every unit test with no network. The live tests of
`src/amqp/transport.zig` open a real connection, and each one returns
`error.SkipZigTest` when its environment variables are absent. So a developer
with no broker still gets a green build.

The layer purity rule forbids the name of the vendor inside `src/amqp/`, so the
variables carry neutral names:

| Variable | Meaning |
|---|---|
| `AMQP_LIVE_HOST` | The host name of the broker, such as the fully qualified name of an Event Hubs namespace. Required. |
| `AMQP_LIVE_PORT` | The port. The default is 5671, the AMQP over TLS port. |
| `AMQP_LIVE_AUTHCID` | The identity of the SASL PLAIN test. |
| `AMQP_LIVE_PASSWORD` | The secret of the SASL PLAIN test. |

For an Event Hubs namespace, `AMQP_LIVE_HOST` takes the `Endpoint` host of the
connection string, and the two PLAIN variables take `SharedAccessKeyName` and
`SharedAccessKey`. Event Hubs itself authorizes over a token and accepts SASL
ANONYMOUS, so the ANONYMOUS test needs `AMQP_LIVE_HOST` alone:

```
AMQP_LIVE_HOST=<namespace>.servicebus.windows.net zig build test
```

Every live test has a 30 second timeout, so a peer that never answers fails the
test instead of stopping the suite. No test ever prints a credential.

## License

MIT. See [`LICENSE`](LICENSE).
