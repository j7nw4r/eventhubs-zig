# eventhubs-zig

Two Zig libraries in one package.

- **`amqp`**: a general-purpose AMQP 1.0 client. It implements the client
  (initiator) role of the AMQP 1.0 specification. It names no broker and no
  cloud service, so it works against any AMQP 1.0 peer.
- **`eventhubs`**: an Azure Event Hubs client SDK. It builds on the `amqp`
  module and adds the Event Hubs protocol details.

## Status

This package is under construction. The scaffold builds and both modules are
importable, but the client API is not written yet. Do not use it in production
until version 1.0.0.

Track the work in [issue #1](https://github.com/j7nw4r/eventhubs-zig/issues/1).
Read [`docs/design.md`](docs/design.md) for the full design.

| Milestone | Scope | Status |
|---|---|---|
| M0 | Bootstrap: the two modules, the test steps, and CI | Complete |
| M1 | AMQP 1.0 core: codec, framing, transport, connection, session, links, interoperability tier | Not started |
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

## License

MIT. See [`LICENSE`](LICENSE).
