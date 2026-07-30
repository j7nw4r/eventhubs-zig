//! An Azure Event Hubs client SDK for Zig.
//!
//! This module builds on the `amqp` module. It supplies the Event Hubs
//! protocol details, and the `amqp` module stays free of them.
//!
//! Import it with `@import("eventhubs")`.

const std = @import("std");

/// The AMQP 1.0 client library that this SDK is built on. It is re-exported
/// so that a caller can reach the message types through one dependency.
pub const amqp = @import("amqp");

test "the amqp module is wired in" {
    try std.testing.expectEqualStrings("1.0", amqp.protocol_version);
}
