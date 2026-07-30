//! A general-purpose AMQP 1.0 client library.
//!
//! This module implements the client (initiator) role of the AMQP 1.0
//! specification. It knows nothing about any broker or cloud service. The
//! caller supplies every vendor extension: link properties, desired
//! capabilities, source filters, message annotations, and node addresses are
//! all values that the caller passes in.
//!
//! Import it with `@import("amqp")`.

const std = @import("std");

/// The version of the AMQP specification that this library speaks.
pub const protocol_version = "1.0";

test "the module exposes its protocol version" {
    try std.testing.expectEqualStrings("1.0", protocol_version);
}
