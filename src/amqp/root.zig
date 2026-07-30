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

/// The primitive type model: `Value`, `Constructor`, and the pieces that they
/// hold.
pub const types = @import("types.zig");

/// The primitive type codec: `encode`, `decode`, and `Decoder`.
pub const codec = @import("codec.zig");

/// The performatives and the described types that they carry.
pub const performatives = @import("performatives.zig");

/// The message format: `Message`, `Header`, `Properties`, and the sections.
pub const message = @import("message.zig");

/// One AMQP 1.0 value. The decoder allocates it, and `Value.deinit` frees it.
pub const Value = types.Value;

/// A primitive constructor code of section 1.6 of the AMQP 1.0 type
/// specification.
pub const Constructor = types.Constructor;

/// One entry of an AMQP map.
pub const MapEntry = types.MapEntry;

/// An AMQP array. Every element shares one constructor.
pub const Array = types.Array;

/// A described type: a descriptor and the value that it annotates.
pub const Described = types.Described;

/// Writes the AMQP 1.0 encoding of a value to a `std.Io.Writer`.
pub const encode = codec.encode;

/// Returns the number of bytes that `encode` writes for a value.
pub const encodedSize = codec.encodedSize;

/// Reads one value from a byte slice, and makes sure that the value uses every
/// byte.
pub const decode = codec.decode;

/// A cursor over a byte slice that reads one value at a time.
pub const Decoder = codec.Decoder;

/// The errors that `encode` and `encodedSize` return.
pub const EncodeError = codec.EncodeError;

/// The errors that `decode` and `Decoder.next` return.
pub const DecodeError = codec.DecodeError;

/// One AMQP symbol field of a composite type.
pub const Symbol = performatives.Symbol;

/// One AMQP binary field of a composite type.
pub const Binary = performatives.Binary;

/// A decoded composite and the memory that holds it.
pub const Decoded = performatives.Decoded;

/// One AMQP 1.0 message of format 0.
pub const Message = message.Message;

/// The version of the AMQP specification that this library speaks.
pub const protocol_version = "1.0";

test "the module exposes its protocol version" {
    try std.testing.expectEqualStrings("1.0", protocol_version);
}

test {
    std.testing.refAllDecls(@This());
    _ = types;
    _ = codec;
    _ = performatives;
    _ = message;
}
