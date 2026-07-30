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

/// The frame layer: `Frame`, `readFrame`, `writeFrame`, and the protocol
/// headers.
pub const framing = @import("framing.zig");

/// The transport: TCP, TLS, the SASL dialog, and the protocol header exchange.
pub const transport = @import("transport.zig");

/// The connection: the open negotiation, the frame demultiplexer, the
/// heartbeat, and the close handshake.
pub const connection = @import("connection.zig");

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

/// One frame that `readFrame` produced.
pub const Frame = framing.Frame;

/// The body of one frame: one performative, or one SASL security frame.
pub const Body = framing.Body;

/// The type code of a frame header: an AMQP frame or a SASL frame.
pub const FrameType = framing.FrameType;

/// Writes one frame to a `std.Io.Writer`.
pub const writeFrame = framing.writeFrame;

/// Writes one empty frame. The empty frame is the heartbeat.
pub const writeEmptyFrame = framing.writeEmptyFrame;

/// Reads one frame from a `std.Io.Reader`.
pub const readFrame = framing.readFrame;

/// The protocol header of the AMQP layer.
pub const amqp_protocol_header = framing.amqp_protocol_header;

/// The protocol header of the SASL security layer.
pub const sasl_protocol_header = framing.sasl_protocol_header;

/// Sends a protocol header and makes sure that the peer answers with the same
/// one.
pub const exchangeProtocolHeader = framing.exchangeProtocolHeader;

/// The lower bound for the negotiated maximum frame size, in octets.
pub const min_max_frame_size = framing.min_max_frame_size;

/// The errors that `writeFrame` returns.
pub const WriteFrameError = framing.WriteFrameError;

/// The errors that `readFrame` returns.
pub const ReadFrameError = framing.ReadFrameError;

/// One connected AMQP 1.0 byte stream.
pub const Transport = transport.Transport;

/// Whether the transport puts TLS below the AMQP protocol header.
pub const TlsMode = transport.TlsMode;

/// The SASL mechanism and the data that it needs.
pub const Sasl = transport.Sasl;

/// The arguments of `Transport.connect` that are neither the host nor the port.
pub const TransportOptions = transport.Options;

/// The detail of a failed handshake. It never holds a credential.
pub const Diagnostics = transport.Diagnostics;

/// Runs the SASL dialog on a reader and a writer.
pub const performSasl = transport.performSasl;

/// The errors of `Transport.connect`.
pub const ConnectError = transport.ConnectError;

/// The errors of the SASL dialog and the protocol header exchange.
pub const HandshakeError = transport.HandshakeError;

/// One AMQP 1.0 connection: the tasks, the locks, and the negotiated limits.
pub const Connection = connection.Connection;

/// The read side and the write side of one connected byte stream.
pub const Stream = connection.Stream;

/// The arguments of `Connection.open` that the open frame carries.
pub const ConnectionOptions = connection.Options;

/// The queue that receives the frames of one channel.
pub const FrameQueue = connection.FrameQueue;

/// The reason that a connection ended, with the text that came with it.
pub const Failure = connection.Failure;

/// The reasons that a connection ends.
pub const ConnectionError = connection.Error;

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
    _ = framing;
    _ = transport;
    _ = connection;
}
