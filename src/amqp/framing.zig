//! The AMQP 1.0 frame layer.
//!
//! This file turns a byte stream into frames, and frames into bytes. It sits
//! between the transport and the connection. It knows the 8-octet frame
//! header, the empty heartbeat frame, the maximum frame size rule, and the
//! protocol headers of the version handshake.
//!
//! Specifications:
//! OASIS AMQP Version 1.0 Part 2: Transport, sections 2.2, 2.3, 2.4.5, and
//! 2.8.19.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html
//! OASIS AMQP Version 1.0 Part 5: Security, section 5.3.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html
//!
//! # The frame header
//!
//! Section 2.3.1 gives the 8-octet header:
//!
//! ```
//!  +0       +1       +2       +3
//! +--------+--------+--------+--------+
//! |              SIZE                 |
//! +--------+--------+--------+--------+
//! |  DOFF  |  TYPE  |  TYPE-SPECIFIC  |
//! +--------+--------+--------+--------+
//! ```
//!
//! `SIZE` is a big-endian u32. It counts the whole frame, and the 8 header
//! octets are part of that count. A frame with a size below 8 is malformed.
//!
//! `DOFF` is the data offset. It counts 4-octet words, not octets. This is the
//! detail that an implementation gets wrong most often. `DOFF = 2` therefore
//! names the plain 8-octet header with no extended header, and the frame body
//! starts at octet `DOFF * 4`. A frame with a `DOFF` below 2 is malformed,
//! because the header alone takes two words. The octets between the header and
//! `DOFF * 4` hold the extended header, and this file ignores them, as sections
//! 2.3.2 and 5.3.1 allow.
//!
//! `TYPE` is 0x00 for an AMQP frame and 0x01 for a SASL frame.
//!
//! The last two octets depend on the type. Section 2.3.2 puts the channel
//! number there for an AMQP frame. Section 5.3.1 ignores them for a SASL frame,
//! and it tells an implementation to write zero, so `readFrame` reports channel
//! 0 for every SASL frame and `writeFrame` writes zero.
//!
//! # The empty frame
//!
//! A frame with no body is the heartbeat of section 2.4.5. A peer sends one to
//! make traffic when it has nothing else to send. `writeEmptyFrame` writes one,
//! and `readFrame` returns a `Frame` whose `body` is null for one. Section 5.3.1
//! makes an empty SASL frame an unrecoverable error, so `readFrame` rejects it.
//!
//! # Ownership
//!
//! `readFrame` returns a `Frame` that holds memory of two kinds, and the two
//! kinds have different lifetimes.
//!
//! `Frame.body` lives in an arena that the frame owns. `Frame.deinit` frees the
//! arena, and it takes no allocator, which is the rule that
//! `performatives.Decoded` already follows.
//!
//! `Frame.payload` borrows the caller buffer `buf` that `readFrame` filled. The
//! frame copies nothing. So the caller must keep `buf` alive and unchanged for
//! as long as it reads `payload`, and the next call to `readFrame` with the same
//! buffer overwrites it. Copy the payload when you need it for longer.
//!
//! Call `Frame.deinit` on every frame, including an empty frame.

const std = @import("std");
const codec = @import("codec.zig");
const performatives = @import("performatives.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Reader = std.Io.Reader;
const Value = types.Value;
const Writer = std.Io.Writer;

// -------------------------------------------------------------------------
// The constants
// -------------------------------------------------------------------------

/// The number of octets in the fixed frame header. Section 2.3.1.
pub const frame_header_size: u32 = 8;

/// The smallest legal data offset. The value counts 4-octet words, so 2 names
/// the 8-octet header and no extended header. Section 2.3.1.
pub const min_data_offset: u8 = 2;

/// The lower bound for the negotiated maximum frame size, in octets. The
/// constant is MIN-MAX-FRAME-SIZE of section 2.8.19. Two peers must accept a
/// frame of this size before they agree a larger one. Section 5.3.1 also makes
/// it the fixed maximum size of a SASL frame.
pub const min_max_frame_size: u32 = 512;

/// The number of octets in a protocol header. Section 2.2.
pub const protocol_header_size: usize = 8;

/// The protocol header of the AMQP layer. The protocol id is 0. Section 2.2.
pub const amqp_protocol_header: [protocol_header_size]u8 = .{ 'A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00 };

/// The protocol header of the SASL security layer. The protocol id is 3.
/// Section 5.3.
pub const sasl_protocol_header: [protocol_header_size]u8 = .{ 'A', 'M', 'Q', 'P', 0x03, 0x01, 0x00, 0x00 };

/// The type code in octet 5 of the frame header. Section 2.3.1.
pub const FrameType = enum(u8) {
    /// An AMQP frame. Its body is one performative of section 2.7.
    amqp = 0x00,
    /// A SASL frame. Its body is one security frame of section 5.3.3.
    sasl = 0x01,
};

// -------------------------------------------------------------------------
// The frame body
// -------------------------------------------------------------------------

/// The body of one frame.
///
/// The tag selects the frame type: the three SASL bodies travel in a SASL
/// frame, and the other nine travel in an AMQP frame.
///
/// This library speaks SASL ANONYMOUS and SASL PLAIN, and neither mechanism
/// uses a challenge, so `sasl-challenge` (0x42) and `sasl-response` (0x43) have
/// no variant here. `readFrame` reports `error.UnknownPerformative` for them.
pub const Body = union(enum) {
    /// Negotiate connection parameters. Section 2.7.1.
    open: performatives.Open,
    /// Begin a session. Section 2.7.2.
    begin: performatives.Begin,
    /// Attach a link. Section 2.7.3.
    attach: performatives.Attach,
    /// Update the flow state of a link or a session. Section 2.7.4.
    flow: performatives.Flow,
    /// Carry message data. Section 2.7.5.
    transfer: performatives.Transfer,
    /// Report a change of delivery state. Section 2.7.6.
    disposition: performatives.Disposition,
    /// Detach a link. Section 2.7.7.
    detach: performatives.Detach,
    /// End a session. Section 2.7.8.
    end: performatives.End,
    /// Close a connection. Section 2.7.9.
    close: performatives.Close,
    /// Advertise the SASL mechanisms. Section 5.3.3.1.
    sasl_mechanisms: performatives.SaslMechanisms,
    /// Start the SASL dialog. Section 5.3.3.2.
    sasl_init: performatives.SaslInit,
    /// Report the outcome of the SASL dialog. Section 5.3.3.5.
    sasl_outcome: performatives.SaslOutcome,

    /// Returns the frame type that carries this body.
    pub fn frameType(self: Body) FrameType {
        return switch (self) {
            .sasl_mechanisms, .sasl_init, .sasl_outcome => .sasl,
            else => .amqp,
        };
    }

    /// Writes the described list encoding of the body to `w`.
    pub fn encode(self: Body, w: *Writer) performatives.EncodeError!void {
        return switch (self) {
            inline else => |value| value.encode(w),
        };
    }

    /// Returns the number of octets that `encode` writes.
    pub fn encodedSize(self: Body) performatives.EncodeError!usize {
        return switch (self) {
            inline else => |value| value.encodedSize(),
        };
    }
};

/// One frame that `readFrame` produced.
///
/// Read the ownership note at the top of this file before you keep a frame.
/// `body` lives in an arena that this value owns, and `payload` borrows the
/// caller buffer. Free the arena with `deinit`.
pub const Frame = struct {
    /// The type code of the frame header.
    frame_type: FrameType,
    /// The channel number of an AMQP frame. It is 0 for a SASL frame, because
    /// section 5.3.1 ignores the two octets that hold it.
    channel: u16,
    /// The decoded body, or null for an empty heartbeat frame.
    body: ?Body,
    /// The octets after the body, in the caller buffer. Only a transfer frame
    /// carries them. The slice is empty for every other frame.
    payload: []const u8,
    /// The arena that holds every slice of `body`.
    arena_state: std.heap.ArenaAllocator.State,
    /// The allocator that the arena takes its memory from.
    child: Allocator,

    /// Frees the arena, and thus every slice of `body`. The function does not
    /// touch `payload`, because the caller owns that buffer.
    pub fn deinit(self: Frame) void {
        self.arena_state.promote(self.child).deinit();
    }

    /// Returns true for an empty heartbeat frame. Section 2.4.5.
    pub fn isEmpty(self: Frame) bool {
        return self.body == null;
    }
};

// -------------------------------------------------------------------------
// The errors
// -------------------------------------------------------------------------

/// The errors that `writeFrame` and `writeEmptyFrame` return.
pub const WriteFrameError = performatives.EncodeError || error{
    /// The frame is larger than the negotiated maximum frame size.
    FrameTooLarge,
};

/// The errors that `readFrame` returns.
///
/// The reader keeps no position after an error, because the function stops in
/// the middle of the frame. Close the connection when `readFrame` fails.
pub const ReadFrameError = Reader.Error || codec.DecodeError || error{
    /// The frame is larger than the negotiated maximum frame size.
    FrameTooLarge,
    /// The caller buffer is smaller than the frame. Give `readFrame` a buffer
    /// of at least the negotiated maximum frame size.
    BufferTooSmall,
    /// Octet 5 of the header holds a type code that this library does not
    /// know.
    UnknownFrameType,
    /// The body holds a descriptor that this library does not know.
    UnknownPerformative,
    /// The peer sent an empty SASL frame. Section 5.3.1 makes this an
    /// unrecoverable error.
    EmptySaslFrame,
};

/// The errors that `exchangeProtocolHeader` returns.
pub const ExchangeError = Reader.Error || Writer.Error || error{
    /// The peer answered with a different protocol header. Read the
    /// `ProtocolHeaderMismatch` that the call filled to learn which one.
    ProtocolHeaderMismatch,
};

// -------------------------------------------------------------------------
// The write path
// -------------------------------------------------------------------------

/// Writes one frame to `w`.
///
/// The function writes the header, then the body, then `payload`. Give an empty
/// slice for `payload` when the body carries no message data. Only a transfer
/// frame carries a payload.
///
/// `channel` names the session channel. The function ignores it for a SASL
/// frame and writes zero, as section 5.3.1 tells it to.
///
/// The function returns `error.FrameTooLarge` when the whole frame is larger
/// than `max_frame_size`. Pass the size that the two peers negotiated in their
/// open frames, or `min_max_frame_size` before they negotiate one. A SASL frame
/// takes the smaller of `max_frame_size` and `min_max_frame_size`, because
/// section 5.3.1 fixes the size of a SASL frame at MIN-MAX-FRAME-SIZE.
///
/// The function does not flush `w`.
///
/// The function sizes the body before it writes the header, and `encodedSize`
/// applies the rules that `encode` applies, so `error.InvalidValue` arrives
/// before any octet reaches `w`. After the header, only a write failure stops
/// the function, and a write failure ends the connection.
pub fn writeFrame(
    w: *Writer,
    channel: u16,
    body: Body,
    payload: []const u8,
    max_frame_size: u32,
) WriteFrameError!void {
    const body_size = try body.encodedSize();
    const total = @as(usize, frame_header_size) + body_size + payload.len;
    // Section 5.3.1: MIN-MAX-FRAME-SIZE is the maximum size of a SASL frame,
    // and the SASL dialog holds no way to agree a larger one.
    const limit = switch (body.frameType()) {
        .amqp => max_frame_size,
        .sasl => @min(max_frame_size, min_max_frame_size),
    };
    if (total > limit) return error.FrameTooLarge;

    // The test above holds `total` below `limit`, and `limit` is a u32, so the
    // cast cannot lose a bit.
    try writeHeader(w, @intCast(total), body.frameType(), channel);
    try body.encode(w);
    try w.writeAll(payload);
}

/// Writes one empty frame to `w`. The frame is the heartbeat of section 2.4.5.
///
/// The frame holds the 8-octet header alone, so it is never larger than
/// `min_max_frame_size` and it needs no size test.
///
/// Section 2.4.5 tells a peer to send the heartbeat on channel 0, and it
/// requires channel 0 before the peers exchange their open frames. Pass 0
/// unless you have a reason to do otherwise.
///
/// The function does not flush `w`.
pub fn writeEmptyFrame(w: *Writer, channel: u16) Writer.Error!void {
    return writeHeader(w, frame_header_size, .amqp, channel);
}

/// Writes the 8-octet frame header. Section 2.3.1.
fn writeHeader(w: *Writer, size: u32, frame_type: FrameType, channel: u16) Writer.Error!void {
    try w.writeInt(u32, size, .big);
    // The data offset counts 4-octet words. This library writes no extended
    // header, so the body starts at word 2, which is octet 8.
    try w.writeByte(min_data_offset);
    try w.writeByte(@intFromEnum(frame_type));
    // Section 2.3.2 puts the channel in octets 6 and 7 of an AMQP frame.
    // Section 5.3.1 ignores the two octets of a SASL frame and asks for zero.
    try w.writeInt(u16, switch (frame_type) {
        .amqp => channel,
        .sasl => 0,
    }, .big);
}

// -------------------------------------------------------------------------
// The read path
// -------------------------------------------------------------------------

/// Reads one frame from `r`.
///
/// The function reads the header, tests it against the rules of section 2.3.1,
/// reads the rest of the frame into `buf`, and decodes the body. `buf` must
/// hold `max_frame_size` octets, or a legal large frame returns
/// `error.BufferTooSmall`.
///
/// The function returns `error.FrameTooLarge` when the header names a size
/// above `max_frame_size`. It makes the test before it reads the body, so a
/// hostile peer cannot make it read a large frame. A SASL frame takes the
/// smaller of `max_frame_size` and `min_max_frame_size`, because section 5.3.1
/// fixes the size of a SASL frame at MIN-MAX-FRAME-SIZE.
///
/// The result borrows `buf` for its payload and owns an arena for its body.
/// Read the ownership note at the top of this file. Free the result with
/// `Frame.deinit`.
pub fn readFrame(
    gpa: Allocator,
    r: *Reader,
    buf: []u8,
    max_frame_size: u32,
) ReadFrameError!Frame {
    var header: [frame_header_size]u8 = undefined;
    try r.readSliceAll(&header);

    // Section 2.3.1: the size counts the header, so a size below 8 is
    // malformed.
    const size = std.mem.readInt(u32, header[0..4], .big);
    if (size < frame_header_size) return error.Malformed;
    if (size > max_frame_size) return error.FrameTooLarge;

    // Section 2.3.1: the data offset counts 4-octet words, and the mandatory
    // 8-octet header makes a value below 2 malformed.
    const data_offset = header[4];
    if (data_offset < min_data_offset) return error.Malformed;
    const body_start: u32 = @as(u32, data_offset) * 4;
    if (body_start > size) return error.Malformed;

    const frame_type: FrameType = switch (header[5]) {
        @intFromEnum(FrameType.amqp) => .amqp,
        @intFromEnum(FrameType.sasl) => .sasl,
        else => return error.UnknownFrameType,
    };
    const channel: u16 = switch (frame_type) {
        .amqp => std.mem.readInt(u16, header[6..8], .big),
        .sasl => 0,
    };

    // Section 5.3.1: MIN-MAX-FRAME-SIZE is the maximum size of a SASL frame,
    // and the SASL dialog holds no way to agree a larger one.
    if (frame_type == .sasl and size > min_max_frame_size) return error.FrameTooLarge;

    // Read the extended header and the body together. The size test above
    // holds `rest` at or below `max_frame_size`.
    const rest = size - frame_header_size;
    if (rest > buf.len) return error.BufferTooSmall;
    const tail = buf[0..rest];
    try r.readSliceAll(tail);

    // Sections 2.3.2 and 5.3.1 both ignore the extended header. The subtraction
    // cannot go below zero, because the data offset is at least 2.
    const body_bytes = tail[body_start - frame_header_size ..];

    var arena: std.heap.ArenaAllocator = .init(gpa);
    errdefer arena.deinit();

    if (body_bytes.len == 0) {
        // Section 5.3.1: the body of a SASL frame must hold exactly one type,
        // so an empty SASL frame is an unrecoverable error.
        if (frame_type == .sasl) return error.EmptySaslFrame;
        return .{
            .frame_type = frame_type,
            .channel = channel,
            .body = null,
            .payload = body_bytes,
            .arena_state = arena.state,
            .child = gpa,
        };
    }

    var decoder: codec.Decoder = .init(body_bytes);
    const value = try decoder.next(arena.allocator());
    const body = try bodyFromValue(value, frame_type, arena.allocator());
    const payload = body_bytes[decoder.pos..];

    // Section 5.3.1: a SASL frame carries no payload after its body.
    if (frame_type == .sasl and payload.len != 0) return error.Malformed;

    return .{
        .frame_type = frame_type,
        .channel = channel,
        .body = body,
        .payload = payload,
        .arena_state = arena.state,
        .child = gpa,
    };
}

/// Builds the typed body from the decoded value.
///
/// The result borrows every slice from `value`, so `value` must live as long as
/// the result. `arena` holds both, so one arena keeps the rule.
fn bodyFromValue(value: Value, frame_type: FrameType, arena: Allocator) ReadFrameError!Body {
    if (value != .described) return error.Malformed;

    inline for (@typeInfo(Body).@"union".fields) |field| {
        const T = field.type;
        // AMQP allows the numeric descriptor and the symbolic descriptor.
        const matches = switch (value.described.descriptor.*) {
            .ulong => |code| code == T.descriptor_code,
            .symbol => |name| std.mem.eql(u8, name, T.descriptor_name),
            else => return error.Malformed,
        };
        if (matches) {
            const composite = try performatives.compositeFromValue(T, value, arena);
            const body = @unionInit(Body, field.name, composite);
            // A SASL body in an AMQP frame, or the reverse, breaks section
            // 5.3.1.
            if (body.frameType() != frame_type) return error.Malformed;
            return body;
        }
    }
    return error.UnknownPerformative;
}

// -------------------------------------------------------------------------
// The protocol header
// -------------------------------------------------------------------------

/// The two protocol headers of one failed exchange.
pub const ProtocolHeaderMismatch = struct {
    /// The header that this peer sent.
    sent: [protocol_header_size]u8,
    /// The header that the other peer sent back.
    received: [protocol_header_size]u8,
};

/// The number of octets that `describeProtocolHeader` can need.
pub const header_text_size: usize = 32;

/// Writes a readable form of a protocol header into `buf`, and returns the
/// text. A header of the AMQP family reads as `AMQP 3.1.0.0`, where the first
/// number is the protocol id. Any other 8 octets read as hexadecimal.
///
/// Give the result to a person who debugs a handshake. The text says which
/// layer the peer asked for.
pub fn describeProtocolHeader(
    header: [protocol_header_size]u8,
    buf: *[header_text_size]u8,
) []const u8 {
    var w: Writer = .fixed(buf);
    // The longest text is `AMQP 255.255.255.255`, which takes 20 octets, and
    // the hexadecimal form takes 16, so the writer never runs out of room.
    if (std.mem.eql(u8, header[0..4], "AMQP")) {
        w.print("AMQP {d}.{d}.{d}.{d}", .{
            header[4],
            header[5],
            header[6],
            header[7],
        }) catch unreachable;
    } else {
        w.printHex(&header, .lower) catch unreachable;
    }
    return w.buffered();
}

/// Sends `header` to the peer and reads the answer of the peer.
///
/// Section 2.2 makes the client send its header first, and it makes the two
/// headers match before either peer sends a frame. The function therefore
/// writes, flushes, and then reads.
///
/// The function returns `error.ProtocolHeaderMismatch` when the two headers
/// differ. It first writes the two headers to `mismatch`, when the caller gives
/// one, so the caller can report which header the peer sent. Pass the result to
/// `describeProtocolHeader` for readable text. Section 2.2 makes the peers close
/// the connection after a mismatch.
pub fn exchangeProtocolHeader(
    r: *Reader,
    w: *Writer,
    header: [protocol_header_size]u8,
    mismatch: ?*ProtocolHeaderMismatch,
) ExchangeError!void {
    try w.writeAll(&header);
    try w.flush();

    var peer: [protocol_header_size]u8 = undefined;
    try r.readSliceAll(&peer);
    if (!std.mem.eql(u8, &header, &peer)) {
        if (mismatch) |out| out.* = .{ .sent = header, .received = peer };
        return error.ProtocolHeaderMismatch;
    }
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

const testing = std.testing;

/// Writes one frame to a fresh buffer and compares it with `want`.
fn expectFrameBytes(
    channel: u16,
    body: Body,
    payload: []const u8,
    want: []const u8,
) !void {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try writeFrame(&sink.writer, channel, body, payload, min_max_frame_size);
    try testing.expectEqualSlices(u8, want, sink.written());
}

test "the golden bytes of a transfer frame hold the header, the body, and the payload" {
    // 00 00 00 11: the size counts the 8 header octets, the 7 body octets, and
    // the 2 payload octets.
    // 02: the data offset, in 4-octet words.
    // 00: an AMQP frame.
    // 00 05: channel 5.
    // 00 53 14: a described type with the smallulong descriptor 0x14.
    // c0 02 01 43: a list8 of one field, and the field is the uint 0.
    // 68 69: the payload.
    try expectFrameBytes(5, .{ .transfer = .{ .handle = 0 } }, "hi", &.{
        0x00, 0x00, 0x00, 0x11,
        0x02, 0x00, 0x00, 0x05,
        0x00, 0x53, 0x14, 0xc0,
        0x02, 0x01, 0x43, 'h',
        'i',
    });
}

test "the golden bytes of an open frame carry the container id" {
    try expectFrameBytes(0, .{ .open = .{ .container_id = "c" } }, "", &.{
        0x00, 0x00, 0x00, 0x11,
        0x02, 0x00, 0x00, 0x00,
        0x00, 0x53, 0x10, 0xc0,
        0x04, 0x01, 0xa1, 0x01,
        'c',
    });
}

test "the golden bytes of a sasl frame hold the type code and a zero channel" {
    // The channel argument is 7, and the two octets still hold zero, because
    // section 5.3.1 ignores them.
    try expectFrameBytes(7, .{ .sasl_init = .{ .mechanism = .of("PLAIN") } }, "", &.{
        0x00, 0x00, 0x00, 0x15,
        0x02, 0x01, 0x00, 0x00,
        0x00, 0x53, 0x41, 0xc0,
        0x08, 0x01, 0xa3, 0x05,
        'P',  'L',  'A',  'I',
        'N',
    });
}

test "the golden bytes of the empty heartbeat frame" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();

    try writeEmptyFrame(&sink.writer, 0);
    try testing.expectEqualSlices(u8, &.{
        0x00, 0x00, 0x00, 0x08,
        0x02, 0x00, 0x00, 0x00,
    }, sink.written());
}

test "the golden bytes of both protocol headers" {
    try testing.expectEqualSlices(u8, &.{ 'A', 'M', 'Q', 'P', 0, 1, 0, 0 }, &amqp_protocol_header);
    try testing.expectEqualSlices(u8, &.{ 'A', 'M', 'Q', 'P', 3, 1, 0, 0 }, &sasl_protocol_header);
    try testing.expectEqual(@as(usize, 8), protocol_header_size);
}

test "the protocol headers hold the octets that the specification prints" {
    // The specification writes them as AMQP%d0.1.0.0 and AMQP%d3.1.0.0. A
    // string literal in a shell hides the NUL octet, so the test compares the
    // octets one at a time.
    try testing.expectEqual(@as(u8, 0x00), amqp_protocol_header[4]);
    try testing.expectEqual(@as(u8, 0x03), sasl_protocol_header[4]);
    for ([_][protocol_header_size]u8{ amqp_protocol_header, sasl_protocol_header }) |header| {
        try testing.expectEqualSlices(u8, "AMQP", header[0..4]);
        try testing.expectEqual(@as(u8, 0x01), header[5]);
        try testing.expectEqual(@as(u8, 0x00), header[6]);
        try testing.expectEqual(@as(u8, 0x00), header[7]);
    }
}

/// Reads one frame from `bytes` into `buf`.
///
/// The payload of the result borrows `buf`, so `buf` must live in the test that
/// reads the payload. A buffer in this function would die at the return.
fn readOne(gpa: Allocator, buf: []u8, bytes: []const u8, max_frame_size: u32) ReadFrameError!Frame {
    var reader: Reader = .fixed(bytes);
    return readFrame(gpa, &reader, buf, max_frame_size);
}

/// Reads one frame from `bytes` and frees it. The error tests use this
/// function, because they never look at the frame.
fn readAndDiscard(gpa: Allocator, bytes: []const u8, max_frame_size: u32) ReadFrameError!void {
    var buf: [min_max_frame_size]u8 = undefined;
    const frame = try readOne(gpa, &buf, bytes, max_frame_size);
    frame.deinit();
}

test "a written frame reads back with the same channel, body, and payload" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try writeFrame(&sink.writer, 9, .{ .transfer = .{ .handle = 3, .delivery_id = 4 } }, "abc", min_max_frame_size);

    var reader: Reader = .fixed(sink.written());
    var buf: [min_max_frame_size]u8 = undefined;
    const frame = try readFrame(gpa, &reader, &buf, min_max_frame_size);
    defer frame.deinit();

    try testing.expectEqual(FrameType.amqp, frame.frame_type);
    try testing.expectEqual(@as(u16, 9), frame.channel);
    try testing.expect(!frame.isEmpty());
    try testing.expectEqual(@as(u32, 3), frame.body.?.transfer.handle.?);
    try testing.expectEqual(@as(u32, 4), frame.body.?.transfer.delivery_id.?);
    try testing.expectEqualSlices(u8, "abc", frame.payload);
}

test "a written sasl frame reads back as a sasl frame on channel 0" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try writeFrame(&sink.writer, 7, .{ .sasl_outcome = .{ .code = .ok } }, "", min_max_frame_size);

    var buf: [min_max_frame_size]u8 = undefined;
    const frame = try readOne(gpa, &buf, sink.written(), min_max_frame_size);
    defer frame.deinit();

    try testing.expectEqual(FrameType.sasl, frame.frame_type);
    try testing.expectEqual(@as(u16, 0), frame.channel);
    try testing.expectEqual(performatives.SaslCode.ok, frame.body.?.sasl_outcome.code.?);
    try testing.expectEqual(@as(usize, 0), frame.payload.len);
}

test "an open frame with slices keeps them alive after the read" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try writeFrame(&sink.writer, 0, .{ .open = .{
        .container_id = "sender",
        .hostname = "host",
        .max_frame_size = 65536,
    } }, "", min_max_frame_size);

    var buf: [min_max_frame_size]u8 = undefined;
    const frame = try readOne(gpa, &buf, sink.written(), min_max_frame_size);
    defer frame.deinit();

    // The slices of the body live in the arena of the frame, not in `buf`, so
    // they must survive a write over the whole buffer.
    @memset(&buf, 0xff);
    try testing.expectEqualStrings("sender", frame.body.?.open.container_id.?);
    try testing.expectEqualStrings("host", frame.body.?.open.hostname.?);
    try testing.expectEqual(@as(u32, 65536), frame.body.?.open.max_frame_size.?);
}

test "the empty frame reads back as a frame with no body" {
    const gpa = testing.allocator;
    var buf: [min_max_frame_size]u8 = undefined;
    const frame = try readOne(gpa, &buf, &.{ 0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00 }, min_max_frame_size);
    defer frame.deinit();

    try testing.expect(frame.isEmpty());
    try testing.expectEqual(@as(?Body, null), frame.body);
    try testing.expectEqual(@as(u16, 0), frame.channel);
    try testing.expectEqual(@as(usize, 0), frame.payload.len);
}

test "an empty frame on a channel above 0 still reads" {
    // Section 2.4.5 makes an implementation accept an empty frame on any valid
    // channel.
    const gpa = testing.allocator;
    var buf: [min_max_frame_size]u8 = undefined;
    const frame = try readOne(gpa, &buf, &.{ 0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x01, 0x2c }, min_max_frame_size);
    defer frame.deinit();

    try testing.expect(frame.isEmpty());
    try testing.expectEqual(@as(u16, 300), frame.channel);
}

test "the reader skips an extended header" {
    const gpa = testing.allocator;
    var buf: [min_max_frame_size]u8 = undefined;
    const frame = try readOne(gpa, &buf, &extended_header_frame, min_max_frame_size);
    defer frame.deinit();

    try testing.expectEqual(@as(u16, 2), frame.channel);
    try testing.expectEqual(@as(u32, 0), frame.body.?.transfer.handle.?);
    try testing.expectEqualSlices(u8, "z", frame.payload);
}

test "an empty frame with an extended header and no body reads as empty" {
    const gpa = testing.allocator;
    var buf: [min_max_frame_size]u8 = undefined;
    const frame = try readOne(gpa, &buf, &.{
        0x00, 0x00, 0x00, 0x0c,
        0x03, 0x00, 0x00, 0x00,
        0x01, 0x02, 0x03, 0x04,
    }, min_max_frame_size);
    defer frame.deinit();

    try testing.expect(frame.isEmpty());
}

test "the writer rejects a frame above the negotiated size" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();

    // The frame takes 8 header octets, 7 body octets, and 200 payload octets.
    const payload: [200]u8 = @splat('x');
    try testing.expectError(error.FrameTooLarge, writeFrame(
        &sink.writer,
        0,
        .{ .transfer = .{ .handle = 0 } },
        &payload,
        214,
    ));
    // The same frame fits in one more octet.
    try writeFrame(&sink.writer, 0, .{ .transfer = .{ .handle = 0 } }, &payload, 215);
    try testing.expectEqual(@as(usize, 215), sink.written().len);
}

test "the reader rejects a frame above the negotiated size" {
    const gpa = testing.allocator;
    // The header names 300 octets, and the peer negotiated 256.
    try testing.expectError(error.FrameTooLarge, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x01, 0x2c, 0x02, 0x00, 0x00, 0x00,
    }, 256));
    // The same header passes when the negotiated size is large enough. The
    // body is absent, so the read stops at the end of the stream.
    try testing.expectError(error.EndOfStream, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x01, 0x2c, 0x02, 0x00, 0x00, 0x00,
    }, 512));
}

test "a sasl frame keeps the fixed limit above the negotiated size" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();

    // The peers negotiated 65536, and section 5.3.1 still holds a SASL frame
    // at 512 octets.
    const large: [600]u8 = @splat('x');
    try testing.expectError(error.FrameTooLarge, writeFrame(
        &sink.writer,
        0,
        .{ .sasl_init = .{ .initial_response = .of(&large) } },
        "",
        65536,
    ));

    // The reader applies the same limit to a SASL header of 513 octets.
    try testing.expectError(error.FrameTooLarge, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x02, 0x01, 0x02, 0x01, 0x00, 0x00,
    }, 65536));
    // An AMQP header of the same size passes the limit, and then the read
    // stops at the end of the stream.
    try testing.expectError(error.EndOfStream, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x02, 0x01, 0x02, 0x00, 0x00, 0x00,
    }, 65536));
}

test "the reader rejects a size below the frame header" {
    const gpa = testing.allocator;
    for ([_]u8{ 0, 1, 7 }) |size| {
        try testing.expectError(error.Malformed, readAndDiscard(gpa, &.{
            0x00, 0x00, 0x00, size, 0x02, 0x00, 0x00, 0x00,
        }, min_max_frame_size));
    }
}

test "the reader rejects a data offset below 2" {
    const gpa = testing.allocator;
    for ([_]u8{ 0, 1 }) |data_offset| {
        try testing.expectError(error.Malformed, readAndDiscard(gpa, &.{
            0x00, 0x00, 0x00, 0x08, data_offset, 0x00, 0x00, 0x00,
        }, min_max_frame_size));
    }
}

test "the reader rejects a data offset that points past the frame" {
    const gpa = testing.allocator;
    // The size is 12 and the data offset names octet 16.
    try testing.expectError(error.Malformed, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x00, 0x0c, 0x04, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    }, min_max_frame_size));
    // The largest data offset overflows nothing, and it still reports the
    // malformed frame.
    try testing.expectError(error.Malformed, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x00, 0x08, 0xff, 0x00, 0x00, 0x00,
    }, min_max_frame_size));
}

test "the reader rejects a truncated header and a truncated body" {
    const gpa = testing.allocator;
    try testing.expectError(error.EndOfStream, readAndDiscard(gpa, &.{ 0x00, 0x00, 0x00 }, min_max_frame_size));
    try testing.expectError(error.EndOfStream, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x00, 0x11, 0x02, 0x00, 0x00, 0x05,
        0x00, 0x53, 0x14,
    }, min_max_frame_size));
}

test "the reader rejects an unknown frame type" {
    const gpa = testing.allocator;
    try testing.expectError(error.UnknownFrameType, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x00, 0x08, 0x02, 0x02, 0x00, 0x00,
    }, min_max_frame_size));
}

test "the reader rejects an unknown descriptor" {
    const gpa = testing.allocator;
    // The descriptor 0x99 names no performative of this library.
    try testing.expectError(error.UnknownPerformative, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x00, 0x0f, 0x02, 0x00, 0x00, 0x00,
        0x00, 0x53, 0x99, 0xc0, 0x02, 0x01, 0x43,
    }, min_max_frame_size));
}

test "the reader rejects a body that is not a described type" {
    const gpa = testing.allocator;
    try testing.expectError(error.Malformed, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x00, 0x09, 0x02, 0x00, 0x00, 0x00, 0x40,
    }, min_max_frame_size));
}

test "the reader rejects a sasl body in an amqp frame" {
    const gpa = testing.allocator;
    // The descriptor 0x44 names sasl-outcome, and the type code says AMQP.
    try testing.expectError(error.Malformed, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x00, 0x10, 0x02, 0x00, 0x00, 0x00,
        0x00, 0x53, 0x44, 0xc0, 0x03, 0x01, 0x50, 0x00,
    }, min_max_frame_size));
}

test "the reader rejects an amqp body in a sasl frame" {
    const gpa = testing.allocator;
    try testing.expectError(error.Malformed, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x00, 0x0f, 0x02, 0x01, 0x00, 0x00,
        0x00, 0x53, 0x14, 0xc0, 0x02, 0x01, 0x43,
    }, min_max_frame_size));
}

test "the reader rejects an empty sasl frame" {
    const gpa = testing.allocator;
    try testing.expectError(error.EmptySaslFrame, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x00, 0x08, 0x02, 0x01, 0x00, 0x00,
    }, min_max_frame_size));
}

test "the reader rejects a payload after a sasl body" {
    const gpa = testing.allocator;
    try testing.expectError(error.Malformed, readAndDiscard(gpa, &.{
        0x00, 0x00, 0x00, 0x12, 0x02, 0x01, 0x00, 0x00,
        0x00, 0x53, 0x44, 0xc0, 0x03, 0x01, 0x50, 0x00,
        'n',  'o',
    }, min_max_frame_size));
}

test "the reader rejects a frame that is larger than the buffer" {
    const gpa = testing.allocator;
    var reader: Reader = .fixed(&.{ 0x00, 0x00, 0x00, 0x40, 0x02, 0x00, 0x00, 0x00 });
    var buf: [16]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, readFrame(gpa, &reader, &buf, min_max_frame_size));
}

test "the reader accepts the symbolic descriptor" {
    const gpa = testing.allocator;
    // 00 a3 0f "amqp:close:list", and then the empty list.
    var buf: [min_max_frame_size]u8 = undefined;
    const frame = try readOne(gpa, &buf, &.{
        0x00, 0x00, 0x00, 0x1b, 0x02, 0x00, 0x00, 0x00,
        0x00, 0xa3, 0x0f, 'a',  'm',  'q',  'p',  ':',
        'c',  'l',  'o',  's',  'e',  ':',  'l',  'i',
        's',  't',  0x45,
    }, min_max_frame_size);
    defer frame.deinit();

    try testing.expect(frame.body.? == .close);
}

test "a frame of the largest negotiated size still reads" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();

    // The header takes 8 octets and the transfer body takes 7, so the payload
    // fills the frame to exactly 512 octets.
    const payload: [min_max_frame_size - 15]u8 = @splat('p');
    try writeFrame(&sink.writer, 1, .{ .transfer = .{ .handle = 0 } }, &payload, min_max_frame_size);
    try testing.expectEqual(@as(usize, min_max_frame_size), sink.written().len);

    var buf: [min_max_frame_size]u8 = undefined;
    const frame = try readOne(gpa, &buf, sink.written(), min_max_frame_size);
    defer frame.deinit();
    try testing.expectEqualSlices(u8, &payload, frame.payload);
}

test "every body type round trips through a frame" {
    const gpa = testing.allocator;
    const bodies = [_]Body{
        .{ .open = .{ .container_id = "c" } },
        .{ .begin = .{ .next_outgoing_id = 1 } },
        .{ .attach = .{ .name = "link", .handle = 0 } },
        .{ .flow = .{ .incoming_window = 10 } },
        .{ .transfer = .{ .handle = 0 } },
        .{ .disposition = .{ .first = 1 } },
        .{ .detach = .{ .handle = 0 } },
        .{ .end = .{} },
        .{ .close = .{} },
        .{ .sasl_mechanisms = .{ .sasl_server_mechanisms = &.{.of("PLAIN")} } },
        .{ .sasl_init = .{ .mechanism = .of("ANONYMOUS") } },
        .{ .sasl_outcome = .{ .code = .ok } },
    };

    for (bodies) |body| {
        var sink: Writer.Allocating = .init(gpa);
        defer sink.deinit();
        try writeFrame(&sink.writer, 0, body, "", min_max_frame_size);

        var buf: [min_max_frame_size]u8 = undefined;
        const frame = try readOne(gpa, &buf, sink.written(), min_max_frame_size);
        defer frame.deinit();
        try testing.expectEqual(body.frameType(), frame.frame_type);
        try testing.expectEqual(std.meta.activeTag(body), std.meta.activeTag(frame.body.?));
    }
}

test "the protocol header exchange accepts a matching answer" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    var reader: Reader = .fixed(&amqp_protocol_header);

    try exchangeProtocolHeader(&reader, &sink.writer, amqp_protocol_header, null);
    try testing.expectEqualSlices(u8, &amqp_protocol_header, sink.written());
}

test "the protocol header exchange reports the header that the peer sent" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    // The client asks for the AMQP layer, and the server answers that it wants
    // the SASL layer first.
    var reader: Reader = .fixed(&sasl_protocol_header);

    var mismatch: ProtocolHeaderMismatch = undefined;
    try testing.expectError(
        error.ProtocolHeaderMismatch,
        exchangeProtocolHeader(&reader, &sink.writer, amqp_protocol_header, &mismatch),
    );
    try testing.expectEqualSlices(u8, &amqp_protocol_header, &mismatch.sent);
    try testing.expectEqualSlices(u8, &sasl_protocol_header, &mismatch.received);

    var text: [header_text_size]u8 = undefined;
    try testing.expectEqualStrings("AMQP 3.1.0.0", describeProtocolHeader(mismatch.received, &text));
    try testing.expectEqualStrings("AMQP 0.1.0.0", describeProtocolHeader(mismatch.sent, &text));
}

test "the protocol header exchange reports a short answer" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    var reader: Reader = .fixed("AMQP");

    try testing.expectError(
        error.EndOfStream,
        exchangeProtocolHeader(&reader, &sink.writer, sasl_protocol_header, null),
    );
}

test "a header of another protocol reads as hexadecimal" {
    var text: [header_text_size]u8 = undefined;
    try testing.expectEqualStrings(
        "474554202f204854",
        describeProtocolHeader(.{ 'G', 'E', 'T', ' ', '/', ' ', 'H', 'T' }, &text),
    );
}

/// Reads one frame from `bytes` and frees it. The function returns true when
/// the read produced a frame, so a fuzz test can count the inputs that reach
/// the decoder.
fn checkReadFrame(gpa: Allocator, bytes: []const u8) !bool {
    var reader: Reader = .fixed(bytes);
    var buf: [min_max_frame_size]u8 = undefined;
    const frame = readFrame(gpa, &reader, &buf, min_max_frame_size) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return false,
    };
    defer frame.deinit();

    // A frame that reads must hold the rules of section 2.3.1.
    const size = std.mem.readInt(u32, bytes[0..4], .big);
    try testing.expect(size >= frame_header_size);
    try testing.expect(size <= min_max_frame_size);
    try testing.expect(bytes[4] >= min_data_offset);
    try testing.expect(frame.payload.len <= size - frame_header_size);
    if (frame.frame_type == .sasl) {
        try testing.expectEqual(@as(u16, 0), frame.channel);
        try testing.expectEqual(@as(usize, 0), frame.payload.len);
    }
    return true;
}

fn fuzzReadFrame(_: void, smith: *testing.Smith) anyerror!void {
    var buffer: [min_max_frame_size]u8 = undefined;
    const len = smith.slice(&buffer);
    if (len < 4) return;
    _ = try checkReadFrame(testing.allocator, buffer[0..len]);
}

test "fuzz the frame reader" {
    try testing.fuzz({}, fuzzReadFrame, .{ .corpus = &.{
        corpusEntry(&empty_frame),
        corpusEntry(&transfer_frame),
        corpusEntry(&sasl_frame),
        corpusEntry(&close_frame),
        corpusEntry(&open_frame),
        corpusEntry(&extended_header_frame),
    } });
}

/// Wraps `bytes` in the form that `std.testing.Smith` reads for a slice: a
/// 4-octet little-endian length, and then the bytes.
fn corpusEntry(comptime bytes: []const u8) []const u8 {
    const len: u32 = @intCast(bytes.len);
    const header: [4]u8 = .{
        @truncate(len),
        @truncate(len >> 8),
        @truncate(len >> 16),
        @truncate(len >> 24),
    };
    return &(header ++ bytes[0..bytes.len].*);
}

const empty_frame = [_]u8{ 0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00 };

const transfer_frame = [_]u8{
    0x00, 0x00, 0x00, 0x11, 0x02, 0x00, 0x00, 0x05,
    0x00, 0x53, 0x14, 0xc0, 0x02, 0x01, 0x43, 'h',
    'i',
};

const sasl_frame = [_]u8{
    0x00, 0x00, 0x00, 0x10, 0x02, 0x01, 0x00, 0x00,
    0x00, 0x53, 0x44, 0xc0, 0x03, 0x01, 0x50, 0x00,
};

const close_frame = [_]u8{
    0x00, 0x00, 0x00, 0x0c, 0x02, 0x00, 0x00, 0x00,
    0x00, 0x53, 0x18, 0x45,
};

const open_frame = [_]u8{
    0x00, 0x00, 0x00, 0x11, 0x02, 0x00, 0x00, 0x00,
    0x00, 0x53, 0x10, 0xc0, 0x04, 0x01, 0xa1, 0x01,
    'c',
};

// The data offset is 3, so 4 octets of extended header sit between the header
// and the body.
const extended_header_frame = [_]u8{
    0x00, 0x00, 0x00, 0x14, 0x03, 0x00, 0x00, 0x02,
    0xde, 0xad, 0xbe, 0xef, 0x00, 0x53, 0x14, 0xc0,
    0x02, 0x01, 0x43, 'z',
};

/// The legal frames that the mutation test starts from.
const fuzz_seeds: []const []const u8 = &.{
    &empty_frame,
    &transfer_frame,
    &sasl_frame,
    &close_frame,
    &open_frame,
    &extended_header_frame,
};

test "every fuzz seed is a legal frame" {
    // A seed that the reader rejects gives the mutation test below a much
    // smaller reach, and nothing else would report it.
    for (fuzz_seeds) |seed| {
        try testing.expect(try checkReadFrame(testing.allocator, seed));
    }
}

// `std.testing.fuzz` needs `zig build test --fuzz`, and that mode does not
// build with every toolchain. This test drives the same rules from a seeded
// generator, so `zig build test` alone still exercises the reader against many
// inputs. It mutates legal frames, because purely random octets fail the size
// test almost every time.
test "the frame reader survives mutated frames" {
    const gpa = testing.allocator;
    var prng: std.Random.DefaultPrng = .init(0x4652414d);
    const random = prng.random();

    var read: usize = 0;
    var buffer: [min_max_frame_size]u8 = undefined;
    for (0..20000) |_| {
        const seed = fuzz_seeds[random.uintLessThan(usize, fuzz_seeds.len)];
        @memcpy(buffer[0..seed.len], seed);
        var input: []u8 = buffer[0..seed.len];

        for (0..random.uintLessThan(usize, 4) + 1) |_| {
            switch (random.uintLessThan(u8, 4)) {
                // Change one octet.
                0 => input[random.uintLessThan(usize, input.len)] = random.int(u8),
                // Change one octet of the header, because a random position
                // reaches the header in one input of two.
                1 => input[random.uintLessThan(usize, @min(input.len, frame_header_size))] = random.int(u8),
                // Cut the input short.
                2 => if (input.len > 1) {
                    input = input[0..random.uintLessThan(usize, input.len)];
                },
                // Add one octet, and keep the size field honest.
                else => if (input.len < buffer.len) {
                    buffer[input.len] = random.int(u8);
                    input = buffer[0 .. input.len + 1];
                    std.mem.writeInt(u32, input[0..4], @intCast(input.len), .big);
                },
            }
            if (input.len < 4) break;
        }

        if (input.len < 4) continue;
        if (try checkReadFrame(gpa, input)) read += 1;
    }

    // The seeded run reads a frame from about 2750 of the 20000 inputs. A much
    // smaller number means that the generator stopped making legal frames, and
    // that the test no longer reaches the body of the reader.
    try testing.expect(read > 1000);
}

test "the frame reader survives random octets" {
    const gpa = testing.allocator;
    var prng: std.Random.DefaultPrng = .init(0x48454144);
    const random = prng.random();

    var buffer: [128]u8 = undefined;
    for (0..20000) |_| {
        const len = random.uintLessThan(usize, buffer.len - 4) + 4;
        random.bytes(buffer[0..len]);
        // Hold the size in a range that the reader can accept, so the test
        // reaches more than the size rule.
        std.mem.writeInt(u32, buffer[0..4], random.uintLessThan(u32, 600), .big);
        _ = try checkReadFrame(gpa, buffer[0..len]);
    }
}
