//! The AMQP 1.0 message format.
//!
//! A message is a sequence of described sections, and not one value. This file
//! holds `Message`, which carries the sections of message format 0, and the two
//! composite sections `Header` and `Properties`.
//!
//! Specification: OASIS AMQP Version 1.0 Part 3: Messaging, section 3.2.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html
//!
//! # The order of the sections
//!
//! Section 3.2 gives this order, and every section is optional:
//!
//! 1. Zero or one header sections.
//! 2. Zero or one delivery-annotations sections.
//! 3. Zero or one message-annotations sections.
//! 4. Zero or one properties sections.
//! 5. Zero or one application-properties sections.
//! 6. The body, which holds one or more data sections, or one or more
//!    amqp-sequence sections, or one amqp-value section.
//! 7. Zero or one footer sections.
//!
//! The encoder writes the sections in this order. The decoder rejects a message
//! that breaks the order, that repeats a section which cannot repeat, or that
//! mixes two kinds of body section.
//!
//! The decoder accepts a message that holds no body, because a sender can split
//! one message across several transfer frames. Join the payloads of the frames
//! of one delivery, and then decode the result.
//!
//! # The maps are generic
//!
//! The annotation maps, the application properties, and the footer are plain
//! AMQP maps. This module gives no name to any entry of them. The caller
//! supplies every key and every value.
//!
//! # Ownership
//!
//! A message that you build yourself owns nothing, so you free nothing.
//! `decode` allocates, and it returns a `Decoded` wrapper that owns every byte
//! below it. Free the whole result with one call to `Decoded.deinit`.

const std = @import("std");
const codec = @import("codec.zig");
const performatives = @import("performatives.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Binary = performatives.Binary;
const Decoded = performatives.Decoded;
const MapEntry = types.MapEntry;
const Symbol = performatives.Symbol;
const Value = types.Value;
const Writer = std.Io.Writer;

/// The errors that the encode path returns.
pub const EncodeError = codec.EncodeError;

/// The errors that the decode path returns.
pub const DecodeError = codec.DecodeError;

/// The message format that this file implements. Specification section 3.2.16.
pub const message_format: u32 = 0;

/// The descriptor code of each message section. Specification section 3.2.
pub const SectionCode = enum(u64) {
    header = 0x0000_0000_0000_0070,
    delivery_annotations = 0x0000_0000_0000_0071,
    message_annotations = 0x0000_0000_0000_0072,
    properties = 0x0000_0000_0000_0073,
    application_properties = 0x0000_0000_0000_0074,
    data = 0x0000_0000_0000_0075,
    amqp_sequence = 0x0000_0000_0000_0076,
    amqp_value = 0x0000_0000_0000_0077,
    footer = 0x0000_0000_0000_0078,

    /// Returns the symbolic descriptor that the specification gives for the
    /// section.
    pub fn symbolText(self: SectionCode) []const u8 {
        return switch (self) {
            .header => "amqp:header:list",
            .delivery_annotations => "amqp:delivery-annotations:map",
            .message_annotations => "amqp:message-annotations:map",
            .properties => "amqp:properties:list",
            .application_properties => "amqp:application-properties:map",
            .data => "amqp:data:binary",
            .amqp_sequence => "amqp:amqp-sequence:list",
            .amqp_value => "amqp:amqp-value:*",
            .footer => "amqp:footer:map",
        };
    }

    /// Returns the section that `descriptor` names, or null when it names no
    /// section. AMQP allows a numeric descriptor and a symbolic descriptor, so
    /// this function takes both.
    pub fn fromDescriptor(descriptor: Value) ?SectionCode {
        switch (descriptor) {
            .ulong => |code| return std.enums.fromInt(SectionCode, code),
            .symbol => |name| {
                inline for (@typeInfo(SectionCode).@"enum".fields) |field| {
                    const code: SectionCode = @enumFromInt(field.value);
                    if (std.mem.eql(u8, name, code.symbolText())) return code;
                }
                return null;
            },
            else => return null,
        }
    }

    /// Returns the position of the section in the order of section 3.2. Two
    /// body sections share one position.
    fn order(self: SectionCode) u8 {
        return switch (self) {
            .header => 0,
            .delivery_annotations => 1,
            .message_annotations => 2,
            .properties => 3,
            .application_properties => 4,
            .data, .amqp_sequence, .amqp_value => 5,
            .footer => 6,
        };
    }
};

/// The position of the body in the order of the sections.
const body_order: u8 = 5;

/// The transport headers of a message. Specification section 3.2.1.
pub const Header = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0070;
    pub const descriptor_name = "amqp:header:list";

    /// True when the message is durable. Default false.
    durable: ?bool = null,
    /// The relative priority of the message. Default 4.
    priority: ?u8 = null,
    /// The time to live in milliseconds.
    ttl: ?u32 = null,
    /// True when the message has not been acquired by any other link before.
    /// Default false.
    first_acquirer: ?bool = null,
    /// The number of earlier delivery attempts that failed. Default 0.
    delivery_count: ?u32 = null,

    pub fn encode(self: Header, w: *Writer) EncodeError!void {
        return performatives.writeComposite(Header, self, w);
    }

    pub fn encodedSize(self: Header) EncodeError!usize {
        return performatives.compositeSize(Header, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Header) {
        return performatives.decodeComposite(Header, gpa, bytes);
    }
};

/// The immutable properties of a message. Specification section 3.2.4.
pub const Properties = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0073;
    pub const descriptor_name = "amqp:properties:list";

    /// The id of the message. The specification allows a ulong, a uuid, a
    /// binary, or a string.
    message_id: ?Value = null,
    /// The identity of the user that sends the message.
    user_id: ?Binary = null,
    /// The address of the node that the message goes to.
    to: ?Value = null,
    /// The subject of the message.
    subject: ?[]const u8 = null,
    /// The address of the node to send an answer to.
    reply_to: ?Value = null,
    /// The id of the message that this message answers.
    correlation_id: ?Value = null,
    /// The MIME type of the body.
    content_type: ?Symbol = null,
    /// The content encoding of the body.
    content_encoding: ?Symbol = null,
    /// The time when the message expires, in milliseconds after the epoch.
    absolute_expiry_time: ?i64 = null,
    /// The time when the message was made, in milliseconds after the epoch.
    creation_time: ?i64 = null,
    /// The group that the message belongs to.
    group_id: ?[]const u8 = null,
    /// The position of the message inside its group.
    group_sequence: ?u32 = null,
    /// The group to send an answer to.
    reply_to_group_id: ?[]const u8 = null,

    pub fn encode(self: Properties, w: *Writer) EncodeError!void {
        return performatives.writeComposite(Properties, self, w);
    }

    pub fn encodedSize(self: Properties) EncodeError!usize {
        return performatives.compositeSize(Properties, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Properties) {
        return performatives.decodeComposite(Properties, gpa, bytes);
    }
};

/// The body of a message. Specification section 3.2 gives three choices, and a
/// message that carries no body at all takes `empty`.
pub const Body = union(enum) {
    /// The message holds no body section.
    empty,
    /// One data section for each item. A receiver joins the items to get the
    /// whole body.
    data: []const []const u8,
    /// One amqp-sequence section for each item. Each item is a list.
    sequence: []const []const Value,
    /// One amqp-value section, which holds one value of any type.
    value: Value,
};

/// One AMQP 1.0 message of format 0.
pub const Message = struct {
    /// The transport headers.
    header: ?Header = null,
    /// The annotations of the delivery. An intermediary can change them.
    delivery_annotations: ?[]const MapEntry = null,
    /// The annotations of the message.
    message_annotations: ?[]const MapEntry = null,
    /// The immutable properties.
    properties: ?Properties = null,
    /// The properties of the application.
    application_properties: ?[]const MapEntry = null,
    /// The body.
    body: Body = .empty,
    /// The footer, which carries details of the message such as a hash.
    footer: ?[]const MapEntry = null,

    /// Writes the sections of the message to `w`, in the order of section 3.2.
    pub fn encode(self: Message, w: *Writer) EncodeError!void {
        if (self.header) |header| try header.encode(w);
        if (self.delivery_annotations) |map| {
            try writeSection(.delivery_annotations, .{ .map = map }, w);
        }
        if (self.message_annotations) |map| {
            try writeSection(.message_annotations, .{ .map = map }, w);
        }
        if (self.properties) |properties| try properties.encode(w);
        if (self.application_properties) |map| {
            try writeSection(.application_properties, .{ .map = map }, w);
        }
        switch (self.body) {
            .empty => {},
            .data => |items| for (items) |item| {
                try writeSection(.data, .{ .binary = item }, w);
            },
            .sequence => |items| for (items) |item| {
                try writeSection(.amqp_sequence, .{ .list = item }, w);
            },
            .value => |value| try writeSection(.amqp_value, value, w),
        }
        if (self.footer) |map| try writeSection(.footer, .{ .map = map }, w);
    }

    /// Returns the number of bytes that `encode` writes.
    pub fn encodedSize(self: Message) EncodeError!usize {
        var total: usize = 0;
        if (self.header) |header| total += try header.encodedSize();
        if (self.delivery_annotations) |map| {
            total += try sectionSize(.delivery_annotations, .{ .map = map });
        }
        if (self.message_annotations) |map| {
            total += try sectionSize(.message_annotations, .{ .map = map });
        }
        if (self.properties) |properties| total += try properties.encodedSize();
        if (self.application_properties) |map| {
            total += try sectionSize(.application_properties, .{ .map = map });
        }
        switch (self.body) {
            .empty => {},
            .data => |items| for (items) |item| {
                total += try sectionSize(.data, .{ .binary = item });
            },
            .sequence => |items| for (items) |item| {
                total += try sectionSize(.amqp_sequence, .{ .list = item });
            },
            .value => |value| total += try sectionSize(.amqp_value, value),
        }
        if (self.footer) |map| total += try sectionSize(.footer, .{ .map = map });
        return total;
    }

    /// Reads a whole message from `bytes`.
    ///
    /// The result owns its memory. Free it with `Decoded.deinit`.
    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Message) {
        var arena: std.heap.ArenaAllocator = .init(gpa);
        errdefer arena.deinit();
        const alloc = arena.allocator();

        var message: Message = .{};
        var data: std.ArrayList([]const u8) = .empty;
        var sequence: std.ArrayList([]const Value) = .empty;

        // The order of the last section that the loop read. It starts below
        // the first position, so the first section always passes the test.
        var previous: i16 = -1;

        // One decoder reads the whole message, so the allocation budget of the
        // primitive decoder covers every section together.
        var decoder: codec.Decoder = .init(bytes);
        while (!decoder.atEnd()) {
            const section = try decoder.next(alloc);
            if (section != .described) return error.Malformed;
            const code = SectionCode.fromDescriptor(section.described.descriptor.*) orelse
                return error.Malformed;
            const body = section.described.value.*;

            const position: i16 = code.order();
            if (position < previous) return error.Malformed;
            // Only the body holds more than one section.
            if (position == previous and position != body_order) return error.Malformed;
            previous = position;

            switch (code) {
                .header => message.header = try performatives.compositeFromValue(
                    Header,
                    section,
                    alloc,
                ),
                .properties => message.properties = try performatives.compositeFromValue(
                    Properties,
                    section,
                    alloc,
                ),
                .delivery_annotations => message.delivery_annotations = try mapOf(body),
                .message_annotations => message.message_annotations = try mapOf(body),
                .application_properties => message.application_properties = try mapOf(body),
                .footer => message.footer = try mapOf(body),
                .data => {
                    if (message.body != .empty and message.body != .data) return error.Malformed;
                    message.body = .{ .data = &.{} };
                    try data.append(alloc, switch (body) {
                        .binary => |item| item,
                        else => return error.Malformed,
                    });
                },
                .amqp_sequence => {
                    if (message.body != .empty and message.body != .sequence) {
                        return error.Malformed;
                    }
                    message.body = .{ .sequence = &.{} };
                    try sequence.append(alloc, switch (body) {
                        .list => |item| item,
                        else => return error.Malformed,
                    });
                },
                .amqp_value => {
                    // Only one amqp-value section can stand in a message.
                    if (message.body != .empty) return error.Malformed;
                    message.body = .{ .value = body };
                },
            }
        }

        switch (message.body) {
            .data => message.body = .{ .data = try data.toOwnedSlice(alloc) },
            .sequence => message.body = .{ .sequence = try sequence.toOwnedSlice(alloc) },
            .empty, .value => {},
        }

        return .{ .value = message, .arena_state = arena.state, .child = gpa };
    }
};

fn mapOf(body: Value) DecodeError![]const MapEntry {
    return switch (body) {
        .map => |entries| entries,
        else => error.Malformed,
    };
}

fn writeSection(code: SectionCode, body: Value, w: *Writer) EncodeError!void {
    const descriptor: Value = .{ .ulong = @intFromEnum(code) };
    return codec.encode(.{ .described = .{ .descriptor = &descriptor, .value = &body } }, w);
}

fn sectionSize(code: SectionCode, body: Value) EncodeError!usize {
    const descriptor: Value = .{ .ulong = @intFromEnum(code) };
    return codec.encodedSize(.{ .described = .{ .descriptor = &descriptor, .value = &body } });
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

const testing = std.testing;

/// Checks both directions against one golden byte vector.
fn expectGolden(message: Message, bytes: []const u8) !void {
    const gpa = testing.allocator;

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try message.encode(&sink.writer);
    try testing.expectEqualSlices(u8, bytes, sink.written());
    try testing.expectEqual(bytes.len, try message.encodedSize());

    const decoded = try Message.decode(gpa, bytes);
    defer decoded.deinit();
    try testing.expectEqualDeep(message, decoded.value);
}

const delivery_entries: []const MapEntry = &.{
    .{ .key = .{ .symbol = "d" }, .value = .{ .uint = 1 } },
};
const message_entries: []const MapEntry = &.{
    .{ .key = .{ .symbol = "m" }, .value = .{ .uint = 2 } },
};
const application_entries: []const MapEntry = &.{
    .{ .key = .{ .string = "a" }, .value = .{ .string = "b" } },
};
const footer_entries: []const MapEntry = &.{
    .{ .key = .{ .symbol = "f" }, .value = .{ .uint = 3 } },
};

test "golden vector: a message that holds every section" {
    try expectGolden(.{
        .header = .{ .durable = true },
        .delivery_annotations = delivery_entries,
        .message_annotations = message_entries,
        .properties = .{ .message_id = .{ .ulong = 1 }, .subject = "s" },
        .application_properties = application_entries,
        .body = .{ .data = &.{ "x", "y" } },
        .footer = footer_entries,
    }, &.{
        // header
        0x00, 0x53, 0x70, 0xc0, 0x02, 0x01, 0x41,
        // delivery-annotations
        0x00, 0x53, 0x71, 0xc1, 0x06, 0x02, 0xa3,
        0x01, 'd',  0x52, 0x01,
        // message-annotations
        0x00, 0x53, 0x72,
        0xc1, 0x06, 0x02, 0xa3, 0x01, 'm',  0x52,
        0x02,
        // properties
        0x00, 0x53, 0x73, 0xc0, 0x08, 0x04,
        0x53, 0x01, 0x40, 0x40, 0xa1, 0x01, 's',
        // application-properties
        0x00, 0x53, 0x74, 0xc1, 0x07, 0x02, 0xa1,
        0x01, 'a',  0xa1, 0x01, 'b',
        // the first data section
         0x00, 0x53,
        0x75, 0xa0, 0x01, 'x',
        // the second data section
         0x00, 0x53, 0x75,
        0xa0, 0x01, 'y',
        // footer
         0x00, 0x53, 0x78, 0xc1,
        0x06, 0x02, 0xa3, 0x01, 'f',  0x52, 0x03,
    });
}

test "golden vector: a message that holds one amqp-value section" {
    try expectGolden(.{
        .body = .{ .value = .{ .string = "hello" } },
    }, &.{
        0x00, 0x53, 0x77, 0xa1, 0x05, 'h', 'e', 'l', 'l', 'o',
    });
}

test "golden vector: a message that holds two amqp-sequence sections" {
    try expectGolden(.{
        .body = .{ .sequence = &.{
            &.{ .{ .uint = 1 }, .{ .string = "a" } },
            &.{.{ .boolean = true }},
        } },
    }, &.{
        0x00, 0x53, 0x76, 0xc0, 0x06, 0x02, 0x52, 0x01, 0xa1, 0x01, 'a',
        0x00, 0x53, 0x76, 0xc0, 0x02, 0x01, 0x41,
    });
}

test "golden vector: a message that holds a header only" {
    try expectGolden(.{
        .header = .{ .durable = true, .priority = 9, .ttl = 1000 },
    }, &.{
        0x00, 0x53, 0x70, 0xc0, 0x09, 0x03, 0x41, 0x50,
        0x09, 0x70, 0x00, 0x00, 0x03, 0xe8,
    });
}

test "the field order of properties follows the specification" {
    const gpa = testing.allocator;
    const properties: Properties = .{
        .message_id = .{ .ulong = 1 },
        .user_id = .of("u"),
        .to = .{ .string = "t" },
        .subject = "s",
        .reply_to = .{ .string = "r" },
        .correlation_id = .{ .uuid = [_]u8{0x11} ** 16 },
        .content_type = .of("text/plain"),
        .content_encoding = .of("identity"),
        .absolute_expiry_time = 2,
        .creation_time = 3,
        .group_id = "g",
        .group_sequence = 4,
        .reply_to_group_id = "rg",
    };

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try properties.encode(&sink.writer);

    const tree = try codec.decode(gpa, sink.written());
    defer tree.deinit(gpa);
    try testing.expectEqualDeep(Value{ .ulong = Properties.descriptor_code }, tree.described.descriptor.*);
    try testing.expectEqualDeep(@as([]const Value, &.{
        .{ .ulong = 1 },
        .{ .binary = "u" },
        .{ .string = "t" },
        .{ .string = "s" },
        .{ .string = "r" },
        .{ .uuid = [_]u8{0x11} ** 16 },
        .{ .symbol = "text/plain" },
        .{ .symbol = "identity" },
        .{ .timestamp = 2 },
        .{ .timestamp = 3 },
        .{ .string = "g" },
        .{ .uint = 4 },
        .{ .string = "rg" },
    }), tree.described.value.list);

    const decoded = try Properties.decode(gpa, sink.written());
    defer decoded.deinit();
    try testing.expectEqualDeep(properties, decoded.value);
}

test "the field order of header follows the specification" {
    const gpa = testing.allocator;
    const header: Header = .{
        .durable = true,
        .priority = 1,
        .ttl = 2,
        .first_acquirer = false,
        .delivery_count = 3,
    };

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try header.encode(&sink.writer);

    const tree = try codec.decode(gpa, sink.written());
    defer tree.deinit(gpa);
    try testing.expectEqualDeep(Value{ .ulong = Header.descriptor_code }, tree.described.descriptor.*);
    try testing.expectEqualDeep(@as([]const Value, &.{
        .{ .boolean = true },
        .{ .ubyte = 1 },
        .{ .uint = 2 },
        .{ .boolean = false },
        .{ .uint = 3 },
    }), tree.described.value.list);
}

test "a message with no section decodes to an empty message" {
    const gpa = testing.allocator;
    const decoded = try Message.decode(gpa, &.{});
    defer decoded.deinit();
    try testing.expectEqualDeep(Message{}, decoded.value);
}

test "the decoder takes a symbolic section descriptor" {
    const gpa = testing.allocator;
    const bytes = [_]u8{
        0x00, 0xa3, 0x11, 'a', 'm', 'q', 'p', ':', 'a', 'm',  'q',
        'p',  '-',  'v',  'a', 'l', 'u', 'e', ':', '*', 0x52, 0x07,
    };
    const decoded = try Message.decode(gpa, &bytes);
    defer decoded.deinit();
    try testing.expectEqualDeep(Message{ .body = .{ .value = .{ .uint = 7 } } }, decoded.value);
}

test "the decoder rejects a section that stands out of order" {
    const gpa = testing.allocator;
    // The properties section stands after the body.
    const bytes = [_]u8{
        0x00, 0x53, 0x75, 0xa0, 0x01, 'x',
        0x00, 0x53, 0x73, 0x45,
    };
    try testing.expectError(error.Malformed, Message.decode(gpa, &bytes));
}

test "the decoder rejects a section that repeats" {
    const gpa = testing.allocator;
    const bytes = [_]u8{
        0x00, 0x53, 0x70, 0x45,
        0x00, 0x53, 0x70, 0x45,
    };
    try testing.expectError(error.Malformed, Message.decode(gpa, &bytes));
}

test "the decoder rejects two kinds of body section" {
    const gpa = testing.allocator;

    const data_then_sequence = [_]u8{
        0x00, 0x53, 0x75, 0xa0, 0x01, 'x',
        0x00, 0x53, 0x76, 0x45,
    };
    try testing.expectError(error.Malformed, Message.decode(gpa, &data_then_sequence));

    const two_values = [_]u8{
        0x00, 0x53, 0x77, 0x52, 0x01,
        0x00, 0x53, 0x77, 0x52, 0x02,
    };
    try testing.expectError(error.Malformed, Message.decode(gpa, &two_values));

    const value_then_data = [_]u8{
        0x00, 0x53, 0x77, 0x52, 0x01,
        0x00, 0x53, 0x75, 0xa0, 0x01,
        'x',
    };
    try testing.expectError(error.Malformed, Message.decode(gpa, &value_then_data));
}

test "the decoder rejects a section body of the wrong type" {
    const gpa = testing.allocator;
    // A data section must hold binary, and this one holds a string.
    const bytes = [_]u8{ 0x00, 0x53, 0x75, 0xa1, 0x01, 'x' };
    try testing.expectError(error.Malformed, Message.decode(gpa, &bytes));
}

test "the decoder rejects a descriptor that names no section" {
    const gpa = testing.allocator;
    const bytes = [_]u8{ 0x00, 0x53, 0x10, 0x45 };
    try testing.expectError(error.Malformed, Message.decode(gpa, &bytes));
}

test "the decoder rejects a value that is not a described section" {
    const gpa = testing.allocator;
    const bytes = [_]u8{ 0x52, 0x01 };
    try testing.expectError(error.Malformed, Message.decode(gpa, &bytes));
}

test "a header section tolerates a longer list and a shorter list" {
    const gpa = testing.allocator;

    // Seven fields, and this module knows five of them.
    const longer = [_]u8{
        0x00, 0x53, 0x70, 0xc0, 0x09, 0x07, 0x41, 0x50,
        0x04, 0x40, 0x42, 0x43, 0x41, 0x41,
    };
    const from_longer = try Message.decode(gpa, &longer);
    defer from_longer.deinit();
    try testing.expectEqualDeep(Header{
        .durable = true,
        .priority = 4,
        .ttl = null,
        .first_acquirer = false,
        .delivery_count = 0,
    }, from_longer.value.header.?);

    // One field only, so the other four stay null.
    const shorter = [_]u8{ 0x00, 0x53, 0x70, 0xc0, 0x02, 0x01, 0x41 };
    const from_shorter = try Message.decode(gpa, &shorter);
    defer from_shorter.deinit();
    try testing.expectEqualDeep(Header{ .durable = true }, from_shorter.value.header.?);
}

test "a message round trips through a copy of the input" {
    const gpa = testing.allocator;

    const message: Message = .{
        .header = .{ .durable = false, .priority = 4 },
        .message_annotations = message_entries,
        .properties = .{ .to = .{ .string = "node" }, .creation_time = 1 },
        .application_properties = application_entries,
        .body = .{ .data = &.{ "one", "two", "three" } },
        .footer = footer_entries,
    };

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try message.encode(&sink.writer);
    try testing.expectEqual(sink.written().len, try message.encodedSize());

    const bytes = try gpa.dupe(u8, sink.written());
    const decoded = try Message.decode(gpa, bytes);
    defer decoded.deinit();
    gpa.free(bytes);

    try testing.expectEqualDeep(message, decoded.value);
}

fn decodeAndFree(gpa: Allocator, bytes: []const u8) !void {
    const decoded = try Message.decode(gpa, bytes);
    decoded.deinit();
}

test "a decode that runs out of memory frees what it allocated" {
    // The input holds a composite section, a map section, and two data
    // sections, so it reaches every allocation of the decode path.
    const bytes = [_]u8{
        0x00, 0x53, 0x70, 0xc0, 0x02, 0x01, 0x41,
        0x00, 0x53, 0x72, 0xc1, 0x06, 0x02, 0xa3,
        0x01, 'm',  0x52, 0x02, 0x00, 0x53, 0x75,
        0xa0, 0x01, 'x',  0x00, 0x53, 0x75, 0xa0,
        0x01, 'y',
    };
    try testing.checkAllAllocationFailures(testing.allocator, decodeAndFree, .{&bytes});
}
