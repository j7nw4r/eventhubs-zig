//! The AMQP 1.0 performatives and the described types that they carry.
//!
//! Every type in this file is a composite type: the encoding is a described
//! value whose descriptor is a ulong and whose body is a list of the fields in
//! the order that the specification gives.
//!
//! Specifications:
//! OASIS AMQP Version 1.0 Part 2: Transport, sections 2.7 and 2.8.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html
//! OASIS AMQP Version 1.0 Part 3: Messaging, sections 3.4 and 3.5.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html
//! OASIS AMQP Version 1.0 Part 5: Security, section 5.3.3.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html
//!
//! # Every field is optional
//!
//! Each field of each type is an optional, and `null` means that the field is
//! absent from the encoding. The specification marks some fields mandatory, and
//! the doc comment of each such field says so.
//!
//! The decoder must accept a list that is shorter than the field count, so a
//! mandatory field must be able to hold `null`. This module therefore applies
//! no mandatory rule, on the encoder or on the decoder. Set the mandatory
//! fields before you send.
//!
//! The rule belongs to the layer above. A peer that omits a mandatory field
//! commits a protocol error, and the answer is an AMQP error with the correct
//! condition, which this module cannot select.
//!
//! A field that is absent takes the default value that the specification gives
//! for it. The doc comments give the defaults. This module does not apply a
//! default, so a decoded `null` stays `null`.
//!
//! # Version skew
//!
//! The encoder omits the trailing fields that are null, so a short list goes on
//! the wire. The decoder accepts a list of any length: it ignores the fields
//! after the ones that it knows, and it sets the fields that the list does not
//! reach to null. Section 1.3 of Part 1 requires both directions.
//!
//! # Ownership
//!
//! A performative that you build yourself owns nothing, so you free nothing.
//! `decode` allocates, and it returns a `Decoded` wrapper that owns every byte
//! below it. Free the whole result with one call to `Decoded.deinit`.
//!
//! # Two field names differ from the specification
//!
//! The specification names one field `error` and one field `resume`. Both words
//! are Zig keywords, so this module names them `error_condition` and `resumed`.

const std = @import("std");
const codec = @import("codec.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Constructor = types.Constructor;
const MapEntry = types.MapEntry;
const Value = types.Value;
const Writer = std.Io.Writer;

const max_u32: usize = std.math.maxInt(u32);

/// The errors that the encode path returns. They are the errors of the
/// primitive codec.
pub const EncodeError = codec.EncodeError;

/// The errors that the decode path returns. They are the errors of the
/// primitive codec.
pub const DecodeError = codec.DecodeError;

// -------------------------------------------------------------------------
// The field types
// -------------------------------------------------------------------------

/// An AMQP symbol field. The type is separate from a string field, because the
/// two have different encodings and a Zig slice cannot tell them apart.
pub const Symbol = struct {
    /// The text of the symbol. AMQP limits a symbol to 7-bit ASCII.
    text: []const u8,

    /// Returns a symbol that holds `text`.
    pub fn of(text: []const u8) Symbol {
        return .{ .text = text };
    }
};

/// An AMQP binary field. The type is separate from a string field for the same
/// reason as `Symbol`.
pub const Binary = struct {
    /// The bytes of the field.
    bytes: []const u8,

    /// Returns a binary field that holds `bytes`.
    pub fn of(bytes: []const u8) Binary {
        return .{ .bytes = bytes };
    }
};

/// The role of a link endpoint. Specification section 2.8.1. The encoding is a
/// boolean, and `false` is the sender.
pub const Role = enum(u1) {
    sender = 0,
    receiver = 1,
};

/// The settlement policy of a sender. Specification section 2.8.2.
pub const SenderSettleMode = enum(u8) {
    unsettled = 0,
    settled = 1,
    mixed = 2,
};

/// The settlement policy of a receiver. Specification section 2.8.3.
pub const ReceiverSettleMode = enum(u8) {
    first = 0,
    second = 1,
};

/// The durability policy of a terminus. Specification section 3.5.5.
pub const TerminusDurability = enum(u32) {
    none = 0,
    configuration = 1,
    unsettled_state = 2,
};

/// The expiry policy of a terminus. Specification section 3.5.6. The encoding
/// is a symbol, and `symbolText` gives the text of each policy.
pub const TerminusExpiryPolicy = enum {
    link_detach,
    session_end,
    connection_close,
    never,

    /// Returns the symbol text that the specification gives for the policy.
    pub fn symbolText(self: TerminusExpiryPolicy) []const u8 {
        return switch (self) {
            .link_detach => "link-detach",
            .session_end => "session-end",
            .connection_close => "connection-close",
            .never => "never",
        };
    }

    /// Returns the policy that `text` names, or null when no policy has that
    /// text.
    pub fn fromSymbol(text: []const u8) ?TerminusExpiryPolicy {
        inline for (@typeInfo(TerminusExpiryPolicy).@"enum".fields) |field| {
            const policy: TerminusExpiryPolicy = @enumFromInt(field.value);
            if (std.mem.eql(u8, text, policy.symbolText())) return policy;
        }
        return null;
    }
};

/// The outcome of the SASL dialog. Specification section 5.3.3.6.
pub const SaslCode = enum(u8) {
    ok = 0,
    auth = 1,
    sys = 2,
    sys_perm = 3,
    sys_temp = 4,
};

// -------------------------------------------------------------------------
// The composite engine
// -------------------------------------------------------------------------

/// A decoded composite and the memory that holds it.
///
/// The slices of `value` point into an arena that this wrapper owns. Free the
/// arena, and thus the whole result, with `deinit`.
pub fn Decoded(comptime T: type) type {
    return struct {
        const Self = @This();

        /// The decoded value. Its slices live in the arena below.
        value: T,
        /// The arena that holds every byte of `value`.
        arena_state: std.heap.ArenaAllocator.State,
        /// The allocator that the arena takes its memory from.
        child: Allocator,

        /// Frees the arena, and thus every slice of `value`.
        pub fn deinit(self: Self) void {
            self.arena_state.promote(self.child).deinit();
        }
    };
}

/// Makes sure that `T` can act as an AMQP composite. Every field must be an
/// optional, so that the decoder can report an absent field, and so that a
/// short list can leave the fields that it does not reach null.
fn checkComposite(comptime T: type) void {
    comptime {
        const info = @typeInfo(T).@"struct";
        if (info.fields.len > 255) {
            @compileError("an AMQP composite of this module holds at most 255 fields");
        }
        for (info.fields) |field| {
            if (@typeInfo(field.type) != .optional) {
                @compileError("the field " ++ @typeName(T) ++ "." ++ field.name ++
                    " must be an optional");
            }
        }
    }
}

/// Returns true when `T` is one of the composite types of this module.
fn isComposite(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct" => @hasDecl(T, "descriptor_code"),
        else => false,
    };
}

/// Returns the number of fields that the encoding holds. The encoder omits
/// every trailing field that is null.
fn presentCount(comptime T: type, value: T) usize {
    var count: usize = 0;
    inline for (@typeInfo(T).@"struct".fields, 0..) |field, index| {
        if (@field(value, field.name) != null) count = index + 1;
    }
    return count;
}

/// The shape of the field list that the encoder writes.
const ListShape = struct {
    /// The number of fields in the list.
    count: usize,
    /// The number of bytes that the field encodings take.
    total: usize,
    /// True when the list uses the list8 constructor.
    small: bool,
};

fn listShape(comptime T: type, value: T) EncodeError!ListShape {
    const count = presentCount(T, value);
    var total: usize = 0;
    inline for (@typeInfo(T).@"struct".fields, 0..) |field, index| {
        if (index < count) total += try fieldSize(field.type, @field(value, field.name));
    }
    // The size field of the small form holds the count field and the items, so
    // the items take at most 254 octets.
    const small = total <= 254;
    if (!small and total > max_u32 - 4) return error.InvalidValue;
    return .{ .count = count, .total = total, .small = small };
}

/// Returns the number of bytes that `writeComposite` writes for `value`.
pub fn compositeSize(comptime T: type, value: T) EncodeError!usize {
    comptime checkComposite(T);
    const descriptor = try codec.encodedSize(.{ .ulong = T.descriptor_code });
    const shape = try listShape(T, value);
    if (shape.count == 0) return 1 + descriptor + 1;
    if (shape.small) return 1 + descriptor + 3 + shape.total;
    return 1 + descriptor + 9 + shape.total;
}

/// Writes the described list encoding of `value` to `w`.
///
/// The function writes the descriptor as a ulong, and then writes the fields in
/// the order of the specification. It omits every trailing field that is null.
pub fn writeComposite(comptime T: type, value: T, w: *Writer) EncodeError!void {
    comptime checkComposite(T);
    try w.writeByte(types.described_constructor);
    try codec.encode(.{ .ulong = T.descriptor_code }, w);

    const shape = try listShape(T, value);
    if (shape.count == 0) {
        try w.writeByte(@intFromEnum(Constructor.list0));
        return;
    }
    if (shape.small) {
        try w.writeByte(@intFromEnum(Constructor.list8));
        try w.writeByte(@intCast(1 + shape.total));
        try w.writeByte(@intCast(shape.count));
    } else {
        try w.writeByte(@intFromEnum(Constructor.list32));
        try w.writeInt(u32, @intCast(4 + shape.total), .big);
        try w.writeInt(u32, @intCast(shape.count), .big);
    }
    inline for (@typeInfo(T).@"struct".fields, 0..) |field, index| {
        if (index < shape.count) try writeField(field.type, @field(value, field.name), w);
    }
}

/// Returns the number of bytes that one field takes.
fn fieldSize(comptime F: type, field: F) EncodeError!usize {
    const C = @typeInfo(F).optional.child;
    const inner = field orelse return 1;
    if (C == []const Symbol) return symbolsSize(inner);
    if (C == DeliveryState) return inner.encodedSize();
    if (comptime isComposite(C)) return compositeSize(C, inner);
    return codec.encodedSize(simpleValue(C, inner));
}

/// Writes one field.
fn writeField(comptime F: type, field: F, w: *Writer) EncodeError!void {
    const C = @typeInfo(F).optional.child;
    const inner = field orelse return w.writeByte(@intFromEnum(Constructor.null));
    if (C == []const Symbol) return writeSymbols(inner, w);
    if (C == DeliveryState) return inner.encode(w);
    if (comptime isComposite(C)) return writeComposite(C, inner, w);
    return codec.encode(simpleValue(C, inner), w);
}

/// Returns the primitive value of a field whose type maps to one primitive.
fn simpleValue(comptime C: type, inner: C) Value {
    if (C == []const u8) return .{ .string = inner };
    if (C == Symbol) return .{ .symbol = inner.text };
    if (C == Binary) return .{ .binary = inner.bytes };
    if (C == []const MapEntry) return .{ .map = inner };
    if (C == Value) return inner;
    if (C == bool) return .{ .boolean = inner };
    if (C == u8) return .{ .ubyte = inner };
    if (C == u16) return .{ .ushort = inner };
    if (C == u32) return .{ .uint = inner };
    if (C == u64) return .{ .ulong = inner };
    if (C == i64) return .{ .timestamp = inner };
    if (C == Role) return .{ .boolean = inner == .receiver };
    if (@typeInfo(C) == .@"enum") {
        if (@hasDecl(C, "symbolText")) return .{ .symbol = inner.symbolText() };
        const Tag = @typeInfo(C).@"enum".tag_type;
        if (Tag == u8) return .{ .ubyte = @intFromEnum(inner) };
        if (Tag == u32) return .{ .uint = @intFromEnum(inner) };
        @compileError("no AMQP encoding for the enum " ++ @typeName(C));
    }
    @compileError("no AMQP encoding for the type " ++ @typeName(C));
}

/// Reads one composite from `bytes`.
///
/// The result owns its memory. Free it with `Decoded.deinit`.
pub fn decodeComposite(comptime T: type, gpa: Allocator, bytes: []const u8) DecodeError!Decoded(T) {
    var arena: std.heap.ArenaAllocator = .init(gpa);
    errdefer arena.deinit();
    const tree = try codec.decode(arena.allocator(), bytes);
    const value = try compositeFromValue(T, tree, arena.allocator());
    return .{ .value = value, .arena_state = arena.state, .child = gpa };
}

/// Builds a composite from a decoded value.
///
/// The result borrows every slice from `value`, so `value` must outlive it.
/// `arena` holds the slices that the multiple fields need.
pub fn compositeFromValue(comptime T: type, value: Value, arena: Allocator) DecodeError!T {
    comptime checkComposite(T);
    if (value != .described) return error.Malformed;
    if (!descriptorMatches(T, value.described.descriptor.*)) return error.Malformed;
    return fieldsFromValue(T, value.described.value.*, arena);
}

/// Returns true when `descriptor` names the type `T`. AMQP allows the numeric
/// descriptor and the symbolic descriptor, so this function accepts both.
fn descriptorMatches(comptime T: type, descriptor: Value) bool {
    return switch (descriptor) {
        .ulong => |code| code == T.descriptor_code,
        .symbol => |name| std.mem.eql(u8, name, T.descriptor_name),
        else => false,
    };
}

/// Builds a composite from the list that holds its fields.
///
/// The list can hold more fields than `T` knows, and the extra fields go away.
/// It can also hold fewer, and then the fields that it does not reach stay
/// null.
fn fieldsFromValue(comptime T: type, body: Value, arena: Allocator) DecodeError!T {
    const items: []const Value = switch (body) {
        .list => |list| list,
        else => return error.Malformed,
    };
    var result: T = .{};
    inline for (@typeInfo(T).@"struct".fields, 0..) |field, index| {
        if (index < items.len) {
            @field(result, field.name) = try fieldFromValue(field.type, items[index], arena);
        }
    }
    return result;
}

fn fieldFromValue(comptime F: type, value: Value, arena: Allocator) DecodeError!F {
    const C = @typeInfo(F).optional.child;
    if (value == .null) return null;
    if (C == []const u8) return switch (value) {
        .string => |text| text,
        else => error.Malformed,
    };
    if (C == Symbol) return switch (value) {
        .symbol => |text| Symbol{ .text = text },
        else => error.Malformed,
    };
    if (C == Binary) return switch (value) {
        .binary => |data| Binary{ .bytes = data },
        else => error.Malformed,
    };
    if (C == []const Symbol) return try symbolsFromValue(value, arena);
    if (C == []const MapEntry) return switch (value) {
        .map => |entries| entries,
        else => error.Malformed,
    };
    if (C == Value) return value;
    if (C == bool) return switch (value) {
        .boolean => |flag| flag,
        else => error.Malformed,
    };
    if (C == u8) return switch (value) {
        .ubyte => |number| number,
        else => error.Malformed,
    };
    if (C == u16) return switch (value) {
        .ushort => |number| number,
        else => error.Malformed,
    };
    if (C == u32) return switch (value) {
        .uint => |number| number,
        else => error.Malformed,
    };
    if (C == u64) return switch (value) {
        .ulong => |number| number,
        else => error.Malformed,
    };
    if (C == i64) return switch (value) {
        .timestamp => |number| number,
        else => error.Malformed,
    };
    if (C == DeliveryState) return try DeliveryState.fromValue(value, arena);
    if (@typeInfo(C) == .@"enum") return try enumFromValue(C, value);
    if (comptime isComposite(C)) return try compositeFromValue(C, value, arena);
    @compileError("no AMQP encoding for the type " ++ @typeName(C));
}

fn enumFromValue(comptime C: type, value: Value) DecodeError!C {
    if (C == Role) return switch (value) {
        .boolean => |flag| if (flag) .receiver else .sender,
        else => error.Malformed,
    };
    if (@hasDecl(C, "symbolText")) return switch (value) {
        .symbol => |text| C.fromSymbol(text) orelse error.Malformed,
        else => error.Malformed,
    };
    const Tag = @typeInfo(C).@"enum".tag_type;
    if (Tag == u8) return switch (value) {
        .ubyte => |number| std.enums.fromInt(C, number) orelse error.Malformed,
        else => error.Malformed,
    };
    if (Tag == u32) return switch (value) {
        .uint => |number| std.enums.fromInt(C, number) orelse error.Malformed,
        else => error.Malformed,
    };
    @compileError("no AMQP encoding for the enum " ++ @typeName(C));
}

// -------------------------------------------------------------------------
// The multiple fields
// -------------------------------------------------------------------------
//
// Section 1.3 of Part 1 lets a field that the specification marks
// `multiple="true"` hold null, one value, or an array of values. This module
// holds such a field as a slice of symbols. It writes one symbol for a slice of
// one, and an array for every other length. It reads a symbol, an array, or a
// list.

/// The shape of the array that a multiple field writes.
const SymbolArray = struct {
    /// The constructor of the elements. Every element of an array shares one
    /// constructor, so one long symbol makes the whole array use sym32.
    element: Constructor,
    /// The number of bytes that the element constructor and the element bodies
    /// take.
    payload: usize,
    /// True when the array uses the array8 constructor.
    small: bool,
};

fn symbolArrayShape(items: []const Symbol) EncodeError!SymbolArray {
    var element: Constructor = .sym8;
    for (items) |item| {
        for (item.text) |byte| {
            if (byte > 0x7f) return error.InvalidValue;
        }
        if (item.text.len > max_u32) return error.InvalidValue;
        if (item.text.len > 255) element = .sym32;
    }

    const width: usize = if (element == .sym8) 1 else 4;
    var payload: usize = 1;
    for (items) |item| payload += width + item.text.len;

    const small = items.len <= 255 and payload <= 254;
    if (!small and (items.len > max_u32 or payload > max_u32 - 4)) return error.InvalidValue;
    return .{ .element = element, .payload = payload, .small = small };
}

fn symbolsSize(items: []const Symbol) EncodeError!usize {
    if (items.len == 1) return codec.encodedSize(.{ .symbol = items[0].text });
    const shape = try symbolArrayShape(items);
    return if (shape.small) 3 + shape.payload else 9 + shape.payload;
}

fn writeSymbols(items: []const Symbol, w: *Writer) EncodeError!void {
    if (items.len == 1) return codec.encode(.{ .symbol = items[0].text }, w);

    const shape = try symbolArrayShape(items);
    if (shape.small) {
        try w.writeByte(@intFromEnum(Constructor.array8));
        try w.writeByte(@intCast(1 + shape.payload));
        try w.writeByte(@intCast(items.len));
    } else {
        try w.writeByte(@intFromEnum(Constructor.array32));
        try w.writeInt(u32, @intCast(4 + shape.payload), .big);
        try w.writeInt(u32, @intCast(items.len), .big);
    }
    try w.writeByte(@intFromEnum(shape.element));
    for (items) |item| {
        if (shape.element == .sym8) {
            try w.writeByte(@intCast(item.text.len));
        } else {
            try w.writeInt(u32, @intCast(item.text.len), .big);
        }
        try w.writeAll(item.text);
    }
}

fn symbolsFromValue(value: Value, arena: Allocator) DecodeError![]const Symbol {
    switch (value) {
        .symbol => |text| {
            const items = try arena.alloc(Symbol, 1);
            items[0] = .{ .text = text };
            return items;
        },
        // A peer can send a list where the specification gives an array. The
        // two carry the same information, so the decoder takes both.
        .array, .list => {
            const source: []const Value = switch (value) {
                .array => |array| array.items,
                .list => |list| list,
                else => unreachable,
            };
            const items = try arena.alloc(Symbol, source.len);
            for (source, items) |element, *slot| {
                slot.* = switch (element) {
                    .symbol => |text| .{ .text = text },
                    else => return error.Malformed,
                };
            }
            return items;
        },
        else => return error.Malformed,
    }
}

// -------------------------------------------------------------------------
// The transport performatives
// -------------------------------------------------------------------------

/// Negotiate connection parameters. Specification section 2.7.1.
pub const Open = struct {
    /// The numeric descriptor of the type.
    pub const descriptor_code: u64 = 0x0000_0000_0000_0010;
    /// The symbolic descriptor of the type.
    pub const descriptor_name = "amqp:open:list";

    /// The id of the source container. Mandatory.
    container_id: ?[]const u8 = null,
    /// The name of the target host.
    hostname: ?[]const u8 = null,
    /// The largest frame size that the peer can accept. Default 4294967295.
    max_frame_size: ?u32 = null,
    /// The largest channel number that the peer can accept. Default 65535.
    channel_max: ?u16 = null,
    /// The idle timeout in milliseconds.
    idle_time_out: ?u32 = null,
    /// The locales that are available for outgoing text.
    outgoing_locales: ?[]const Symbol = null,
    /// The locales that the peer wants for incoming text.
    incoming_locales: ?[]const Symbol = null,
    /// The extension capabilities that the sender supports.
    offered_capabilities: ?[]const Symbol = null,
    /// The extension capabilities that the sender wants.
    desired_capabilities: ?[]const Symbol = null,
    /// The properties of the connection.
    properties: ?[]const MapEntry = null,

    pub fn encode(self: Open, w: *Writer) EncodeError!void {
        return writeComposite(Open, self, w);
    }

    pub fn encodedSize(self: Open) EncodeError!usize {
        return compositeSize(Open, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Open) {
        return decodeComposite(Open, gpa, bytes);
    }
};

/// Begin a session on a channel. Specification section 2.7.2.
pub const Begin = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0011;
    pub const descriptor_name = "amqp:begin:list";

    /// The channel of the remote session, when this frame answers one.
    remote_channel: ?u16 = null,
    /// The transfer id of the first transfer that the sender sends. Mandatory.
    next_outgoing_id: ?u32 = null,
    /// The initial incoming window of the sender. Mandatory.
    incoming_window: ?u32 = null,
    /// The initial outgoing window of the sender. Mandatory.
    outgoing_window: ?u32 = null,
    /// The largest handle that the peer can accept. Default 4294967295.
    handle_max: ?u32 = null,
    /// The extension capabilities that the sender supports.
    offered_capabilities: ?[]const Symbol = null,
    /// The extension capabilities that the sender wants.
    desired_capabilities: ?[]const Symbol = null,
    /// The properties of the session.
    properties: ?[]const MapEntry = null,

    pub fn encode(self: Begin, w: *Writer) EncodeError!void {
        return writeComposite(Begin, self, w);
    }

    pub fn encodedSize(self: Begin) EncodeError!usize {
        return compositeSize(Begin, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Begin) {
        return decodeComposite(Begin, gpa, bytes);
    }
};

/// Attach a link to a session. Specification section 2.7.3.
pub const Attach = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0012;
    pub const descriptor_name = "amqp:attach:list";

    /// The name of the link. Mandatory.
    name: ?[]const u8 = null,
    /// The handle of the link. Mandatory.
    handle: ?u32 = null,
    /// The role of the endpoint that sends this frame. Mandatory.
    role: ?Role = null,
    /// The settlement policy of the sender. Default `mixed`.
    snd_settle_mode: ?SenderSettleMode = null,
    /// The settlement policy of the receiver. Default `first`.
    rcv_settle_mode: ?ReceiverSettleMode = null,
    /// The source of the messages.
    source: ?Source = null,
    /// The target of the messages.
    target: ?Target = null,
    /// The unsettled deliveries of the link, as a map from delivery tag to
    /// delivery state.
    unsettled: ?[]const MapEntry = null,
    /// True when the `unsettled` map holds part of the unsettled deliveries
    /// only. Default false.
    incomplete_unsettled: ?bool = null,
    /// The first delivery count of a sending endpoint.
    initial_delivery_count: ?u32 = null,
    /// The largest message size that the link accepts.
    max_message_size: ?u64 = null,
    /// The extension capabilities that the sender supports.
    offered_capabilities: ?[]const Symbol = null,
    /// The extension capabilities that the sender wants.
    desired_capabilities: ?[]const Symbol = null,
    /// The properties of the link.
    properties: ?[]const MapEntry = null,

    pub fn encode(self: Attach, w: *Writer) EncodeError!void {
        return writeComposite(Attach, self, w);
    }

    pub fn encodedSize(self: Attach) EncodeError!usize {
        return compositeSize(Attach, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Attach) {
        return decodeComposite(Attach, gpa, bytes);
    }
};

/// Update the link state. Specification section 2.7.4.
pub const Flow = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0013;
    pub const descriptor_name = "amqp:flow:list";

    /// The transfer id that the sender expects next.
    next_incoming_id: ?u32 = null,
    /// The incoming window of the sender. Mandatory.
    incoming_window: ?u32 = null,
    /// The transfer id that the sender assigns next. Mandatory.
    next_outgoing_id: ?u32 = null,
    /// The outgoing window of the sender. Mandatory.
    outgoing_window: ?u32 = null,
    /// The handle of the link that this frame reports.
    handle: ?u32 = null,
    /// The delivery count of the link endpoint.
    delivery_count: ?u32 = null,
    /// The credit of the link.
    link_credit: ?u32 = null,
    /// The number of messages that wait at the sender.
    available: ?u32 = null,
    /// True when the receiver drains the credit that it does not use. Default
    /// false.
    drain: ?bool = null,
    /// True when the sender asks for a flow frame in answer. Default false.
    echo: ?bool = null,
    /// The properties of the frame.
    properties: ?[]const MapEntry = null,

    pub fn encode(self: Flow, w: *Writer) EncodeError!void {
        return writeComposite(Flow, self, w);
    }

    pub fn encodedSize(self: Flow) EncodeError!usize {
        return compositeSize(Flow, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Flow) {
        return decodeComposite(Flow, gpa, bytes);
    }
};

/// Transfer a message. Specification section 2.7.5.
pub const Transfer = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0014;
    pub const descriptor_name = "amqp:transfer:list";

    /// The handle of the link that carries the transfer. Mandatory.
    handle: ?u32 = null,
    /// The delivery id of the transfer.
    delivery_id: ?u32 = null,
    /// The delivery tag of the transfer. It holds up to 32 octets.
    delivery_tag: ?Binary = null,
    /// The message format code.
    message_format: ?u32 = null,
    /// True when the sender settles the delivery itself.
    settled: ?bool = null,
    /// True when more frames of the same delivery follow. Default false.
    more: ?bool = null,
    /// The settlement policy that the receiver applies to this delivery.
    rcv_settle_mode: ?ReceiverSettleMode = null,
    /// The state of the delivery at the sender.
    state: ?DeliveryState = null,
    /// True when the transfer resumes a delivery of an earlier link. Default
    /// false. The specification names this field `resume`, and that word is a
    /// Zig keyword.
    resumed: ?bool = null,
    /// True when the sender aborts the delivery. Default false.
    aborted: ?bool = null,
    /// True when the sender hints that the peer can batch its answer. Default
    /// false.
    batchable: ?bool = null,

    pub fn encode(self: Transfer, w: *Writer) EncodeError!void {
        return writeComposite(Transfer, self, w);
    }

    pub fn encodedSize(self: Transfer) EncodeError!usize {
        return compositeSize(Transfer, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Transfer) {
        return decodeComposite(Transfer, gpa, bytes);
    }
};

/// Report a change of delivery state. Specification section 2.7.6.
pub const Disposition = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0015;
    pub const descriptor_name = "amqp:disposition:list";

    /// The role of the endpoint that sends this frame. Mandatory.
    role: ?Role = null,
    /// The lowest delivery id of the set. Mandatory.
    first: ?u32 = null,
    /// The highest delivery id of the set. It takes the value of `first` when
    /// it is absent.
    last: ?u32 = null,
    /// True when the sender settles the deliveries of the set. Default false.
    settled: ?bool = null,
    /// The state of every delivery of the set.
    state: ?DeliveryState = null,
    /// True when the sender hints that the peer can batch its answer. Default
    /// false.
    batchable: ?bool = null,

    pub fn encode(self: Disposition, w: *Writer) EncodeError!void {
        return writeComposite(Disposition, self, w);
    }

    pub fn encodedSize(self: Disposition) EncodeError!usize {
        return compositeSize(Disposition, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Disposition) {
        return decodeComposite(Disposition, gpa, bytes);
    }
};

/// Detach a link endpoint from a session. Specification section 2.7.7.
pub const Detach = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0016;
    pub const descriptor_name = "amqp:detach:list";

    /// The handle of the link. Mandatory.
    handle: ?u32 = null,
    /// True when the sender closes the link. Default false.
    closed: ?bool = null,
    /// The error that caused the detach. The specification names this field
    /// `error`, and that word is a Zig keyword.
    error_condition: ?ErrorCondition = null,

    pub fn encode(self: Detach, w: *Writer) EncodeError!void {
        return writeComposite(Detach, self, w);
    }

    pub fn encodedSize(self: Detach) EncodeError!usize {
        return compositeSize(Detach, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Detach) {
        return decodeComposite(Detach, gpa, bytes);
    }
};

/// End a session. Specification section 2.7.8.
pub const End = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0017;
    pub const descriptor_name = "amqp:end:list";

    /// The error that caused the end. The specification names this field
    /// `error`, and that word is a Zig keyword.
    error_condition: ?ErrorCondition = null,

    pub fn encode(self: End, w: *Writer) EncodeError!void {
        return writeComposite(End, self, w);
    }

    pub fn encodedSize(self: End) EncodeError!usize {
        return compositeSize(End, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(End) {
        return decodeComposite(End, gpa, bytes);
    }
};

/// Close a connection. Specification section 2.7.9.
pub const Close = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0018;
    pub const descriptor_name = "amqp:close:list";

    /// The error that caused the close. The specification names this field
    /// `error`, and that word is a Zig keyword.
    error_condition: ?ErrorCondition = null,

    pub fn encode(self: Close, w: *Writer) EncodeError!void {
        return writeComposite(Close, self, w);
    }

    pub fn encodedSize(self: Close) EncodeError!usize {
        return compositeSize(Close, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Close) {
        return decodeComposite(Close, gpa, bytes);
    }
};

/// The details of an error. Specification section 2.8.14. The specification
/// names this type `error`, and that word is a Zig keyword.
pub const ErrorCondition = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_001d;
    pub const descriptor_name = "amqp:error:list";

    /// The symbol that names the error condition. Mandatory.
    condition: ?Symbol = null,
    /// The text that describes the error condition.
    description: ?[]const u8 = null,
    /// The map that carries more information about the error condition.
    info: ?[]const MapEntry = null,

    pub fn encode(self: ErrorCondition, w: *Writer) EncodeError!void {
        return writeComposite(ErrorCondition, self, w);
    }

    pub fn encodedSize(self: ErrorCondition) EncodeError!usize {
        return compositeSize(ErrorCondition, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(ErrorCondition) {
        return decodeComposite(ErrorCondition, gpa, bytes);
    }
};

// -------------------------------------------------------------------------
// The messaging types
// -------------------------------------------------------------------------

/// The source of the messages of a link. Specification section 3.5.3.
pub const Source = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0028;
    pub const descriptor_name = "amqp:source:list";

    /// The address of the source node. The specification gives it the type
    /// `*`, so it holds any value, and a string is the usual one.
    address: ?Value = null,
    /// The durability of the terminus. Default `none`.
    durable: ?TerminusDurability = null,
    /// The expiry policy of the terminus. Default `session_end`.
    expiry_policy: ?TerminusExpiryPolicy = null,
    /// The expiry timeout in seconds. Default 0.
    timeout: ?u32 = null,
    /// True when the peer creates a node for this terminus. Default false.
    dynamic: ?bool = null,
    /// The properties of the node that the peer creates.
    dynamic_node_properties: ?[]const MapEntry = null,
    /// The distribution mode of the link.
    distribution_mode: ?Symbol = null,
    /// The filters of the source. The caller supplies every filter name and
    /// every filter value.
    filter: ?[]const MapEntry = null,
    /// The outcome that the source applies to an unsettled delivery.
    default_outcome: ?DeliveryState = null,
    /// The outcomes that the source accepts.
    outcomes: ?[]const Symbol = null,
    /// The extension capabilities of the source.
    capabilities: ?[]const Symbol = null,

    pub fn encode(self: Source, w: *Writer) EncodeError!void {
        return writeComposite(Source, self, w);
    }

    pub fn encodedSize(self: Source) EncodeError!usize {
        return compositeSize(Source, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Source) {
        return decodeComposite(Source, gpa, bytes);
    }
};

/// The target of the messages of a link. Specification section 3.5.4.
pub const Target = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0029;
    pub const descriptor_name = "amqp:target:list";

    /// The address of the target node. The specification gives it the type
    /// `*`, so it holds any value, and a string is the usual one.
    address: ?Value = null,
    /// The durability of the terminus. Default `none`.
    durable: ?TerminusDurability = null,
    /// The expiry policy of the terminus. Default `session_end`.
    expiry_policy: ?TerminusExpiryPolicy = null,
    /// The expiry timeout in seconds. Default 0.
    timeout: ?u32 = null,
    /// True when the peer creates a node for this terminus. Default false.
    dynamic: ?bool = null,
    /// The properties of the node that the peer creates.
    dynamic_node_properties: ?[]const MapEntry = null,
    /// The extension capabilities of the target.
    capabilities: ?[]const Symbol = null,

    pub fn encode(self: Target, w: *Writer) EncodeError!void {
        return writeComposite(Target, self, w);
    }

    pub fn encodedSize(self: Target) EncodeError!usize {
        return compositeSize(Target, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Target) {
        return decodeComposite(Target, gpa, bytes);
    }
};

/// The state of a delivery that the receiver holds in part. Specification
/// section 3.4.1.
pub const Received = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0023;
    pub const descriptor_name = "amqp:received:list";

    /// The number of the section that the receiver holds first. Mandatory.
    section_number: ?u32 = null,
    /// The offset in that section. Mandatory.
    section_offset: ?u64 = null,

    pub fn encode(self: Received, w: *Writer) EncodeError!void {
        return writeComposite(Received, self, w);
    }

    pub fn encodedSize(self: Received) EncodeError!usize {
        return compositeSize(Received, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Received) {
        return decodeComposite(Received, gpa, bytes);
    }
};

/// The accepted outcome. Specification section 3.4.2. It holds no field.
pub const Accepted = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0024;
    pub const descriptor_name = "amqp:accepted:list";

    pub fn encode(self: Accepted, w: *Writer) EncodeError!void {
        return writeComposite(Accepted, self, w);
    }

    pub fn encodedSize(self: Accepted) EncodeError!usize {
        return compositeSize(Accepted, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Accepted) {
        return decodeComposite(Accepted, gpa, bytes);
    }
};

/// The rejected outcome. Specification section 3.4.3.
pub const Rejected = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0025;
    pub const descriptor_name = "amqp:rejected:list";

    /// The error that caused the rejection. The specification names this field
    /// `error`, and that word is a Zig keyword.
    error_condition: ?ErrorCondition = null,

    pub fn encode(self: Rejected, w: *Writer) EncodeError!void {
        return writeComposite(Rejected, self, w);
    }

    pub fn encodedSize(self: Rejected) EncodeError!usize {
        return compositeSize(Rejected, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Rejected) {
        return decodeComposite(Rejected, gpa, bytes);
    }
};

/// The released outcome. Specification section 3.4.4. It holds no field.
pub const Released = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0026;
    pub const descriptor_name = "amqp:released:list";

    pub fn encode(self: Released, w: *Writer) EncodeError!void {
        return writeComposite(Released, self, w);
    }

    pub fn encodedSize(self: Released) EncodeError!usize {
        return compositeSize(Released, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Released) {
        return decodeComposite(Released, gpa, bytes);
    }
};

/// The modified outcome. Specification section 3.4.5.
pub const Modified = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0027;
    pub const descriptor_name = "amqp:modified:list";

    /// True when the delivery attempt failed.
    delivery_failed: ?bool = null,
    /// True when the receiver cannot take the message again.
    undeliverable_here: ?bool = null,
    /// The annotations that the sender adds to the message.
    message_annotations: ?[]const MapEntry = null,

    pub fn encode(self: Modified, w: *Writer) EncodeError!void {
        return writeComposite(Modified, self, w);
    }

    pub fn encodedSize(self: Modified) EncodeError!usize {
        return compositeSize(Modified, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(Modified) {
        return decodeComposite(Modified, gpa, bytes);
    }
};

/// The state of a delivery. Specification section 3.4.
///
/// The `received` member is a state and not an outcome, so it cannot stand as
/// the default outcome of a source. This module does not apply that rule.
pub const DeliveryState = union(enum) {
    received: Received,
    accepted: Accepted,
    rejected: Rejected,
    released: Released,
    modified: Modified,

    pub fn encode(self: DeliveryState, w: *Writer) EncodeError!void {
        switch (self) {
            inline else => |body| return writeComposite(@TypeOf(body), body, w),
        }
    }

    pub fn encodedSize(self: DeliveryState) EncodeError!usize {
        switch (self) {
            inline else => |body| return compositeSize(@TypeOf(body), body),
        }
    }

    /// Builds a delivery state from a decoded value. The result borrows every
    /// slice from `value`.
    pub fn fromValue(value: Value, arena: Allocator) DecodeError!DeliveryState {
        if (value != .described) return error.Malformed;
        inline for (@typeInfo(DeliveryState).@"union".fields) |field| {
            if (descriptorMatches(field.type, value.described.descriptor.*)) {
                return @unionInit(
                    DeliveryState,
                    field.name,
                    try fieldsFromValue(field.type, value.described.value.*, arena),
                );
            }
        }
        return error.Malformed;
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(DeliveryState) {
        var arena: std.heap.ArenaAllocator = .init(gpa);
        errdefer arena.deinit();
        const tree = try codec.decode(arena.allocator(), bytes);
        const value = try fromValue(tree, arena.allocator());
        return .{ .value = value, .arena_state = arena.state, .child = gpa };
    }
};

// -------------------------------------------------------------------------
// The security frame bodies
// -------------------------------------------------------------------------
//
// The SASL dialog also has a challenge body and a response body. A client that
// speaks a one-step mechanism never sends or reads them, so this module leaves
// them out.

/// Advertise the SASL mechanisms of the server. Specification section 5.3.3.1.
pub const SaslMechanisms = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0040;
    pub const descriptor_name = "amqp:sasl-mechanisms:list";

    /// The mechanisms that the server supports. Mandatory.
    sasl_server_mechanisms: ?[]const Symbol = null,

    pub fn encode(self: SaslMechanisms, w: *Writer) EncodeError!void {
        return writeComposite(SaslMechanisms, self, w);
    }

    pub fn encodedSize(self: SaslMechanisms) EncodeError!usize {
        return compositeSize(SaslMechanisms, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(SaslMechanisms) {
        return decodeComposite(SaslMechanisms, gpa, bytes);
    }
};

/// Start the SASL dialog. Specification section 5.3.3.2.
pub const SaslInit = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0041;
    pub const descriptor_name = "amqp:sasl-init:list";

    /// The mechanism that the client selects. Mandatory.
    mechanism: ?Symbol = null,
    /// The initial response of the mechanism.
    initial_response: ?Binary = null,
    /// The name of the target host.
    hostname: ?[]const u8 = null,

    pub fn encode(self: SaslInit, w: *Writer) EncodeError!void {
        return writeComposite(SaslInit, self, w);
    }

    pub fn encodedSize(self: SaslInit) EncodeError!usize {
        return compositeSize(SaslInit, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(SaslInit) {
        return decodeComposite(SaslInit, gpa, bytes);
    }
};

/// Report the outcome of the SASL dialog. Specification section 5.3.3.5.
pub const SaslOutcome = struct {
    pub const descriptor_code: u64 = 0x0000_0000_0000_0044;
    pub const descriptor_name = "amqp:sasl-outcome:list";

    /// The outcome code. Mandatory.
    code: ?SaslCode = null,
    /// The data that the mechanism adds to the outcome.
    additional_data: ?Binary = null,

    pub fn encode(self: SaslOutcome, w: *Writer) EncodeError!void {
        return writeComposite(SaslOutcome, self, w);
    }

    pub fn encodedSize(self: SaslOutcome) EncodeError!usize {
        return compositeSize(SaslOutcome, self);
    }

    pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Decoded(SaslOutcome) {
        return decodeComposite(SaslOutcome, gpa, bytes);
    }
};

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

const testing = std.testing;

/// Checks both directions against one golden byte vector.
fn expectGolden(comptime T: type, value: T, bytes: []const u8) !void {
    const gpa = testing.allocator;

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try value.encode(&sink.writer);
    try testing.expectEqualSlices(u8, bytes, sink.written());
    try testing.expectEqual(bytes.len, try value.encodedSize());

    const decoded = try T.decode(gpa, bytes);
    defer decoded.deinit();
    try testing.expectEqualDeep(value, decoded.value);
}

/// Encodes `value`, reads the bytes back with the primitive decoder, and
/// compares the field list with `expected`.
///
/// The test states the field order on its own, so it catches a field that sits
/// in the wrong position. A round trip of the typed encoder and the typed
/// decoder cannot catch that, because both would hold the same mistake.
fn expectFieldOrder(comptime T: type, value: T, expected: []const Value) !void {
    const gpa = testing.allocator;

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try value.encode(&sink.writer);

    const tree = try codec.decode(gpa, sink.written());
    defer tree.deinit(gpa);

    try testing.expect(tree == .described);
    try testing.expectEqualDeep(Value{ .ulong = T.descriptor_code }, tree.described.descriptor.*);
    try testing.expect(tree.described.value.* == .list);
    try testing.expectEqualDeep(expected, tree.described.value.list);
}

test "golden vector: open" {
    try expectGolden(Open, .{ .container_id = "a" }, &.{
        0x00, 0x53, 0x10, 0xc0, 0x04, 0x01, 0xa1, 0x01, 'a',
    });
}

test "golden vector: begin" {
    try expectGolden(Begin, .{
        .next_outgoing_id = 0,
        .incoming_window = 1,
        .outgoing_window = 2,
    }, &.{
        0x00, 0x53, 0x11, 0xc0, 0x07, 0x04, 0x40, 0x43, 0x52, 0x01, 0x52, 0x02,
    });
}

test "golden vector: attach" {
    try expectGolden(Attach, .{
        .name = "l",
        .handle = 0,
        .role = .sender,
    }, &.{
        0x00, 0x53, 0x12, 0xc0, 0x06, 0x03, 0xa1, 0x01, 'l', 0x43, 0x42,
    });
}

test "golden vector: flow" {
    try expectGolden(Flow, .{
        .incoming_window = 1,
        .next_outgoing_id = 2,
        .outgoing_window = 3,
    }, &.{
        0x00, 0x53, 0x13, 0xc0, 0x08, 0x04, 0x40, 0x52, 0x01, 0x52, 0x02, 0x52, 0x03,
    });
}

test "golden vector: transfer" {
    try expectGolden(Transfer, .{
        .handle = 0,
        .delivery_id = 1,
        .delivery_tag = .of("\xaa"),
        .message_format = 0,
        .settled = true,
    }, &.{
        0x00, 0x53, 0x14, 0xc0, 0x09, 0x05, 0x43, 0x52,
        0x01, 0xa0, 0x01, 0xaa, 0x43, 0x41,
    });
}

test "golden vector: disposition" {
    try expectGolden(Disposition, .{
        .role = .receiver,
        .first = 1,
        .last = 1,
        .settled = true,
        .state = .{ .accepted = .{} },
    }, &.{
        0x00, 0x53, 0x15, 0xc0, 0x0b, 0x05, 0x41, 0x52,
        0x01, 0x52, 0x01, 0x41, 0x00, 0x53, 0x24, 0x45,
    });
}

test "golden vector: detach" {
    try expectGolden(Detach, .{ .handle = 0, .closed = true }, &.{
        0x00, 0x53, 0x16, 0xc0, 0x03, 0x02, 0x43, 0x41,
    });
}

test "golden vector: end" {
    try expectGolden(End, .{
        .error_condition = .{ .condition = .of("e") },
    }, &.{
        0x00, 0x53, 0x17, 0xc0, 0x0a, 0x01, 0x00, 0x53,
        0x1d, 0xc0, 0x04, 0x01, 0xa3, 0x01, 'e',
    });
}

test "golden vector: close" {
    try expectGolden(Close, .{}, &.{ 0x00, 0x53, 0x18, 0x45 });
}

test "golden vector: error" {
    try expectGolden(ErrorCondition, .{
        .condition = .of("e"),
        .description = "d",
    }, &.{
        0x00, 0x53, 0x1d, 0xc0, 0x07, 0x02, 0xa3, 0x01, 'e', 0xa1, 0x01, 'd',
    });
}

test "golden vector: source" {
    try expectGolden(Source, .{
        .address = .{ .string = "q" },
        .durable = .none,
        .expiry_policy = .session_end,
    }, &.{
        0x00, 0x53, 0x28, 0xc0, 0x12, 0x03, 0xa1, 0x01, 'q', 0x43, 0xa3, 0x0b,
        's',  'e',  's',  's',  'i',  'o',  'n',  '-',  'e', 'n',  'd',
    });
}

test "golden vector: target" {
    try expectGolden(Target, .{
        .address = .{ .string = "q" },
        .capabilities = &.{.of("c")},
    }, &.{
        0x00, 0x53, 0x29, 0xc0, 0x0c, 0x07, 0xa1, 0x01, 'q',
        0x40, 0x40, 0x40, 0x40, 0x40, 0xa3, 0x01, 'c',
    });
}

test "golden vector: received" {
    try expectGolden(Received, .{ .section_number = 1, .section_offset = 2 }, &.{
        0x00, 0x53, 0x23, 0xc0, 0x05, 0x02, 0x52, 0x01, 0x53, 0x02,
    });
}

test "golden vector: accepted" {
    try expectGolden(Accepted, .{}, &.{ 0x00, 0x53, 0x24, 0x45 });
}

test "golden vector: rejected" {
    try expectGolden(Rejected, .{
        .error_condition = .{ .condition = .of("e") },
    }, &.{
        0x00, 0x53, 0x25, 0xc0, 0x0a, 0x01, 0x00, 0x53,
        0x1d, 0xc0, 0x04, 0x01, 0xa3, 0x01, 'e',
    });
}

test "golden vector: released" {
    try expectGolden(Released, .{}, &.{ 0x00, 0x53, 0x26, 0x45 });
}

test "golden vector: modified" {
    try expectGolden(Modified, .{
        .delivery_failed = true,
        .undeliverable_here = false,
    }, &.{
        0x00, 0x53, 0x27, 0xc0, 0x03, 0x02, 0x41, 0x42,
    });
}

test "golden vector: sasl-mechanisms" {
    try expectGolden(SaslMechanisms, .{
        .sasl_server_mechanisms = &.{ .of("PLAIN"), .of("ANONYMOUS") },
    }, &.{
        0x00, 0x53, 0x40, 0xc0, 0x15, 0x01, 0xe0, 0x12, 0x02, 0xa3, 0x05, 'P', 'L',
        'A',  'I',  'N',  0x09, 'A',  'N',  'O',  'N',  'Y',  'M',  'O',  'U', 'S',
    });
}

test "golden vector: sasl-init" {
    try expectGolden(SaslInit, .{
        .mechanism = .of("PLAIN"),
        .initial_response = .of("\x00u\x00p"),
    }, &.{
        0x00, 0x53, 0x41, 0xc0, 0x0e, 0x02, 0xa3, 0x05, 'P', 'L',
        'A',  'I',  'N',  0xa0, 0x04, 0x00, 'u',  0x00, 'p',
    });
}

test "golden vector: sasl-outcome" {
    try expectGolden(SaslOutcome, .{ .code = .ok }, &.{
        0x00, 0x53, 0x44, 0xc0, 0x03, 0x01, 0x50, 0x00,
    });
}

test "the field order of open follows the specification" {
    try expectFieldOrder(Open, .{
        .container_id = "c",
        .hostname = "h",
        .max_frame_size = 1,
        .channel_max = 2,
        .idle_time_out = 3,
        .outgoing_locales = &.{.of("en-US")},
        .incoming_locales = &.{.of("en-GB")},
        .offered_capabilities = &.{.of("o")},
        .desired_capabilities = &.{.of("d")},
        .properties = &.{.{ .key = .{ .symbol = "k" }, .value = .{ .uint = 4 } }},
    }, &.{
        .{ .string = "c" },
        .{ .string = "h" },
        .{ .uint = 1 },
        .{ .ushort = 2 },
        .{ .uint = 3 },
        .{ .symbol = "en-US" },
        .{ .symbol = "en-GB" },
        .{ .symbol = "o" },
        .{ .symbol = "d" },
        .{ .map = &.{.{ .key = .{ .symbol = "k" }, .value = .{ .uint = 4 } }} },
    });
}

test "the field order of begin follows the specification" {
    try expectFieldOrder(Begin, .{
        .remote_channel = 1,
        .next_outgoing_id = 2,
        .incoming_window = 3,
        .outgoing_window = 4,
        .handle_max = 5,
        .offered_capabilities = &.{.of("o")},
        .desired_capabilities = &.{.of("d")},
        .properties = &.{},
    }, &.{
        .{ .ushort = 1 },
        .{ .uint = 2 },
        .{ .uint = 3 },
        .{ .uint = 4 },
        .{ .uint = 5 },
        .{ .symbol = "o" },
        .{ .symbol = "d" },
        .{ .map = &.{} },
    });
}

test "the field order of attach follows the specification" {
    try expectFieldOrder(Attach, .{
        .name = "n",
        .handle = 1,
        .role = .receiver,
        .snd_settle_mode = .mixed,
        .rcv_settle_mode = .second,
        .source = .{ .address = .{ .string = "s" } },
        .target = .{ .address = .{ .string = "t" } },
        .unsettled = &.{},
        .incomplete_unsettled = true,
        .initial_delivery_count = 2,
        .max_message_size = 3,
        .offered_capabilities = &.{.of("o")},
        .desired_capabilities = &.{.of("d")},
        .properties = &.{},
    }, &.{
        .{ .string = "n" },
        .{ .uint = 1 },
        .{ .boolean = true },
        .{ .ubyte = 2 },
        .{ .ubyte = 1 },
        .{ .described = .{
            .descriptor = &.{ .ulong = Source.descriptor_code },
            .value = &.{ .list = &.{.{ .string = "s" }} },
        } },
        .{ .described = .{
            .descriptor = &.{ .ulong = Target.descriptor_code },
            .value = &.{ .list = &.{.{ .string = "t" }} },
        } },
        .{ .map = &.{} },
        .{ .boolean = true },
        .{ .uint = 2 },
        .{ .ulong = 3 },
        .{ .symbol = "o" },
        .{ .symbol = "d" },
        .{ .map = &.{} },
    });
}

test "the field order of flow follows the specification" {
    try expectFieldOrder(Flow, .{
        .next_incoming_id = 1,
        .incoming_window = 2,
        .next_outgoing_id = 3,
        .outgoing_window = 4,
        .handle = 5,
        .delivery_count = 6,
        .link_credit = 7,
        .available = 8,
        .drain = true,
        .echo = false,
        .properties = &.{},
    }, &.{
        .{ .uint = 1 },
        .{ .uint = 2 },
        .{ .uint = 3 },
        .{ .uint = 4 },
        .{ .uint = 5 },
        .{ .uint = 6 },
        .{ .uint = 7 },
        .{ .uint = 8 },
        .{ .boolean = true },
        .{ .boolean = false },
        .{ .map = &.{} },
    });
}

test "the field order of transfer follows the specification" {
    try expectFieldOrder(Transfer, .{
        .handle = 1,
        .delivery_id = 2,
        .delivery_tag = .of("t"),
        .message_format = 3,
        .settled = true,
        .more = false,
        .rcv_settle_mode = .first,
        .state = .{ .released = .{} },
        .resumed = false,
        .aborted = false,
        .batchable = true,
    }, &.{
        .{ .uint = 1 },
        .{ .uint = 2 },
        .{ .binary = "t" },
        .{ .uint = 3 },
        .{ .boolean = true },
        .{ .boolean = false },
        .{ .ubyte = 0 },
        .{ .described = .{
            .descriptor = &.{ .ulong = Released.descriptor_code },
            .value = &.{ .list = &.{} },
        } },
        .{ .boolean = false },
        .{ .boolean = false },
        .{ .boolean = true },
    });
}

test "the field order of disposition follows the specification" {
    try expectFieldOrder(Disposition, .{
        .role = .sender,
        .first = 1,
        .last = 2,
        .settled = true,
        .state = .{ .accepted = .{} },
        .batchable = false,
    }, &.{
        .{ .boolean = false },
        .{ .uint = 1 },
        .{ .uint = 2 },
        .{ .boolean = true },
        .{ .described = .{
            .descriptor = &.{ .ulong = Accepted.descriptor_code },
            .value = &.{ .list = &.{} },
        } },
        .{ .boolean = false },
    });
}

test "the field order of detach and of error follows the specification" {
    try expectFieldOrder(Detach, .{
        .handle = 1,
        .closed = true,
        .error_condition = .{
            .condition = .of("amqp:not-found"),
            .description = "gone",
            .info = &.{},
        },
    }, &.{
        .{ .uint = 1 },
        .{ .boolean = true },
        .{ .described = .{
            .descriptor = &.{ .ulong = ErrorCondition.descriptor_code },
            .value = &.{ .list = &.{
                .{ .symbol = "amqp:not-found" },
                .{ .string = "gone" },
                .{ .map = &.{} },
            } },
        } },
    });
}

test "the field order of source follows the specification" {
    try expectFieldOrder(Source, .{
        .address = .{ .string = "a" },
        .durable = .unsettled_state,
        .expiry_policy = .never,
        .timeout = 1,
        .dynamic = true,
        .dynamic_node_properties = &.{},
        .distribution_mode = .of("move"),
        .filter = &.{},
        .default_outcome = .{ .modified = .{ .delivery_failed = true } },
        .outcomes = &.{.of("amqp:accepted:list")},
        .capabilities = &.{.of("c")},
    }, &.{
        .{ .string = "a" },
        .{ .uint = 2 },
        .{ .symbol = "never" },
        .{ .uint = 1 },
        .{ .boolean = true },
        .{ .map = &.{} },
        .{ .symbol = "move" },
        .{ .map = &.{} },
        .{ .described = .{
            .descriptor = &.{ .ulong = Modified.descriptor_code },
            .value = &.{ .list = &.{.{ .boolean = true }} },
        } },
        .{ .symbol = "amqp:accepted:list" },
        .{ .symbol = "c" },
    });
}

test "the field order of target follows the specification" {
    try expectFieldOrder(Target, .{
        .address = .{ .string = "a" },
        .durable = .configuration,
        .expiry_policy = .link_detach,
        .timeout = 1,
        .dynamic = false,
        .dynamic_node_properties = &.{},
        .capabilities = &.{.of("c")},
    }, &.{
        .{ .string = "a" },
        .{ .uint = 1 },
        .{ .symbol = "link-detach" },
        .{ .uint = 1 },
        .{ .boolean = false },
        .{ .map = &.{} },
        .{ .symbol = "c" },
    });
}

test "the field order of modified follows the specification" {
    try expectFieldOrder(Modified, .{
        .delivery_failed = false,
        .undeliverable_here = true,
        .message_annotations = &.{},
    }, &.{
        .{ .boolean = false },
        .{ .boolean = true },
        .{ .map = &.{} },
    });
}

test "the field order of the sasl bodies follows the specification" {
    try expectFieldOrder(SaslInit, .{
        .mechanism = .of("PLAIN"),
        .initial_response = .of("r"),
        .hostname = "h",
    }, &.{
        .{ .symbol = "PLAIN" },
        .{ .binary = "r" },
        .{ .string = "h" },
    });
    try expectFieldOrder(SaslOutcome, .{
        .code = .sys_temp,
        .additional_data = .of("d"),
    }, &.{
        .{ .ubyte = 4 },
        .{ .binary = "d" },
    });
    try expectFieldOrder(SaslMechanisms, .{
        .sasl_server_mechanisms = &.{.of("PLAIN")},
    }, &.{
        .{ .symbol = "PLAIN" },
    });
}

test "the encoder omits the optional trailing fields that are null" {
    const gpa = testing.allocator;

    // Open holds ten fields, and this value sets the first one only. The
    // encoding must hold a list of one.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    const open: Open = .{ .container_id = "c" };
    try open.encode(&sink.writer);
    try testing.expectEqualSlices(u8, &.{
        0x00, 0x53, 0x10, 0xc0, 0x04, 0x01, 0xa1, 0x01, 'c',
    }, sink.written());

    // A null field that sits before a field that is set stays in the list.
    var second: Writer.Allocating = .init(gpa);
    defer second.deinit();
    const flow: Flow = .{ .incoming_window = 1 };
    try flow.encode(&second.writer);
    try testing.expectEqualSlices(u8, &.{
        0x00, 0x53, 0x13, 0xc0, 0x04, 0x02, 0x40, 0x52, 0x01,
    }, second.written());

    // A value with no field at all writes the empty list.
    var third: Writer.Allocating = .init(gpa);
    defer third.deinit();
    const close: Close = .{};
    try close.encode(&third.writer);
    try testing.expectEqualSlices(u8, &.{ 0x00, 0x53, 0x18, 0x45 }, third.written());
}

test "the decoder ignores the extra trailing fields of a longer list" {
    const gpa = testing.allocator;

    // A detach of five fields. This module knows three of them. The two extra
    // fields hold a string and a boolean, and the decoder must drop both.
    const bytes = [_]u8{
        0x00, 0x53, 0x16, 0xc0, 0x09, 0x05, 0x52, 0x07,
        0x41, 0x40, 0xa1, 0x01, 'x',  0x41,
    };
    const decoded = try Detach.decode(gpa, &bytes);
    defer decoded.deinit();
    try testing.expectEqualDeep(Detach{
        .handle = 7,
        .closed = true,
        .error_condition = null,
    }, decoded.value);
}

test "the decoder sets the missing fields of a shorter list to null" {
    const gpa = testing.allocator;

    // An open of two fields. The other eight fields must become null.
    const bytes = [_]u8{
        0x00, 0x53, 0x10, 0xc0, 0x07, 0x02, 0xa1, 0x01, 'c', 0xa1, 0x01, 'h',
    };
    const decoded = try Open.decode(gpa, &bytes);
    defer decoded.deinit();
    try testing.expectEqualDeep(Open{
        .container_id = "c",
        .hostname = "h",
    }, decoded.value);

    // The empty list leaves every field null.
    const empty = [_]u8{ 0x00, 0x53, 0x10, 0x45 };
    const none = try Open.decode(gpa, &empty);
    defer none.deinit();
    try testing.expectEqualDeep(Open{}, none.value);
}

test "the decoder takes a symbolic descriptor" {
    const gpa = testing.allocator;

    const bytes = [_]u8{
        0x00, 0xa3, 0x0f, 'a', 'm', 'q', 'p', ':', 'c',  'l',
        'o',  's',  'e',  ':', 'l', 'i', 's', 't', 0x45,
    };
    const decoded = try Close.decode(gpa, &bytes);
    defer decoded.deinit();
    try testing.expectEqualDeep(Close{}, decoded.value);
}

test "the decoder rejects a descriptor of another type" {
    const bytes = [_]u8{ 0x00, 0x53, 0x11, 0x45 };
    try testing.expectError(error.Malformed, Open.decode(testing.allocator, &bytes));
}

test "the decoder rejects a body that is not a list" {
    const bytes = [_]u8{ 0x00, 0x53, 0x10, 0xa1, 0x01, 'c' };
    try testing.expectError(error.Malformed, Open.decode(testing.allocator, &bytes));
}

test "the decoder rejects a field of the wrong type" {
    // The container id must be a string, and this list holds a uint.
    const bytes = [_]u8{ 0x00, 0x53, 0x10, 0xc0, 0x03, 0x01, 0x52, 0x01 };
    try testing.expectError(error.Malformed, Open.decode(testing.allocator, &bytes));
}

test "the decoder rejects a restricted value that no choice names" {
    // 3 is not a sender settle mode.
    const bytes = [_]u8{
        0x00, 0x53, 0x12, 0xc0, 0x06, 0x04, 0x40, 0x40, 0x40, 0x50, 0x03,
    };
    try testing.expectError(error.Malformed, Attach.decode(testing.allocator, &bytes));
}

test "a multiple field takes one symbol, an array, or a list" {
    const gpa = testing.allocator;

    const one = [_]u8{
        0x00, 0x53, 0x40, 0xc0, 0x08, 0x01, 0xa3, 0x05, 'P', 'L', 'A', 'I', 'N',
    };
    const from_symbol = try SaslMechanisms.decode(gpa, &one);
    defer from_symbol.deinit();
    try testing.expectEqualDeep(SaslMechanisms{
        .sasl_server_mechanisms = &.{Symbol.of("PLAIN")},
    }, from_symbol.value);

    const array = [_]u8{
        0x00, 0x53, 0x40, 0xc0, 0x09, 0x01, 0xe0, 0x06,
        0x02, 0xa3, 0x01, 'a',  0x01, 'b',
    };
    const from_array = try SaslMechanisms.decode(gpa, &array);
    defer from_array.deinit();
    try testing.expectEqualDeep(SaslMechanisms{
        .sasl_server_mechanisms = &.{ Symbol.of("a"), Symbol.of("b") },
    }, from_array.value);

    const list = [_]u8{
        0x00, 0x53, 0x40, 0xc0, 0x0a, 0x01, 0xc0, 0x07,
        0x02, 0xa3, 0x01, 'a',  0xa3, 0x01, 'b',
    };
    const from_list = try SaslMechanisms.decode(gpa, &list);
    defer from_list.deinit();
    try testing.expectEqualDeep(SaslMechanisms{
        .sasl_server_mechanisms = &.{ Symbol.of("a"), Symbol.of("b") },
    }, from_list.value);
}

test "an empty multiple field writes an empty array" {
    try expectGolden(SaslMechanisms, .{ .sasl_server_mechanisms = &.{} }, &.{
        0x00, 0x53, 0x40, 0xc0, 0x05, 0x01, 0xe0, 0x02, 0x00, 0xa3,
    });
}

test "a long symbol makes a multiple field use the wide forms" {
    const gpa = testing.allocator;

    const long = "s" ** 300;
    const items = [_]Symbol{ .of("a"), .of(long) };
    const value: Open = .{ .desired_capabilities = &items };

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try value.encode(&sink.writer);
    try testing.expectEqual(sink.written().len, try value.encodedSize());

    // The array holds one long symbol, so every element takes the sym32 form.
    const tree = try codec.decode(gpa, sink.written());
    defer tree.deinit(gpa);
    const items_out = tree.described.value.list;
    try testing.expectEqual(@as(usize, 9), items_out.len);
    try testing.expectEqual(Constructor.sym32, items_out[8].array.element);

    const decoded = try Open.decode(gpa, sink.written());
    defer decoded.deinit();
    try testing.expectEqualDeep(value, decoded.value);
}

test "a delivery state round trips on its own" {
    const gpa = testing.allocator;

    const state: DeliveryState = .{ .rejected = .{
        .error_condition = .{ .condition = .of("amqp:internal-error") },
    } };

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try state.encode(&sink.writer);
    try testing.expectEqual(sink.written().len, try state.encodedSize());

    const decoded = try DeliveryState.decode(gpa, sink.written());
    defer decoded.deinit();
    try testing.expectEqualDeep(state, decoded.value);
}

test "the decoder rejects a delivery state that no type names" {
    // 0x22 is not a delivery state.
    const bytes = [_]u8{ 0x00, 0x53, 0x22, 0x45 };
    try testing.expectError(error.Malformed, DeliveryState.decode(testing.allocator, &bytes));
}

test "a large performative uses the wide list form" {
    const gpa = testing.allocator;

    const text = "d" ** 300;
    const value: Open = .{ .container_id = text };

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try value.encode(&sink.writer);
    try testing.expectEqual(sink.written().len, try value.encodedSize());
    // 0x00, the smallulong descriptor, and then the list32 constructor.
    try testing.expectEqual(@as(u8, 0xd0), sink.written()[3]);

    const decoded = try Open.decode(gpa, sink.written());
    defer decoded.deinit();
    try testing.expectEqualDeep(value, decoded.value);
}

test "the encoder rejects a symbol that is not 7-bit ASCII" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();

    const items = [_]Symbol{ .of("a"), .of("\xff") };
    const value: Open = .{ .offered_capabilities = &items };
    try testing.expectError(error.InvalidValue, value.encode(&sink.writer));
    try testing.expectError(error.InvalidValue, value.encodedSize());
}

test "every performative round trips with every field set" {
    const gpa = testing.allocator;
    const entries: []const MapEntry = &.{
        .{ .key = .{ .symbol = "k" }, .value = .{ .string = "v" } },
    };
    const caps: []const Symbol = &.{ .of("one"), .of("two") };

    const attach: Attach = .{
        .name = "link",
        .handle = 1,
        .role = .receiver,
        .snd_settle_mode = .settled,
        .rcv_settle_mode = .second,
        .source = .{
            .address = .{ .string = "s" },
            .durable = .configuration,
            .expiry_policy = .connection_close,
            .timeout = 9,
            .dynamic = true,
            .dynamic_node_properties = entries,
            .distribution_mode = .of("copy"),
            .filter = entries,
            .default_outcome = .{ .released = .{} },
            .outcomes = caps,
            .capabilities = caps,
        },
        .target = .{
            .address = .{ .string = "t" },
            .durable = .none,
            .expiry_policy = .never,
            .timeout = 0,
            .dynamic = false,
            .dynamic_node_properties = entries,
            .capabilities = caps,
        },
        .unsettled = entries,
        .incomplete_unsettled = true,
        .initial_delivery_count = 3,
        .max_message_size = 1 << 40,
        .offered_capabilities = caps,
        .desired_capabilities = caps,
        .properties = entries,
    };

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try attach.encode(&sink.writer);
    try testing.expectEqual(sink.written().len, try attach.encodedSize());

    const decoded = try Attach.decode(gpa, sink.written());
    defer decoded.deinit();
    try testing.expectEqualDeep(attach, decoded.value);
}

test "the decoded value survives the free of the input" {
    const gpa = testing.allocator;

    const source = [_]u8{
        0x00, 0x53, 0x10, 0xc0, 0x04, 0x01, 0xa1, 0x01, 'c',
    };
    const bytes = try gpa.dupe(u8, &source);
    const decoded = try Open.decode(gpa, bytes);
    defer decoded.deinit();
    gpa.free(bytes);

    try testing.expectEqualStrings("c", decoded.value.container_id.?);
}

fn decodeOpenAndFree(gpa: Allocator, bytes: []const u8) !void {
    const decoded = try Open.decode(gpa, bytes);
    decoded.deinit();
}

fn decodeMechanismsAndFree(gpa: Allocator, bytes: []const u8) !void {
    const decoded = try SaslMechanisms.decode(gpa, bytes);
    decoded.deinit();
}

test "a decode that runs out of memory frees what it allocated" {
    const open = [_]u8{
        0x00, 0x53, 0x10, 0xc0, 0x07, 0x02, 0xa1, 0x01, 'c', 0xa1, 0x01, 'h',
    };
    try testing.checkAllAllocationFailures(testing.allocator, decodeOpenAndFree, .{&open});

    // This input also reaches the allocation that a multiple field makes.
    const mechanisms = [_]u8{
        0x00, 0x53, 0x40, 0xc0, 0x09, 0x01, 0xe0, 0x06,
        0x02, 0xa3, 0x01, 'a',  0x01, 'b',
    };
    try testing.checkAllAllocationFailures(
        testing.allocator,
        decodeMechanismsAndFree,
        .{&mechanisms},
    );
}
