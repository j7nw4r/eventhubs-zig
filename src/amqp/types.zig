//! The AMQP 1.0 primitive type model.
//!
//! This file holds `Value`, the tagged union that carries one AMQP 1.0
//! primitive value. It also holds `Constructor`, the set of constructor codes
//! from section 1.6 of the AMQP 1.0 type specification.
//!
//! Specification: OASIS AMQP Version 1.0 Part 1: Types, section 1.6.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html
//!
//! # Ownership
//!
//! The decoder allocates, and the caller frees. `codec.decode` returns a
//! `Value` that owns every slice and every pointer below it. The caller frees
//! the whole tree with one call to `Value.deinit`, and gives `deinit` the same
//! allocator that the decode call used.
//!
//! A `Value` that you build yourself owns nothing. Do not call `deinit` on it.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// The constructor byte that introduces a described type. The descriptor and
/// the described value follow it, in that order. Specification section 1.5.
pub const described_constructor: u8 = 0x00;

/// A primitive constructor code from section 1.6 of the AMQP 1.0 type
/// specification. One AMQP type can have more than one constructor. For
/// example, a uint has the `uint0`, `smalluint`, and `uint` codes.
///
/// The decoder accepts every code below. The encoder writes the shortest code
/// that holds the value, so it never writes the `boolean` code 0x56 for a
/// value on its own. `Array.element` reaches the other codes, because the
/// caller names the element constructor of an array.
///
/// This library does not support the decimal32, decimal64, and decimal128
/// types, so their codes are absent. The decoder reports them as
/// `error.Malformed`.
pub const Constructor = enum(u8) {
    /// The null value. Width 0.
    null = 0x40,
    /// The boolean value true. Width 0.
    boolean_true = 0x41,
    /// The boolean value false. Width 0.
    boolean_false = 0x42,
    /// The uint value 0. Width 0.
    uint0 = 0x43,
    /// The ulong value 0. Width 0.
    ulong0 = 0x44,
    /// The empty list. Width 0.
    list0 = 0x45,
    /// An 8-bit unsigned integer. Width 1.
    ubyte = 0x50,
    /// An 8-bit two's-complement integer. Width 1.
    byte = 0x51,
    /// A uint in the range 0 to 255. Width 1.
    smalluint = 0x52,
    /// A ulong in the range 0 to 255. Width 1.
    smallulong = 0x53,
    /// An int that fits in 8 bits, two's complement. Width 1.
    smallint = 0x54,
    /// A long that fits in 8 bits, two's complement. Width 1.
    smalllong = 0x55,
    /// A boolean. The octet 0x00 is false and the octet 0x01 is true. Width 1.
    boolean = 0x56,
    /// A 16-bit unsigned integer in network byte order. Width 2.
    ushort = 0x60,
    /// A 16-bit two's-complement integer in network byte order. Width 2.
    short = 0x61,
    /// A 32-bit unsigned integer in network byte order. Width 4.
    uint = 0x70,
    /// A 32-bit two's-complement integer in network byte order. Width 4.
    int = 0x71,
    /// An IEEE 754-2008 binary32 value. Width 4.
    float = 0x72,
    /// A UTF-32BE encoded Unicode character. Width 4.
    char = 0x73,
    /// A 64-bit unsigned integer in network byte order. Width 8.
    ulong = 0x80,
    /// A 64-bit two's-complement integer in network byte order. Width 8.
    long = 0x81,
    /// An IEEE 754-2008 binary64 value. Width 8.
    double = 0x82,
    /// Milliseconds after the Unix epoch, as a 64-bit two's-complement
    /// integer. Width 8.
    timestamp = 0x83,
    /// A UUID, as in section 4.1.2 of RFC 4122. Width 16.
    uuid = 0x98,
    /// Up to 255 octets of binary data. The size field is 1 octet.
    vbin8 = 0xa0,
    /// Up to 255 octets of UTF-8 text. The size field is 1 octet.
    str8 = 0xa1,
    /// Up to 255 octets of 7-bit ASCII text. The size field is 1 octet.
    sym8 = 0xa3,
    /// Up to 2^32 - 1 octets of binary data. The size field is 4 octets.
    vbin32 = 0xb0,
    /// Up to 2^32 - 1 octets of UTF-8 text. The size field is 4 octets.
    str32 = 0xb1,
    /// Up to 2^32 - 1 octets of 7-bit ASCII text. The size field is 4 octets.
    sym32 = 0xb3,
    /// A list. The size field and the count field are 1 octet each.
    list8 = 0xc0,
    /// A map. The size field and the count field are 1 octet each.
    map8 = 0xc1,
    /// A list. The size field and the count field are 4 octets each.
    list32 = 0xd0,
    /// A map. The size field and the count field are 4 octets each.
    map32 = 0xd1,
    /// An array. The size field and the count field are 1 octet each.
    array8 = 0xe0,
    /// An array. The size field and the count field are 4 octets each.
    array32 = 0xf0,
};

/// One entry of an AMQP map. AMQP keeps the entries in the order that the
/// encoding gives, and the key can be a value of any type.
pub const MapEntry = struct {
    /// The key of the entry.
    key: Value,
    /// The value of the entry.
    value: Value,
};

/// An AMQP array. Every element of an array shares one constructor, so the
/// encoding writes that constructor one time and then writes the element
/// bodies.
pub const Array = struct {
    /// The constructor that every element body uses.
    element: Constructor,
    /// The descriptor that every element shares, or `null` when the element
    /// constructor is a plain constructor. AMQP allows a described element
    /// constructor for an array. Specification section 1.2.4.
    descriptor: ?*const Value = null,
    /// The element bodies, in order. Each element must agree with `element`.
    items: []const Value = &.{},
};

/// A described type: a descriptor and the value that the descriptor annotates.
/// Specification section 1.1.2.
pub const Described = struct {
    /// The descriptor. AMQP reserves every type other than symbol and ulong.
    descriptor: *const Value,
    /// The value that the descriptor annotates.
    value: *const Value,
};

/// One AMQP 1.0 value. The tag names follow the type names of specification
/// section 1.6.
///
/// The decoder allocates the slices and the pointers below this union. Read
/// the ownership rule at the top of this file before you free a value.
pub const Value = union(enum) {
    /// The null value.
    null,
    /// A boolean.
    boolean: bool,
    /// An 8-bit unsigned integer.
    ubyte: u8,
    /// A 16-bit unsigned integer.
    ushort: u16,
    /// A 32-bit unsigned integer.
    uint: u32,
    /// A 64-bit unsigned integer.
    ulong: u64,
    /// An 8-bit signed integer.
    byte: i8,
    /// A 16-bit signed integer.
    short: i16,
    /// A 32-bit signed integer.
    int: i32,
    /// A 64-bit signed integer.
    long: i64,
    /// An IEEE 754-2008 binary32 value.
    float: f32,
    /// An IEEE 754-2008 binary64 value.
    double: f64,
    /// A single Unicode code point. A surrogate is not a legal value.
    char: u21,
    /// Milliseconds after the Unix epoch. This type is separate from `long`,
    /// because it has its own constructor code.
    timestamp: i64,
    /// A UUID, in the byte order that RFC 4122 section 4.1.2 gives.
    uuid: [16]u8,
    /// Binary data of any content.
    binary: []const u8,
    /// UTF-8 text. The codec rejects text that is not valid UTF-8.
    string: []const u8,
    /// A symbolic value. AMQP limits a symbol to 7-bit ASCII, and the codec
    /// rejects a byte above 0x7f.
    symbol: []const u8,
    /// A list. The elements can have different types.
    list: []const Value,
    /// A map. The entries stay in the order of the encoding.
    map: []const MapEntry,
    /// An array. Every element shares one constructor.
    array: Array,
    /// A described type.
    described: Described,

    /// Frees a value that the decoder produced, and frees every value below
    /// it. Give the same allocator that the decode call used.
    ///
    /// Do not call this function on a value that you built yourself, because
    /// such a value owns no memory.
    pub fn deinit(self: Value, gpa: Allocator) void {
        switch (self) {
            .binary, .string, .symbol => |slice| gpa.free(slice),
            .list => |items| {
                for (items) |item| item.deinit(gpa);
                gpa.free(items);
            },
            .map => |entries| {
                for (entries) |entry| {
                    entry.key.deinit(gpa);
                    entry.value.deinit(gpa);
                }
                gpa.free(entries);
            },
            .array => |array| {
                if (array.descriptor) |descriptor| {
                    descriptor.deinit(gpa);
                    gpa.destroy(descriptor);
                }
                for (array.items) |item| item.deinit(gpa);
                gpa.free(array.items);
            },
            .described => |described| {
                described.descriptor.deinit(gpa);
                gpa.destroy(described.descriptor);
                described.value.deinit(gpa);
                gpa.destroy(described.value);
            },
            .null,
            .boolean,
            .ubyte,
            .ushort,
            .uint,
            .ulong,
            .byte,
            .short,
            .int,
            .long,
            .float,
            .double,
            .char,
            .timestamp,
            .uuid,
            => {},
        }
    }
};

test "deinit frees nothing for a scalar value" {
    const value: Value = .{ .uint = 7 };
    value.deinit(std.testing.allocator);
}

test "deinit frees an allocated tree" {
    const gpa = std.testing.allocator;

    const items = try gpa.alloc(Value, 2);
    items[0] = .{ .string = try gpa.dupe(u8, "one") };
    items[1] = .{ .symbol = try gpa.dupe(u8, "two") };

    const descriptor = try gpa.create(Value);
    descriptor.* = .{ .ulong = 3 };
    const inner = try gpa.create(Value);
    inner.* = .{ .list = items };

    const value: Value = .{ .described = .{ .descriptor = descriptor, .value = inner } };
    value.deinit(gpa);
}
