//! The AMQP 1.0 primitive type codec.
//!
//! This file holds the encoder and the decoder for the primitive encodings of
//! section 1.6 of the AMQP 1.0 type specification.
//!
//! Specification: OASIS AMQP Version 1.0 Part 1: Types.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html
//!
//! # The encoder
//!
//! `encode` writes one value to a `std.Io.Writer`. It does not flush the
//! writer, so the caller flushes when the writer needs it. The encoder always
//! writes the shortest constructor that holds the value. For example, it
//! writes the `uint0` code 0x43 for the uint value 0. One result of this rule
//! is that the encoder never writes the 1-octet `boolean` code 0x56 for a
//! value on its own, because the 0-octet `true` and `false` codes are shorter.
//! The decoder still accepts every code.
//!
//! # The decoder
//!
//! `decode` reads one value from a byte slice. It makes sure that every length
//! agrees with the bytes that remain. Thus it never reads outside the slice.
//!
//! The decoder returns `error.Malformed` for a truncated input, for an unknown
//! constructor, and for text that is not valid UTF-8. It applies the other
//! rules that this file documents in the same way.
//!
//! The decoder allocates, and the caller frees the result with `Value.deinit`.
//! One decode also has a budget, so a short input cannot drive a large
//! allocation. Read the note on `elementBudget`.

const std = @import("std");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Writer = std.Io.Writer;
const Array = types.Array;
const Constructor = types.Constructor;
const MapEntry = types.MapEntry;
const Value = types.Value;

const max_u32: usize = std.math.maxInt(u32);

/// The largest nesting depth that the codec accepts. A list inside a list
/// counts as one level, and so does a described value. The codec applies the
/// limit so that a deep input cannot exhaust the stack. The decoder reports a
/// deeper input as `error.Malformed`, and the encoder reports a deeper value
/// as `error.InvalidValue`.
pub const max_depth: usize = 64;

/// The largest element count that the decoder accepts for ONE array whose
/// element constructor has a width of zero, such as an array of null. The
/// encoded size of such an array does not grow with the element count, so a
/// short input can declare a very large count. The decoder rejects a count
/// above this limit with `error.Malformed`.
///
/// This limit bounds one array. It does not bound a whole decode, because an
/// input can hold many such arrays. `elementBudget` bounds the whole decode.
pub const max_zero_width_elements: usize = 1 << 13;

/// The number of values that one decode of `input_len` octets may allocate.
///
/// Every value of a legal encoding costs at least one octet, with one
/// exception: an array whose element constructor has a width of zero holds
/// elements that cost no octets at all. So `input_len` pays for every ordinary
/// value, and `max_zero_width_elements` pays for the zero-width elements of
/// the whole input, not of each array.
///
/// Without this budget a short input reaches a large allocation. An input of
/// 10 kB that holds a thousand arrays, each of 10 octets and each declaring
/// 8192 null elements, allocates about 328 MB.
fn elementBudget(input_len: usize) usize {
    return input_len +| max_zero_width_elements;
}

/// The errors that `encode` and `encodedSize` return.
pub const EncodeError = Writer.Error || error{
    /// The value cannot be encoded. The value nests deeper than `max_depth`,
    /// or a string holds text that is not valid UTF-8, or a symbol holds a
    /// byte above 0x7f, or a char holds a code point that Unicode does not
    /// allow, or an array element does not agree with `Array.element`, or a
    /// length does not fit in the 4-octet size field of the encoding.
    InvalidValue,
};

/// The errors that `decode` and `Decoder.next` return.
pub const DecodeError = Allocator.Error || error{
    /// The bytes are not a legal AMQP 1.0 encoding. The input is truncated, or
    /// it names an unknown constructor, or it holds text that is not valid
    /// UTF-8, or it breaks one of the other rules that this file documents.
    Malformed,
};

// -------------------------------------------------------------------------
// The encoder
// -------------------------------------------------------------------------

/// Writes the AMQP 1.0 encoding of `value` to `w`.
///
/// The function writes the shortest constructor that holds the value. It does
/// not flush `w`.
pub fn encode(value: Value, w: *Writer) EncodeError!void {
    return writeValue(w, value, 0);
}

/// Returns the number of bytes that `encode` writes for `value`.
///
/// The function reports the same errors as `encode`, so a caller can size a
/// buffer before it writes.
pub fn encodedSize(value: Value) EncodeError!usize {
    return valueSize(value, 0);
}

/// The constructor that the encoder selects for a value, and the number of
/// bytes that the whole encoding takes, including the constructor.
const Chosen = struct {
    code: Constructor,
    size: usize,
};

/// Returns the payload of `value` when its tag is `tag`, and returns
/// `error.InvalidValue` when the tag differs.
inline fn payload(
    value: Value,
    comptime tag: std.meta.Tag(Value),
) EncodeError!@FieldType(Value, @tagName(tag)) {
    if (value != tag) return error.InvalidValue;
    return @field(value, @tagName(tag));
}

fn checkString(text: []const u8) EncodeError!void {
    if (!std.unicode.utf8ValidateSlice(text)) return error.InvalidValue;
}

fn checkSymbol(text: []const u8) EncodeError!void {
    for (text) |byte| {
        if (byte > 0x7f) return error.InvalidValue;
    }
}

fn checkChar(code_point: u21) EncodeError!void {
    if (code_point > 0x10ffff) return error.InvalidValue;
    if (code_point >= 0xd800 and code_point <= 0xdfff) return error.InvalidValue;
}

fn chooseVariable(text: []const u8, small: Constructor, large: Constructor) EncodeError!Chosen {
    if (text.len <= 255) return .{ .code = small, .size = 2 + text.len };
    if (text.len > max_u32) return error.InvalidValue;
    return .{ .code = large, .size = 5 + text.len };
}

fn chooseCompound(
    count: usize,
    total: usize,
    small: Constructor,
    large: Constructor,
) EncodeError!Chosen {
    // The small form holds the size and the count in one octet each. The size
    // field counts the count field, so the items take at most 254 octets.
    if (count <= 255 and total <= 254) return .{ .code = small, .size = 3 + total };
    if (count > max_u32 or total > max_u32 - 4) return error.InvalidValue;
    return .{ .code = large, .size = 9 + total };
}

/// Returns the number of bytes that the element constructor of `array` takes.
fn elementConstructorSize(array: Array, depth: usize) EncodeError!usize {
    if (array.descriptor) |descriptor| {
        return 1 + try valueSize(descriptor.*, depth) + 1;
    }
    return 1;
}

/// Selects the constructor for `value` and returns the size of the whole
/// encoding. The caller handles a described value, because a described value
/// has no primitive constructor of its own.
fn choose(value: Value, depth: usize) EncodeError!Chosen {
    switch (value) {
        .null => return .{ .code = .null, .size = 1 },
        .boolean => |flag| return .{
            .code = if (flag) .boolean_true else .boolean_false,
            .size = 1,
        },
        .ubyte => return .{ .code = .ubyte, .size = 2 },
        .ushort => return .{ .code = .ushort, .size = 3 },
        .uint => |number| {
            if (number == 0) return .{ .code = .uint0, .size = 1 };
            if (number <= 255) return .{ .code = .smalluint, .size = 2 };
            return .{ .code = .uint, .size = 5 };
        },
        .ulong => |number| {
            if (number == 0) return .{ .code = .ulong0, .size = 1 };
            if (number <= 255) return .{ .code = .smallulong, .size = 2 };
            return .{ .code = .ulong, .size = 9 };
        },
        .byte => return .{ .code = .byte, .size = 2 },
        .short => return .{ .code = .short, .size = 3 },
        .int => |number| {
            if (number >= -128 and number <= 127) return .{ .code = .smallint, .size = 2 };
            return .{ .code = .int, .size = 5 };
        },
        .long => |number| {
            if (number >= -128 and number <= 127) return .{ .code = .smalllong, .size = 2 };
            return .{ .code = .long, .size = 9 };
        },
        .float => return .{ .code = .float, .size = 5 },
        .double => return .{ .code = .double, .size = 9 },
        .char => |code_point| {
            try checkChar(code_point);
            return .{ .code = .char, .size = 5 };
        },
        .timestamp => return .{ .code = .timestamp, .size = 9 },
        .uuid => return .{ .code = .uuid, .size = 17 },
        .binary => |data| return chooseVariable(data, .vbin8, .vbin32),
        .string => |text| {
            try checkString(text);
            return chooseVariable(text, .str8, .str32);
        },
        .symbol => |text| {
            try checkSymbol(text);
            return chooseVariable(text, .sym8, .sym32);
        },
        .list => |items| {
            if (items.len == 0) return .{ .code = .list0, .size = 1 };
            var total: usize = 0;
            for (items) |item| total += try valueSize(item, depth + 1);
            return chooseCompound(items.len, total, .list8, .list32);
        },
        .map => |entries| {
            if (entries.len > max_u32 / 2) return error.InvalidValue;
            var total: usize = 0;
            for (entries) |entry| {
                total += try valueSize(entry.key, depth + 1);
                total += try valueSize(entry.value, depth + 1);
            }
            return chooseCompound(entries.len * 2, total, .map8, .map32);
        },
        .array => |array| {
            const constructor = try elementConstructorSize(array, depth + 1);
            var total: usize = 0;
            for (array.items) |item| total += try bodySize(item, array.element, depth + 1);
            const body = constructor + total;
            if (array.items.len <= 255 and body <= 254) {
                return .{ .code = .array8, .size = 3 + body };
            }
            if (array.items.len > max_u32 or body > max_u32 - 4) return error.InvalidValue;
            return .{ .code = .array32, .size = 9 + body };
        },
        // `valueSize` and `writeValue` handle a described value before they
        // call this function.
        .described => return error.InvalidValue,
    }
}

/// Returns the number of bytes that the whole encoding of `value` takes.
fn valueSize(value: Value, depth: usize) EncodeError!usize {
    if (depth > max_depth) return error.InvalidValue;
    switch (value) {
        .described => |described| {
            const descriptor = try valueSize(described.descriptor.*, depth + 1);
            const inner = try valueSize(described.value.*, depth + 1);
            return 1 + descriptor + inner;
        },
        else => return (try choose(value, depth)).size,
    }
}

/// Returns the number of bytes that the body of `value` takes under the
/// constructor `code`. The body excludes the constructor itself.
///
/// This function and `writeBody` must agree. The test "the size agrees with
/// the encoder" holds them together.
fn bodySize(value: Value, code: Constructor, depth: usize) EncodeError!usize {
    if (depth > max_depth) return error.InvalidValue;
    switch (code) {
        .null => {
            _ = try payload(value, .null);
            return 0;
        },
        .boolean_true => {
            if (!try payload(value, .boolean)) return error.InvalidValue;
            return 0;
        },
        .boolean_false => {
            if (try payload(value, .boolean)) return error.InvalidValue;
            return 0;
        },
        .uint0 => {
            if (try payload(value, .uint) != 0) return error.InvalidValue;
            return 0;
        },
        .ulong0 => {
            if (try payload(value, .ulong) != 0) return error.InvalidValue;
            return 0;
        },
        .list0 => {
            if ((try payload(value, .list)).len != 0) return error.InvalidValue;
            return 0;
        },
        .ubyte => {
            _ = try payload(value, .ubyte);
            return 1;
        },
        .byte => {
            _ = try payload(value, .byte);
            return 1;
        },
        .smalluint => {
            if (try payload(value, .uint) > 255) return error.InvalidValue;
            return 1;
        },
        .smallulong => {
            if (try payload(value, .ulong) > 255) return error.InvalidValue;
            return 1;
        },
        .smallint => {
            const number = try payload(value, .int);
            if (number < -128 or number > 127) return error.InvalidValue;
            return 1;
        },
        .smalllong => {
            const number = try payload(value, .long);
            if (number < -128 or number > 127) return error.InvalidValue;
            return 1;
        },
        .boolean => {
            _ = try payload(value, .boolean);
            return 1;
        },
        .ushort => {
            _ = try payload(value, .ushort);
            return 2;
        },
        .short => {
            _ = try payload(value, .short);
            return 2;
        },
        .uint => {
            _ = try payload(value, .uint);
            return 4;
        },
        .int => {
            _ = try payload(value, .int);
            return 4;
        },
        .float => {
            _ = try payload(value, .float);
            return 4;
        },
        .char => {
            try checkChar(try payload(value, .char));
            return 4;
        },
        .ulong => {
            _ = try payload(value, .ulong);
            return 8;
        },
        .long => {
            _ = try payload(value, .long);
            return 8;
        },
        .double => {
            _ = try payload(value, .double);
            return 8;
        },
        .timestamp => {
            _ = try payload(value, .timestamp);
            return 8;
        },
        .uuid => {
            _ = try payload(value, .uuid);
            return 16;
        },
        .vbin8 => {
            const data = try payload(value, .binary);
            if (data.len > 255) return error.InvalidValue;
            return 1 + data.len;
        },
        .str8 => {
            const text = try payload(value, .string);
            if (text.len > 255) return error.InvalidValue;
            try checkString(text);
            return 1 + text.len;
        },
        .sym8 => {
            const text = try payload(value, .symbol);
            if (text.len > 255) return error.InvalidValue;
            try checkSymbol(text);
            return 1 + text.len;
        },
        .vbin32 => {
            const data = try payload(value, .binary);
            if (data.len > max_u32) return error.InvalidValue;
            return 4 + data.len;
        },
        .str32 => {
            const text = try payload(value, .string);
            if (text.len > max_u32) return error.InvalidValue;
            try checkString(text);
            return 4 + text.len;
        },
        .sym32 => {
            const text = try payload(value, .symbol);
            if (text.len > max_u32) return error.InvalidValue;
            try checkSymbol(text);
            return 4 + text.len;
        },
        .list8, .list32 => {
            const items = try payload(value, .list);
            var total: usize = 0;
            for (items) |item| total += try valueSize(item, depth + 1);
            return compoundBodySize(items.len, total, code == .list8);
        },
        .map8, .map32 => {
            const entries = try payload(value, .map);
            if (entries.len > max_u32 / 2) return error.InvalidValue;
            var total: usize = 0;
            for (entries) |entry| {
                total += try valueSize(entry.key, depth + 1);
                total += try valueSize(entry.value, depth + 1);
            }
            return compoundBodySize(entries.len * 2, total, code == .map8);
        },
        .array8, .array32 => {
            const array = try payload(value, .array);
            const constructor = try elementConstructorSize(array, depth + 1);
            var total: usize = 0;
            for (array.items) |item| total += try bodySize(item, array.element, depth + 1);
            const body = constructor + total;
            if (code == .array8) {
                if (array.items.len > 255 or body > 254) return error.InvalidValue;
                return 2 + body;
            }
            if (array.items.len > max_u32 or body > max_u32 - 4) return error.InvalidValue;
            return 8 + body;
        },
    }
}

fn compoundBodySize(count: usize, total: usize, small: bool) EncodeError!usize {
    if (small) {
        if (count > 255 or total > 254) return error.InvalidValue;
        return 2 + total;
    }
    if (count > max_u32 or total > max_u32 - 4) return error.InvalidValue;
    return 8 + total;
}

fn writeValue(w: *Writer, value: Value, depth: usize) EncodeError!void {
    if (depth > max_depth) return error.InvalidValue;
    switch (value) {
        .described => |described| {
            try w.writeByte(types.described_constructor);
            try writeValue(w, described.descriptor.*, depth + 1);
            try writeValue(w, described.value.*, depth + 1);
        },
        else => {
            const chosen = try choose(value, depth);
            try w.writeByte(@intFromEnum(chosen.code));
            try writeBody(w, value, chosen.code, depth);
        },
    }
}

fn writeElementConstructor(w: *Writer, array: Array, depth: usize) EncodeError!void {
    if (array.descriptor) |descriptor| {
        try w.writeByte(types.described_constructor);
        try writeValue(w, descriptor.*, depth);
    }
    try w.writeByte(@intFromEnum(array.element));
}

/// Writes the body of `value` under the constructor `code`. The body excludes
/// the constructor itself.
fn writeBody(w: *Writer, value: Value, code: Constructor, depth: usize) EncodeError!void {
    // Each arm asks `bodySize` for the size that it needs, so the size field
    // that it writes and the bytes that follow always agree.
    switch (code) {
        .null, .boolean_true, .boolean_false, .uint0, .ulong0, .list0 => {
            _ = try bodySize(value, code, depth);
        },
        .ubyte => try w.writeByte(try payload(value, .ubyte)),
        .byte => try w.writeInt(i8, try payload(value, .byte), .big),
        .smalluint => {
            _ = try bodySize(value, code, depth);
            try w.writeByte(@intCast(try payload(value, .uint)));
        },
        .smallulong => {
            _ = try bodySize(value, code, depth);
            try w.writeByte(@intCast(try payload(value, .ulong)));
        },
        .smallint => {
            _ = try bodySize(value, code, depth);
            try w.writeInt(i8, @intCast(try payload(value, .int)), .big);
        },
        .smalllong => {
            _ = try bodySize(value, code, depth);
            try w.writeInt(i8, @intCast(try payload(value, .long)), .big);
        },
        .boolean => try w.writeByte(@intFromBool(try payload(value, .boolean))),
        .ushort => try w.writeInt(u16, try payload(value, .ushort), .big),
        .short => try w.writeInt(i16, try payload(value, .short), .big),
        .uint => try w.writeInt(u32, try payload(value, .uint), .big),
        .int => try w.writeInt(i32, try payload(value, .int), .big),
        .float => try w.writeInt(u32, @bitCast(try payload(value, .float)), .big),
        .char => {
            const code_point = try payload(value, .char);
            try checkChar(code_point);
            try w.writeInt(u32, code_point, .big);
        },
        .ulong => try w.writeInt(u64, try payload(value, .ulong), .big),
        .long => try w.writeInt(i64, try payload(value, .long), .big),
        .double => try w.writeInt(u64, @bitCast(try payload(value, .double)), .big),
        .timestamp => try w.writeInt(i64, try payload(value, .timestamp), .big),
        .uuid => try w.writeAll(&try payload(value, .uuid)),
        .vbin8, .str8, .sym8 => {
            const data = try variablePayload(value, code);
            _ = try bodySize(value, code, depth);
            try w.writeByte(@intCast(data.len));
            try w.writeAll(data);
        },
        .vbin32, .str32, .sym32 => {
            const data = try variablePayload(value, code);
            _ = try bodySize(value, code, depth);
            try w.writeInt(u32, @intCast(data.len), .big);
            try w.writeAll(data);
        },
        .list8, .list32 => {
            const items = try payload(value, .list);
            const size = try bodySize(value, code, depth);
            if (code == .list8) {
                try w.writeByte(@intCast(size - 1));
                try w.writeByte(@intCast(items.len));
            } else {
                try w.writeInt(u32, @intCast(size - 4), .big);
                try w.writeInt(u32, @intCast(items.len), .big);
            }
            for (items) |item| try writeValue(w, item, depth + 1);
        },
        .map8, .map32 => {
            const entries = try payload(value, .map);
            const size = try bodySize(value, code, depth);
            if (code == .map8) {
                try w.writeByte(@intCast(size - 1));
                try w.writeByte(@intCast(entries.len * 2));
            } else {
                try w.writeInt(u32, @intCast(size - 4), .big);
                try w.writeInt(u32, @intCast(entries.len * 2), .big);
            }
            for (entries) |entry| {
                try writeValue(w, entry.key, depth + 1);
                try writeValue(w, entry.value, depth + 1);
            }
        },
        .array8, .array32 => {
            const array = try payload(value, .array);
            const size = try bodySize(value, code, depth);
            if (code == .array8) {
                try w.writeByte(@intCast(size - 1));
                try w.writeByte(@intCast(array.items.len));
            } else {
                try w.writeInt(u32, @intCast(size - 4), .big);
                try w.writeInt(u32, @intCast(array.items.len), .big);
            }
            try writeElementConstructor(w, array, depth + 1);
            for (array.items) |item| try writeBody(w, item, array.element, depth + 1);
        },
    }
}

fn variablePayload(value: Value, code: Constructor) EncodeError![]const u8 {
    return switch (code) {
        .vbin8, .vbin32 => try payload(value, .binary),
        .str8, .str32 => try payload(value, .string),
        .sym8, .sym32 => try payload(value, .symbol),
        else => error.InvalidValue,
    };
}

// -------------------------------------------------------------------------
// The decoder
// -------------------------------------------------------------------------

/// Decodes one value from `bytes` and makes sure that the value uses every
/// byte. Use `Decoder` when the slice holds more than one value.
///
/// The result owns its memory. Free it with `Value.deinit` and the same
/// allocator.
pub fn decode(gpa: Allocator, bytes: []const u8) DecodeError!Value {
    var decoder: Decoder = .init(bytes);
    const value = try decoder.next(gpa);
    errdefer value.deinit(gpa);
    if (!decoder.atEnd()) return error.Malformed;
    return value;
}

/// A cursor over a byte slice that reads one value at a time.
///
/// Build a decoder with `init`, and then use `next` and `atEnd` only. The
/// fields below hold an invariant that the methods keep. Do not write them.
/// A write puts the cursor outside the slice, and the next read then traps on
/// an integer overflow.
///
/// One decoder holds one budget for the whole slice, and every call to `next`
/// takes from it. So a slice that holds many arrays of zero-width elements can
/// reject a later value that decodes on its own. This is deliberate. A budget
/// for each call would restore the amplification, because a sender would then
/// simply send more values. Read the note on `elementBudget`.
pub const Decoder = struct {
    /// The bytes that the decoder reads. A compound encoding narrows this
    /// slice while it reads its items, and restores it afterwards, so an item
    /// can never read past the end of the compound that holds it.
    bytes: []const u8,
    /// The offset of the next byte to read.
    pos: usize = 0,
    /// The current nesting depth.
    depth: usize = 0,
    /// The number of values that the rest of this decode can still allocate.
    /// One decode shares one budget, so a repeated small structure cannot
    /// exhaust memory. Read the note on `elementBudget` for the reasoning.
    budget: usize,

    /// Returns a decoder that reads `bytes` from the start.
    pub fn init(bytes: []const u8) Decoder {
        return .{ .bytes = bytes, .budget = elementBudget(bytes.len) };
    }

    /// Returns true when the decoder has read every byte.
    pub fn atEnd(d: *const Decoder) bool {
        return d.pos >= d.bytes.len;
    }

    /// Reads the next value. The result owns its memory. Free it with
    /// `Value.deinit` and the same allocator.
    pub fn next(d: *Decoder, gpa: Allocator) DecodeError!Value {
        return d.decodeValue(gpa);
    }

    /// Takes `count` values from the budget of this decode, and reports
    /// `error.Malformed` when the budget cannot pay for them.
    fn spend(d: *Decoder, count: usize) DecodeError!void {
        if (count > d.budget) return error.Malformed;
        d.budget -= count;
    }

    fn remaining(d: *const Decoder) usize {
        return d.bytes.len - d.pos;
    }

    fn takeSlice(d: *Decoder, len: usize) DecodeError![]const u8 {
        if (len > d.remaining()) return error.Malformed;
        const slice = d.bytes[d.pos..][0..len];
        d.pos += len;
        return slice;
    }

    fn takeByte(d: *Decoder) DecodeError!u8 {
        return (try d.takeSlice(1))[0];
    }

    fn takeInt(d: *Decoder, comptime T: type) DecodeError!T {
        const len = @divExact(@typeInfo(T).int.bits, 8);
        const slice = try d.takeSlice(len);
        return std.mem.readInt(T, slice[0..len], .big);
    }

    /// Reads a size field or a count field of `width` octets.
    fn takeWidth(d: *Decoder, comptime width: usize) DecodeError!usize {
        return switch (width) {
            1 => try d.takeByte(),
            4 => try d.takeInt(u32),
            else => @compileError("a compound width is 1 or 4"),
        };
    }

    fn takeVariable(d: *Decoder, comptime width: usize) DecodeError![]const u8 {
        const len = try d.takeWidth(width);
        return d.takeSlice(len);
    }

    fn decodeValue(d: *Decoder, gpa: Allocator) DecodeError!Value {
        if (d.depth > max_depth) return error.Malformed;
        const code_byte = try d.takeByte();
        if (code_byte == types.described_constructor) return d.decodeDescribed(gpa);
        const code = std.enums.fromInt(Constructor, code_byte) orelse return error.Malformed;
        return d.decodeBody(gpa, code);
    }

    fn decodeDescribed(d: *Decoder, gpa: Allocator) DecodeError!Value {
        d.depth += 1;
        defer d.depth -= 1;

        const descriptor = try gpa.create(Value);
        errdefer gpa.destroy(descriptor);
        descriptor.* = try d.decodeValue(gpa);
        errdefer descriptor.deinit(gpa);

        const inner = try gpa.create(Value);
        errdefer gpa.destroy(inner);
        inner.* = try d.decodeValue(gpa);

        return .{ .described = .{ .descriptor = descriptor, .value = inner } };
    }

    /// Reads the body of a value under the constructor `code`.
    fn decodeBody(d: *Decoder, gpa: Allocator, code: Constructor) DecodeError!Value {
        switch (code) {
            .null => return .null,
            .boolean_true => return .{ .boolean = true },
            .boolean_false => return .{ .boolean = false },
            .uint0 => return .{ .uint = 0 },
            .ulong0 => return .{ .ulong = 0 },
            .list0 => return .{ .list = &.{} },
            .ubyte => return .{ .ubyte = try d.takeByte() },
            .byte => return .{ .byte = try d.takeInt(i8) },
            .smalluint => return .{ .uint = try d.takeByte() },
            .smallulong => return .{ .ulong = try d.takeByte() },
            .smallint => return .{ .int = try d.takeInt(i8) },
            .smalllong => return .{ .long = try d.takeInt(i8) },
            .boolean => return switch (try d.takeByte()) {
                0x00 => .{ .boolean = false },
                0x01 => .{ .boolean = true },
                // Section 1.6.2 gives a meaning to 0x00 and to 0x01 only.
                else => error.Malformed,
            },
            .ushort => return .{ .ushort = try d.takeInt(u16) },
            .short => return .{ .short = try d.takeInt(i16) },
            .uint => return .{ .uint = try d.takeInt(u32) },
            .int => return .{ .int = try d.takeInt(i32) },
            .float => return .{ .float = @bitCast(try d.takeInt(u32)) },
            .char => {
                const code_point = try d.takeInt(u32);
                if (code_point > 0x10ffff) return error.Malformed;
                if (code_point >= 0xd800 and code_point <= 0xdfff) return error.Malformed;
                return .{ .char = @intCast(code_point) };
            },
            .ulong => return .{ .ulong = try d.takeInt(u64) },
            .long => return .{ .long = try d.takeInt(i64) },
            .double => return .{ .double = @bitCast(try d.takeInt(u64)) },
            .timestamp => return .{ .timestamp = try d.takeInt(i64) },
            .uuid => return .{ .uuid = (try d.takeSlice(16))[0..16].* },
            .vbin8 => return .{ .binary = try gpa.dupe(u8, try d.takeVariable(1)) },
            .vbin32 => return .{ .binary = try gpa.dupe(u8, try d.takeVariable(4)) },
            .str8 => return d.decodeString(gpa, 1),
            .str32 => return d.decodeString(gpa, 4),
            .sym8 => return d.decodeSymbol(gpa, 1),
            .sym32 => return d.decodeSymbol(gpa, 4),
            .list8 => return d.decodeList(gpa, 1),
            .list32 => return d.decodeList(gpa, 4),
            .map8 => return d.decodeMap(gpa, 1),
            .map32 => return d.decodeMap(gpa, 4),
            .array8 => return d.decodeArray(gpa, 1),
            .array32 => return d.decodeArray(gpa, 4),
        }
    }

    fn decodeString(d: *Decoder, gpa: Allocator, comptime width: usize) DecodeError!Value {
        const text = try d.takeVariable(width);
        if (!std.unicode.utf8ValidateSlice(text)) return error.Malformed;
        return .{ .string = try gpa.dupe(u8, text) };
    }

    fn decodeSymbol(d: *Decoder, gpa: Allocator, comptime width: usize) DecodeError!Value {
        const text = try d.takeVariable(width);
        // Section 1.6.21 limits a symbol to 7-bit ASCII.
        for (text) |byte| {
            if (byte > 0x7f) return error.Malformed;
        }
        return .{ .symbol = try gpa.dupe(u8, text) };
    }

    /// Reads the size field of a compound or an array, and narrows `bytes` to
    /// the end of the body. The caller restores `bytes` from `outer`.
    fn openBody(d: *Decoder, comptime width: usize, outer: *[]const u8) DecodeError!usize {
        const size = try d.takeWidth(width);
        // The size field counts the count field, so the size is at least the
        // width of the count field.
        if (size < width) return error.Malformed;
        if (size > d.remaining()) return error.Malformed;
        const end = d.pos + size;
        outer.* = d.bytes;
        d.bytes = d.bytes[0..end];
        return end;
    }

    fn decodeList(d: *Decoder, gpa: Allocator, comptime width: usize) DecodeError!Value {
        var outer: []const u8 = undefined;
        const end = try d.openBody(width, &outer);
        defer d.bytes = outer;

        const count = try d.takeWidth(width);
        // Every item starts with a constructor octet, so the count cannot pass
        // the number of octets that remain.
        if (count > d.remaining()) return error.Malformed;

        var items: std.ArrayList(Value) = .empty;
        errdefer {
            for (items.items) |item| item.deinit(gpa);
            items.deinit(gpa);
        }
        try d.spend(count);
        try items.ensureTotalCapacity(gpa, count);

        d.depth += 1;
        defer d.depth -= 1;
        for (0..count) |_| items.appendAssumeCapacity(try d.decodeValue(gpa));

        if (d.pos != end) return error.Malformed;
        return .{ .list = try items.toOwnedSlice(gpa) };
    }

    fn decodeMap(d: *Decoder, gpa: Allocator, comptime width: usize) DecodeError!Value {
        var outer: []const u8 = undefined;
        const end = try d.openBody(width, &outer);
        defer d.bytes = outer;

        const count = try d.takeWidth(width);
        // A map holds a key and a value for each entry, so the count is even.
        if (count % 2 != 0) return error.Malformed;
        if (count > d.remaining()) return error.Malformed;

        var entries: std.ArrayList(MapEntry) = .empty;
        errdefer {
            for (entries.items) |entry| {
                entry.key.deinit(gpa);
                entry.value.deinit(gpa);
            }
            entries.deinit(gpa);
        }
        try d.spend(count);
        try entries.ensureTotalCapacity(gpa, count / 2);

        d.depth += 1;
        defer d.depth -= 1;
        for (0..count / 2) |_| {
            const key = try d.decodeValue(gpa);
            errdefer key.deinit(gpa);
            const value = try d.decodeValue(gpa);
            entries.appendAssumeCapacity(.{ .key = key, .value = value });
        }

        if (d.pos != end) return error.Malformed;
        return .{ .map = try entries.toOwnedSlice(gpa) };
    }

    fn decodeArray(d: *Decoder, gpa: Allocator, comptime width: usize) DecodeError!Value {
        var outer: []const u8 = undefined;
        const end = try d.openBody(width, &outer);
        defer d.bytes = outer;

        const count = try d.takeWidth(width);

        d.depth += 1;
        defer d.depth -= 1;
        if (d.depth > max_depth) return error.Malformed;

        var descriptor: ?*Value = null;
        errdefer if (descriptor) |pointer| {
            pointer.deinit(gpa);
            gpa.destroy(pointer);
        };

        var code_byte = try d.takeByte();
        if (code_byte == types.described_constructor) {
            const pointer = try gpa.create(Value);
            pointer.* = d.decodeValue(gpa) catch |err| {
                gpa.destroy(pointer);
                return err;
            };
            descriptor = pointer;
            code_byte = try d.takeByte();
            // A descriptor cannot introduce a second descriptor.
            if (code_byte == types.described_constructor) return error.Malformed;
        }
        const code = std.enums.fromInt(Constructor, code_byte) orelse return error.Malformed;

        const min = minBodySize(code);
        if (min == 0) {
            // The encoded size does not bound the count of a zero-width
            // element, so the decoder applies its own limit.
            if (count > max_zero_width_elements) return error.Malformed;
        } else if (count > d.remaining() / min) return error.Malformed;

        var items: std.ArrayList(Value) = .empty;
        errdefer {
            for (items.items) |item| item.deinit(gpa);
            items.deinit(gpa);
        }
        try d.spend(count);
        try items.ensureTotalCapacity(gpa, count);

        for (0..count) |_| items.appendAssumeCapacity(try d.decodeBody(gpa, code));

        if (d.pos != end) return error.Malformed;
        return .{ .array = .{
            .element = code,
            .descriptor = descriptor,
            .items = try items.toOwnedSlice(gpa),
        } };
    }
};

/// Returns the smallest number of octets that a body under `code` can take.
/// The decoder uses it to reject a count that the input cannot hold.
fn minBodySize(code: Constructor) usize {
    return switch (code) {
        .null, .boolean_true, .boolean_false, .uint0, .ulong0, .list0 => 0,
        .ubyte, .byte, .smalluint, .smallulong, .smallint, .smalllong, .boolean => 1,
        .ushort, .short => 2,
        .uint, .int, .float, .char => 4,
        .ulong, .long, .double, .timestamp => 8,
        .uuid => 16,
        // The size field alone.
        .vbin8, .str8, .sym8 => 1,
        .vbin32, .str32, .sym32 => 4,
        // The size field and the count field.
        .list8, .map8 => 2,
        .list32, .map32 => 8,
        // The size field, the count field, and the element constructor.
        .array8 => 3,
        .array32 => 9,
    };
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

const testing = std.testing;

/// Encodes `value` and compares the bytes with `expected`.
fn expectEncoding(value: Value, expected: []const u8) !void {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try encode(value, &sink.writer);
    try testing.expectEqualSlices(u8, expected, sink.written());
    try testing.expectEqual(expected.len, try encodedSize(value));
}

/// Decodes `bytes` and compares the value with `expected`.
fn expectDecoding(bytes: []const u8, expected: Value) !void {
    const gpa = testing.allocator;
    const value = try decode(gpa, bytes);
    defer value.deinit(gpa);
    try testing.expectEqualDeep(expected, value);
}

/// Checks both directions against one golden byte vector.
fn expectGolden(bytes: []const u8, value: Value) !void {
    try expectEncoding(value, bytes);
    try expectDecoding(bytes, value);
}

/// Encodes a value, decodes the bytes again, and compares the two values.
fn expectRoundTrip(value: Value) !void {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try encode(value, &sink.writer);
    try testing.expectEqual(sink.written().len, try encodedSize(value));

    const back = try decode(gpa, sink.written());
    defer back.deinit(gpa);
    try testing.expectEqualDeep(value, back);
}

test "golden vector: null" {
    try expectGolden(&.{0x40}, .null);
}

test "golden vector: boolean in the compact forms" {
    try expectGolden(&.{0x41}, .{ .boolean = true });
    try expectGolden(&.{0x42}, .{ .boolean = false });
}

test "golden vector: boolean in the 1-octet form" {
    // The encoder always writes the compact form, so this code is decode only.
    try expectDecoding(&.{ 0x56, 0x01 }, .{ .boolean = true });
    try expectDecoding(&.{ 0x56, 0x00 }, .{ .boolean = false });
    try testing.expectError(error.Malformed, decode(testing.allocator, &.{ 0x56, 0x02 }));
}

test "golden vector: ubyte and ushort" {
    try expectGolden(&.{ 0x50, 0xff }, .{ .ubyte = 255 });
    try expectGolden(&.{ 0x60, 0x12, 0x34 }, .{ .ushort = 0x1234 });
}

test "golden vector: uint in the three forms" {
    try expectGolden(&.{0x43}, .{ .uint = 0 });
    try expectGolden(&.{ 0x52, 0x7f }, .{ .uint = 127 });
    try expectGolden(&.{ 0x52, 0xff }, .{ .uint = 255 });
    try expectGolden(&.{ 0x70, 0x00, 0x00, 0x01, 0x00 }, .{ .uint = 256 });
    // The decoder accepts a wide form that holds a small value.
    try expectDecoding(&.{ 0x70, 0x00, 0x00, 0x00, 0x07 }, .{ .uint = 7 });
}

test "golden vector: ulong in the three forms" {
    try expectGolden(&.{0x44}, .{ .ulong = 0 });
    try expectGolden(&.{ 0x53, 0x2a }, .{ .ulong = 42 });
    try expectGolden(
        &.{ 0x80, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00 },
        .{ .ulong = 4294967296 },
    );
    try expectDecoding(
        &.{ 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09 },
        .{ .ulong = 9 },
    );
}

test "golden vector: byte and short" {
    try expectGolden(&.{ 0x51, 0x80 }, .{ .byte = -128 });
    try expectGolden(&.{ 0x51, 0x7f }, .{ .byte = 127 });
    try expectGolden(&.{ 0x61, 0xff, 0xfe }, .{ .short = -2 });
}

test "golden vector: int in the two forms" {
    try expectGolden(&.{ 0x54, 0xff }, .{ .int = -1 });
    try expectGolden(&.{ 0x54, 0x80 }, .{ .int = -128 });
    try expectGolden(&.{ 0x71, 0xff, 0xff, 0xfe, 0x00 }, .{ .int = -512 });
    try expectDecoding(&.{ 0x71, 0x00, 0x00, 0x00, 0x05 }, .{ .int = 5 });
}

test "golden vector: long in the two forms" {
    try expectGolden(&.{ 0x55, 0x7f }, .{ .long = 127 });
    try expectGolden(
        &.{ 0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfc, 0x18 },
        .{ .long = -1000 },
    );
}

test "golden vector: float and double" {
    try expectGolden(&.{ 0x72, 0x3f, 0x80, 0x00, 0x00 }, .{ .float = 1.0 });
    try expectGolden(&.{ 0x72, 0xc0, 0x00, 0x00, 0x00 }, .{ .float = -2.0 });
    try expectGolden(
        &.{ 0x82, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
        .{ .double = 1.0 },
    );
}

test "golden vector: char" {
    try expectGolden(&.{ 0x73, 0x00, 0x00, 0x00, 0x41 }, .{ .char = 'A' });
    try expectGolden(&.{ 0x73, 0x00, 0x00, 0x00, 0xe9 }, .{ .char = 0xe9 });
    try expectGolden(&.{ 0x73, 0x00, 0x10, 0xff, 0xff }, .{ .char = 0x10ffff });
    // A code point above the Unicode range, and a surrogate.
    try testing.expectError(
        error.Malformed,
        decode(testing.allocator, &.{ 0x73, 0x00, 0x11, 0x00, 0x00 }),
    );
    try testing.expectError(
        error.Malformed,
        decode(testing.allocator, &.{ 0x73, 0x00, 0x00, 0xd8, 0x00 }),
    );
}

test "golden vector: timestamp" {
    try expectGolden(
        // 1311704463585 is 0x0000013167adb8e1.
        &.{ 0x83, 0x00, 0x00, 0x01, 0x31, 0x67, 0xad, 0xb8, 0xe1 },
        .{ .timestamp = 1311704463585 },
    );
    // A timestamp before the Unix epoch is a negative number.
    try expectGolden(
        &.{ 0x83, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff },
        .{ .timestamp = -1 },
    );
}

test "golden vector: uuid" {
    const uuid: [16]u8 = .{
        0xf8, 0x1d, 0x4f, 0xae, 0x7d, 0xec, 0x11, 0xd0,
        0xa7, 0x65, 0x00, 0xa0, 0xc9, 0x1e, 0x6b, 0xf6,
    };
    try expectGolden(&(.{0x98} ++ uuid), .{ .uuid = uuid });
}

test "golden vector: binary in the vbin8 form" {
    try expectGolden(&.{ 0xa0, 0x03, 0x01, 0x02, 0x03 }, .{ .binary = &.{ 0x01, 0x02, 0x03 } });
    try expectGolden(&.{ 0xa0, 0x00 }, .{ .binary = &.{} });
}

test "golden vector: binary in the vbin32 form" {
    const data = "b" ** 256;
    const expected = .{ 0xb0, 0x00, 0x00, 0x01, 0x00 } ++ data.*;
    try expectGolden(&expected, .{ .binary = data });
}

test "golden vector: string in the str8 form" {
    // Section 1.1.1 of the specification shows this string.
    const text = "Hello Glorious Messaging World";
    const expected = .{ 0xa1, 0x1e } ++ text.*;
    try expectGolden(&expected, .{ .string = text });
}

test "golden vector: string in the str32 form" {
    const text = "s" ** 300;
    const expected = .{ 0xb1, 0x00, 0x00, 0x01, 0x2c } ++ text.*;
    try expectGolden(&expected, .{ .string = text });
}

test "golden vector: symbol in the sym8 form" {
    const text = "example:book:list";
    const expected = .{ 0xa3, 0x11 } ++ text.*;
    try expectGolden(&expected, .{ .symbol = text });
}

test "golden vector: symbol in the sym32 form" {
    const text = "y" ** 256;
    const expected = .{ 0xb3, 0x00, 0x00, 0x01, 0x00 } ++ text.*;
    try expectGolden(&expected, .{ .symbol = text });
}

test "golden vector: list in the list0 and list8 forms" {
    try expectGolden(&.{0x45}, .{ .list = &.{} });
    // One null item. The size field counts the count field.
    try expectGolden(&.{ 0xc0, 0x02, 0x01, 0x40 }, .{ .list = &.{.null} });
    // The two items take 5 octets, so the size field holds 6.
    try expectGolden(
        &.{ 0xc0, 0x06, 0x02, 0x52, 0x07, 0xa1, 0x01, 0x7a },
        .{ .list = &.{ .{ .uint = 7 }, .{ .string = "z" } } },
    );
}

test "golden vector: list in the list32 form" {
    const items: [256]Value = @splat(.null);
    var expected: std.ArrayList(u8) = .empty;
    defer expected.deinit(testing.allocator);
    try expected.appendSlice(testing.allocator, &.{
        0xd0, 0x00, 0x00, 0x01, 0x04, 0x00, 0x00, 0x01, 0x00,
    });
    try expected.appendNTimes(testing.allocator, 0x40, 256);
    try expectGolden(expected.items, .{ .list = &items });
}

test "golden vector: map in the map8 form" {
    try expectGolden(&.{ 0xc1, 0x01, 0x00 }, .{ .map = &.{} });
    try expectGolden(
        &.{ 0xc1, 0x05, 0x02, 0xa3, 0x01, 0x61, 0x40 },
        .{ .map = &.{.{ .key = .{ .symbol = "a" }, .value = .null }} },
    );
}

test "golden vector: map in the map32 form" {
    // 128 entries make a count of 256, which does not fit in one octet.
    var entries: [128]MapEntry = undefined;
    for (&entries, 0..) |*entry, index| {
        entry.* = .{ .key = .{ .ubyte = @intCast(index) }, .value = .null };
    }

    var expected: std.ArrayList(u8) = .empty;
    defer expected.deinit(testing.allocator);
    // Each entry takes 3 octets, so the items take 384 octets and the size
    // field holds 388.
    try expected.appendSlice(testing.allocator, &.{
        0xd1, 0x00, 0x00, 0x01, 0x84, 0x00, 0x00, 0x01, 0x00,
    });
    for (0..128) |index| {
        try expected.appendSlice(testing.allocator, &.{ 0x50, @intCast(index), 0x40 });
    }
    try expectGolden(expected.items, .{ .map = &entries });
}

test "golden vector: array in the array8 form" {
    try expectGolden(
        &.{ 0xe0, 0x04, 0x02, 0x52, 0x01, 0x02 },
        .{ .array = .{
            .element = .smalluint,
            .items = &.{ .{ .uint = 1 }, .{ .uint = 2 } },
        } },
    );
    // An array with no element still names its element constructor.
    try expectGolden(
        &.{ 0xe0, 0x02, 0x00, 0xa3 },
        .{ .array = .{ .element = .sym8, .items = &.{} } },
    );
}

test "golden vector: array in the array32 form" {
    // 256 null elements take no octets at all, so the whole array is 10 bytes.
    const items: [256]Value = @splat(.null);
    try expectGolden(
        &.{ 0xf0, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x01, 0x00, 0x40 },
        .{ .array = .{ .element = .null, .items = &items } },
    );
}

test "golden vector: a described string from the specification" {
    // Section 1.1.2 shows a URL as a described string.
    const descriptor: Value = .{ .string = "URL" };
    const value: Value = .{ .string = "http://example.org/hello-world" };
    const expected =
        .{ 0x00, 0xa1, 0x03 } ++ "URL".* ++
        .{ 0xa1, 0x1e } ++ "http://example.org/hello-world".*;
    try expectGolden(&expected, .{ .described = .{ .descriptor = &descriptor, .value = &value } });
}

test "golden vector: the composite value of specification figure 1.19" {
    const descriptor: Value = .{ .symbol = "example:book:list" };
    const authors: [2]Value = .{
        .{ .string = "Rob J. Godfrey" },
        .{ .string = "Rafael H. Schloming" },
    };
    const items: [3]Value = .{
        .{ .string = "AMQP for & by Dummies" },
        .{ .array = .{ .element = .str8, .items = &authors } },
        .null,
    };
    const body: Value = .{ .list = &items };

    const expected =
        .{ 0x00, 0xa3, 0x11 } ++ "example:book:list".* ++
        .{ 0xc0, 0x40, 0x03 } ++
        .{ 0xa1, 0x15 } ++ "AMQP for & by Dummies".* ++
        .{ 0xe0, 0x25, 0x02, 0xa1, 0x0e } ++ "Rob J. Godfrey".* ++
        .{0x13} ++ "Rafael H. Schloming".* ++
        .{0x40};

    try expectGolden(
        &expected,
        .{ .described = .{ .descriptor = &descriptor, .value = &body } },
    );
}

test "golden vector: an array with a described element constructor" {
    // The element constructor is a described list0, so each element body is
    // empty and the two elements add no octets at all.
    const descriptor: Value = .{ .ulong = 0x24 };
    try expectGolden(
        &.{ 0xe0, 0x05, 0x02, 0x00, 0x53, 0x24, 0x45 },
        .{ .array = .{
            .element = .list0,
            .descriptor = &descriptor,
            .items = &.{ .{ .list = &.{} }, .{ .list = &.{} } },
        } },
    );
}

test "round trip: every scalar encoding" {
    try expectRoundTrip(.null);
    try expectRoundTrip(.{ .boolean = true });
    try expectRoundTrip(.{ .boolean = false });
    try expectRoundTrip(.{ .ubyte = 0 });
    try expectRoundTrip(.{ .ubyte = 255 });
    try expectRoundTrip(.{ .ushort = 0 });
    try expectRoundTrip(.{ .ushort = 65535 });
    try expectRoundTrip(.{ .uint = 0 });
    try expectRoundTrip(.{ .uint = 255 });
    try expectRoundTrip(.{ .uint = 256 });
    try expectRoundTrip(.{ .uint = 4294967295 });
    try expectRoundTrip(.{ .ulong = 0 });
    try expectRoundTrip(.{ .ulong = 255 });
    try expectRoundTrip(.{ .ulong = 256 });
    try expectRoundTrip(.{ .ulong = 18446744073709551615 });
    try expectRoundTrip(.{ .byte = -128 });
    try expectRoundTrip(.{ .byte = 127 });
    try expectRoundTrip(.{ .short = -32768 });
    try expectRoundTrip(.{ .short = 32767 });
    try expectRoundTrip(.{ .int = -128 });
    try expectRoundTrip(.{ .int = 127 });
    try expectRoundTrip(.{ .int = -129 });
    try expectRoundTrip(.{ .int = 2147483647 });
    try expectRoundTrip(.{ .long = -128 });
    try expectRoundTrip(.{ .long = 127 });
    try expectRoundTrip(.{ .long = -129 });
    try expectRoundTrip(.{ .long = 9223372036854775807 });
    try expectRoundTrip(.{ .float = 3.5 });
    try expectRoundTrip(.{ .float = -0.0 });
    try expectRoundTrip(.{ .double = 2.718281828459045 });
    try expectRoundTrip(.{ .char = 0 });
    try expectRoundTrip(.{ .char = 0x10ffff });
    try expectRoundTrip(.{ .timestamp = 0 });
    try expectRoundTrip(.{ .timestamp = -9223372036854775808 });
    try expectRoundTrip(.{ .uuid = @splat(0xab) });
}

test "round trip: a float that is not a number keeps its bits" {
    const gpa = testing.allocator;
    const value: Value = .{ .double = std.math.nan(f64) };
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try encode(value, &sink.writer);

    const back = try decode(gpa, sink.written());
    defer back.deinit(gpa);
    try testing.expectEqual(
        @as(u64, @bitCast(value.double)),
        @as(u64, @bitCast(back.double)),
    );
}

test "round trip: every variable width encoding" {
    try expectRoundTrip(.{ .binary = &.{} });
    try expectRoundTrip(.{ .binary = "\x00\xff\x01" });
    try expectRoundTrip(.{ .binary = "d" ** 255 });
    try expectRoundTrip(.{ .binary = "d" ** 256 });
    try expectRoundTrip(.{ .string = "" });
    try expectRoundTrip(.{ .string = "text with a wide code point: \u{1f10d}" });
    try expectRoundTrip(.{ .string = "t" ** 255 });
    try expectRoundTrip(.{ .string = "t" ** 256 });
    try expectRoundTrip(.{ .symbol = "" });
    try expectRoundTrip(.{ .symbol = "a:b:c" });
    try expectRoundTrip(.{ .symbol = "s" ** 255 });
    try expectRoundTrip(.{ .symbol = "s" ** 256 });
}

test "round trip: every compound encoding" {
    const small_list: [3]Value = .{ .null, .{ .uint = 1 }, .{ .symbol = "k" } };
    try expectRoundTrip(.{ .list = &.{} });
    try expectRoundTrip(.{ .list = &small_list });

    const large_items: [300]Value = @splat(.{ .ubyte = 9 });
    try expectRoundTrip(.{ .list = &large_items });

    try expectRoundTrip(.{ .map = &.{} });
    const entries: [2]MapEntry = .{
        .{ .key = .{ .symbol = "one" }, .value = .{ .uint = 1 } },
        .{ .key = .{ .ulong = 2 }, .value = .{ .list = &small_list } },
    };
    try expectRoundTrip(.{ .map = &entries });

    var large_entries: [200]MapEntry = undefined;
    for (&large_entries, 0..) |*entry, index| {
        entry.* = .{ .key = .{ .ushort = @intCast(index) }, .value = .{ .boolean = true } };
    }
    try expectRoundTrip(.{ .map = &large_entries });
}

test "round trip: every array encoding" {
    try expectRoundTrip(.{ .array = .{ .element = .null, .items = &.{} } });
    try expectRoundTrip(.{ .array = .{
        .element = .sym8,
        .items = &.{ .{ .symbol = "first" }, .{ .symbol = "second" } },
    } });

    const large_items: [300]Value = @splat(.{ .ushort = 0x0102 });
    try expectRoundTrip(.{ .array = .{ .element = .ushort, .items = &large_items } });

    // An array whose elements are lists.
    const inner: [2]Value = .{ .{ .list = &.{.null} }, .{ .list = &.{} } };
    try expectRoundTrip(.{ .array = .{ .element = .list8, .items = &inner } });
}

test "round trip: nested described values" {
    const inner_descriptor: Value = .{ .ulong = 0x73 };
    const inner_body: Value = .{ .list = &.{ .{ .string = "id" }, .null } };
    const inner: Value = .{ .described = .{
        .descriptor = &inner_descriptor,
        .value = &inner_body,
    } };
    const outer_descriptor: Value = .{ .symbol = "outer" };
    try expectRoundTrip(.{ .described = .{
        .descriptor = &outer_descriptor,
        .value = &inner,
    } });
}

test "the encoder rejects a value that its constructor cannot hold" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();

    // The element does not agree with the element constructor.
    try testing.expectError(error.InvalidValue, encode(.{ .array = .{
        .element = .smalluint,
        .items = &.{.{ .uint = 300 }},
    } }, &sink.writer));

    // The element has the wrong type.
    try testing.expectError(error.InvalidValue, encode(.{ .array = .{
        .element = .sym8,
        .items = &.{.{ .uint = 1 }},
    } }, &sink.writer));

    // A string that is not valid UTF-8.
    try testing.expectError(error.InvalidValue, encode(.{ .string = "\xff" }, &sink.writer));

    // A symbol with a byte above 0x7f.
    try testing.expectError(error.InvalidValue, encode(.{ .symbol = "\x80" }, &sink.writer));
}

test "the decoder rejects a truncated input" {
    const gpa = testing.allocator;
    const cases: []const []const u8 = &.{
        &.{},
        &.{0x50},
        &.{ 0x60, 0x01 },
        &.{ 0x70, 0x00, 0x00 },
        &.{ 0x80, 0x00 },
        &.{ 0xa0, 0x04, 0x01 },
        &.{ 0xb1, 0x00, 0x00, 0x00 },
        &.{ 0xc0, 0x05, 0x01, 0x40 },
        &.{ 0xd0, 0x00, 0x00, 0x00, 0x08 },
        &.{ 0xe0, 0x04, 0x02, 0x50, 0x01 },
        &.{0x00},
        &.{ 0x00, 0x53, 0x24 },
    };
    for (cases) |bytes| {
        try testing.expectError(error.Malformed, decode(gpa, bytes));
    }
}

test "the decoder rejects a bad constructor" {
    const gpa = testing.allocator;
    // 0x01 is not a constructor. 0x74 is decimal32, which this codec does not
    // support. 0xa2 is not assigned.
    try testing.expectError(error.Malformed, decode(gpa, &.{0x01}));
    try testing.expectError(error.Malformed, decode(gpa, &.{ 0x74, 0, 0, 0, 0 }));
    try testing.expectError(error.Malformed, decode(gpa, &.{ 0xa2, 0x00 }));
}

test "the decoder rejects text that is not valid UTF-8" {
    const gpa = testing.allocator;
    try testing.expectError(error.Malformed, decode(gpa, &.{ 0xa1, 0x01, 0xff }));
    try testing.expectError(error.Malformed, decode(gpa, &.{ 0xa1, 0x02, 0xc3, 0x28 }));
    // A symbol holds 7-bit ASCII only.
    try testing.expectError(error.Malformed, decode(gpa, &.{ 0xa3, 0x01, 0x80 }));
}

test "the decoder rejects a compound whose size does not match its items" {
    const gpa = testing.allocator;
    // The size field says 3, but the single null item ends after 2 octets.
    try testing.expectError(error.Malformed, decode(gpa, &.{ 0xc0, 0x03, 0x01, 0x40, 0x40 }));
    // The count says 2, but the size holds room for one item only.
    try testing.expectError(error.Malformed, decode(gpa, &.{ 0xc0, 0x02, 0x02, 0x40 }));
    // A map needs an even count.
    try testing.expectError(error.Malformed, decode(gpa, &.{ 0xc1, 0x02, 0x01, 0x40 }));
    // An item may not read past the end of the list that holds it.
    try testing.expectError(
        error.Malformed,
        decode(gpa, &.{ 0xc0, 0x03, 0x01, 0xa0, 0x02, 0x01, 0x02 }),
    );
}

test "the decoder rejects trailing bytes" {
    const gpa = testing.allocator;
    try testing.expectError(error.Malformed, decode(gpa, &.{ 0x40, 0x40 }));
}

test "the decoder rejects an array count that the input cannot hold" {
    const gpa = testing.allocator;
    // A 4-billion element array of ubyte in 7 octets.
    try testing.expectError(
        error.Malformed,
        decode(gpa, &.{ 0xf0, 0x00, 0x00, 0x00, 0x05, 0xff, 0xff, 0xff, 0xff, 0x50 }),
    );
    // A 4-billion element array of null. The size field cannot bound it, so
    // `max_zero_width_elements` does.
    try testing.expectError(
        error.Malformed,
        decode(gpa, &.{ 0xf0, 0x00, 0x00, 0x00, 0x05, 0xff, 0xff, 0xff, 0xff, 0x40 }),
    );
}

test "the decode budget bounds many small zero-width arrays" {
    const gpa = testing.allocator;

    // Each inner array costs 10 octets and declares 8192 null elements, which
    // `max_zero_width_elements` allows one at a time. A list of a thousand of
    // them allocated about 328 MB before the budget existed.
    const inner: [10]u8 = .{ 0xf0, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x20, 0x00, 0x40 };
    const count = 1000;

    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(gpa);
    try input.append(gpa, @intFromEnum(Constructor.list32));
    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], count * inner.len + 4, .big);
    std.mem.writeInt(u32, header[4..8], count, .big);
    try input.appendSlice(gpa, &header);
    for (0..count) |_| try input.appendSlice(gpa, &inner);

    try testing.expectError(error.Malformed, decode(gpa, input.items));
}

test "one array of the largest legal zero-width count still decodes" {
    const gpa = testing.allocator;

    // The budget must not reject what `max_zero_width_elements` allows.
    var input: [10]u8 = .{ 0xf0, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x40 };
    std.mem.writeInt(u32, input[5..9], max_zero_width_elements, .big);

    const value = try decode(gpa, &input);
    defer value.deinit(gpa);
    try testing.expectEqual(@as(usize, max_zero_width_elements), value.array.items.len);
}

test "the decoder invariant holds for a float that is not a number" {
    // `72 7f c0 00 00` is a legal float encoding. A NaN is never equal to a
    // NaN, so an invariant that compares the values reports a false defect.
    try testing.expect(try checkDecoder(testing.allocator, &.{ 0x72, 0x7f, 0xc0, 0x00, 0x00 }));
    try testing.expect(try checkDecoder(testing.allocator, &.{
        0x82, 0x7f, 0xf4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    }));
}

test "the decoder rejects an input that nests too deep" {
    const gpa = testing.allocator;
    // Each described constructor adds one level.
    var bytes: [max_depth + 4]u8 = @splat(types.described_constructor);
    bytes[bytes.len - 1] = 0x40;
    try testing.expectError(error.Malformed, decode(gpa, &bytes));
}

test "the decoder reads two values from one slice" {
    const gpa = testing.allocator;
    var decoder: Decoder = .init(&.{ 0x43, 0xa1, 0x01, 0x7a });

    const first = try decoder.next(gpa);
    defer first.deinit(gpa);
    try testing.expectEqualDeep(Value{ .uint = 0 }, first);

    const second = try decoder.next(gpa);
    defer second.deinit(gpa);
    try testing.expectEqualDeep(Value{ .string = "z" }, second);

    try testing.expect(decoder.atEnd());
}

test "the size agrees with the encoder" {
    // `encodedSize` and `encode` walk the value with separate code, so every
    // test above compares the two through `expectEncoding`
    // and `expectRoundTrip`. This test covers the shapes that the golden
    // vectors do not reach.
    const descriptor: Value = .{ .symbol = "d" };
    const inner: [2]Value = .{ .{ .binary = "b" ** 256 }, .{ .string = "s" ** 256 } };
    const body: Value = .{ .list = &inner };
    try expectRoundTrip(.{ .described = .{ .descriptor = &descriptor, .value = &body } });

    const array_items: [4]Value = .{
        .{ .map = &.{} },
        .{ .map = &.{} },
        .{ .map = &.{} },
        .{ .map = &.{} },
    };
    try expectRoundTrip(.{ .array = .{ .element = .map8, .items = &array_items } });
}

/// The bytes of the composite value of specification figure 1.19.
const book_bytes =
    .{ 0x00, 0xa3, 0x11 } ++ "example:book:list".* ++
    .{ 0xc0, 0x40, 0x03 } ++
    .{ 0xa1, 0x15 } ++ "AMQP for & by Dummies".* ++
    .{ 0xe0, 0x25, 0x02, 0xa1, 0x0e } ++ "Rob J. Godfrey".* ++
    .{0x13} ++ "Rafael H. Schloming".* ++
    .{0x40};

fn decodeAndFree(gpa: Allocator, bytes: []const u8) !void {
    const value = try decode(gpa, bytes);
    value.deinit(gpa);
}

test "a decode that runs out of memory frees what it allocated" {
    // Each input reaches a different set of allocations: the described value,
    // the list, the array, the map, and the array with a described element
    // constructor.
    const inputs: []const []const u8 = &.{
        &book_bytes,
        &.{ 0xc1, 0x0b, 0x04, 0xa3, 0x01, 0x61, 0xa1, 0x01, 0x62, 0xa3, 0x01, 0x63, 0x40 },
        &.{ 0xe0, 0x05, 0x02, 0x00, 0x53, 0x24, 0x45 },
        &.{ 0xe0, 0x0a, 0x02, 0xa1, 0x03, 'o', 'n', 'e', 0x03, 't', 'w', 'o' },
    };
    for (inputs) |bytes| {
        try testing.checkAllAllocationFailures(testing.allocator, decodeAndFree, .{bytes});
    }
}

/// Runs one input through the decoder and applies the rules that must hold for
/// every input. The decoder must never crash and never leak. It either returns
/// `error.Malformed`, or it returns a value that encodes again and decodes to
/// the same value.
///
/// Returns true when the input decoded. A caller counts the true results, so
/// that a generator which stops producing legal input cannot pass unseen.
fn checkDecoder(gpa: Allocator, input: []const u8) !bool {
    const value = decode(gpa, input) catch |err| switch (err) {
        error.Malformed => return false,
        error.OutOfMemory => return false,
    };
    defer value.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    encode(value, &sink.writer) catch |err| switch (err) {
        error.WriteFailed => return true,
        error.InvalidValue => return error.EncodeRejectedADecodedValue,
    };
    try testing.expectEqual(sink.written().len, try encodedSize(value));

    const again = decode(gpa, sink.written()) catch |err| switch (err) {
        error.OutOfMemory => return true,
        error.Malformed => return error.DecodeRejectedItsOwnEncoding,
    };
    defer again.deinit(gpa);

    // Compare the two encodings, not the two values. `expectEqualDeep`
    // compares a float with `==`, and a NaN is never equal to a NaN, so it
    // reports a legal float encoding such as `72 7f c0 00 00` as a defect.
    // The encoding of a decoded value is deterministic, so the comparison of
    // the bytes is the stronger test, and it holds for a NaN.
    var again_sink: Writer.Allocating = .init(gpa);
    defer again_sink.deinit();
    encode(again, &again_sink.writer) catch |err| switch (err) {
        error.WriteFailed => return true,
        error.InvalidValue => return error.EncodeRejectedADecodedValue,
    };
    try testing.expectEqualSlices(u8, sink.written(), again_sink.written());
    return true;
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

fn fuzzDecoder(_: void, smith: *testing.Smith) anyerror!void {
    var buffer: [512]u8 = undefined;
    const len = smith.slice(&buffer);
    _ = try checkDecoder(testing.allocator, buffer[0..len]);
}

test "fuzz the decoder" {
    try testing.fuzz({}, fuzzDecoder, .{ .corpus = &.{
        corpusEntry(&.{0x40}),
        corpusEntry(&.{ 0x56, 0x01 }),
        corpusEntry(&.{ 0xa1, 0x05, 'h', 'e', 'l', 'l', 'o' }),
        corpusEntry(&.{ 0xc0, 0x02, 0x01, 0x40 }),
        corpusEntry(&.{ 0xc1, 0x05, 0x02, 0xa3, 0x01, 0x61, 0x40 }),
        corpusEntry(&.{ 0xe0, 0x04, 0x02, 0x52, 0x01, 0x02 }),
        corpusEntry(&book_bytes),
    } });
}

/// Valid encodings that the mutation test starts from.
const fuzz_seeds: []const []const u8 = &.{
    &.{0x40},
    &.{ 0x56, 0x01 },
    &.{ 0x70, 0x00, 0x00, 0x01, 0x00 },
    &.{ 0x83, 0x00, 0x00, 0x01, 0x31, 0x67, 0xad, 0xb8, 0xe1 },
    &.{ 0x98, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
    &.{ 0xa1, 0x05, 'h', 'e', 'l', 'l', 'o' },
    &.{ 0xb1, 0x00, 0x00, 0x00, 0x02, 0xc3, 0xa9 },
    &.{ 0xa3, 0x03, 'a', ':', 'b' },
    &.{0x45},
    &.{ 0xc0, 0x06, 0x02, 0x52, 0x07, 0xa1, 0x01, 0x7a },
    &.{ 0xd0, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 0x40 },
    &.{ 0xc1, 0x05, 0x02, 0xa3, 0x01, 0x61, 0x40 },
    &.{ 0xe0, 0x04, 0x02, 0x52, 0x01, 0x02 },
    &.{ 0xf0, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x01, 0x00, 0x40 },
    &.{ 0xe0, 0x05, 0x02, 0x00, 0x53, 0x24, 0x45 },
    &book_bytes,
};

test "every fuzz seed is a legal encoding" {
    // A seed that the decoder rejects gives the mutation test below a much
    // smaller reach, and nothing else would report it.
    const gpa = testing.allocator;
    for (fuzz_seeds) |seed| {
        const value = try decode(gpa, seed);
        value.deinit(gpa);
    }
}

// `std.testing.fuzz` needs `zig build test --fuzz`, and that mode does not
// build with every toolchain. This test drives the same rules from a seeded
// generator, so `zig build test` alone still exercises the decoder against
// many inputs. It mutates valid encodings, because purely random bytes stop
// at the first constructor octet almost every time.
test "the decoder survives mutated encodings" {
    const gpa = testing.allocator;
    var prng: std.Random.DefaultPrng = .init(0x414d5150);
    const random = prng.random();

    var decoded: usize = 0;
    var buffer: [512]u8 = undefined;
    for (0..20000) |_| {
        const seed = fuzz_seeds[random.uintLessThan(usize, fuzz_seeds.len)];
        const len = @min(seed.len, buffer.len);
        @memcpy(buffer[0..len], seed[0..len]);
        var input: []u8 = buffer[0..len];

        for (0..random.uintLessThan(usize, 4) + 1) |_| {
            switch (random.uintLessThan(u8, 4)) {
                // Change one octet.
                0 => if (input.len > 0) {
                    input[random.uintLessThan(usize, input.len)] = random.int(u8);
                },
                // Cut the input short.
                1 => if (input.len > 0) {
                    input = input[0..random.uintLessThan(usize, input.len)];
                },
                // Add one octet.
                2 => if (input.len < buffer.len) {
                    buffer[input.len] = random.int(u8);
                    input = buffer[0 .. input.len + 1];
                },
                // Wrap the input in a described constructor.
                else => if (input.len + 3 <= buffer.len) {
                    std.mem.copyBackwards(u8, buffer[3 .. input.len + 3], input);
                    buffer[0] = 0x00;
                    buffer[1] = 0x53;
                    buffer[2] = random.int(u8);
                    input = buffer[0 .. input.len + 3];
                },
            }
        }

        if (try checkDecoder(gpa, input)) decoded += 1;
    }

    // The seeded run decodes about 2600 of the 20000 inputs. A much smaller
    // number means that the generator stopped making legal input, and that
    // the test no longer reaches the body of the decoder.
    try testing.expect(decoded > 1000);
}

test "the decoder survives random bytes" {
    const gpa = testing.allocator;
    var prng: std.Random.DefaultPrng = .init(0x31303030);
    const random = prng.random();

    var buffer: [128]u8 = undefined;
    for (0..20000) |_| {
        const len = random.uintLessThan(usize, buffer.len);
        random.bytes(buffer[0..len]);
        _ = try checkDecoder(gpa, buffer[0..len]);
    }
}
