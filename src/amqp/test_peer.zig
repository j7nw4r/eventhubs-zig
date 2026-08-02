//! The test doubles that drive a connection with a scripted remote peer.
//!
//! Every link test needs the same three parts: a reader that gives scripted
//! octets, a writer that counts the frames of the code under test, and a parser
//! that reads those frames back. This file holds all three, so that the sender
//! and the receiver share one harness.
//!
//! The file holds no test of its own. A test file imports it and builds a
//! `Peer`.

const std = @import("std");

const connection_mod = @import("connection.zig");
const framing = @import("framing.zig");
const transport = @import("transport.zig");

const Allocator = std.mem.Allocator;
const Frame = framing.Frame;
const Io = std.Io;
const Reader = std.Io.Reader;
const Writer = std.Io.Writer;

/// The number of seconds that a test may take before the watchdog stops the
/// process. A test that blocks must fail, and it must never hold CI.
pub const watchdog_seconds: i64 = 20;

/// One hold point in the script of the remote peer.
///
/// The reader gives no octet from the offset `at` until the code under test
/// flushed `after_frames` frames. A test uses the gate to make the answer of
/// the remote peer follow the frame that asks for it.
pub const Gate = struct {
    /// The offset in the script where the reader stops.
    at: usize,
    /// The number of frames that opens the gate.
    after_frames: u32,
    /// The event that opens the gate.
    event: Io.Event = .unset,

    /// An `after_frames` value that no frame count reaches. A test opens such
    /// a gate itself, at the point where the code under test is ready for the
    /// next frame of the remote peer.
    pub const manual: u32 = std.math.maxInt(u32);
};

/// A reader that gives scripted octets to the code under test.
///
/// The gates must run in order of `at`. After the last octet the reader waits,
/// so that a test can hold the demultiplexer inside a read. A cancel request
/// ends that wait.
pub const ScriptReader = struct {
    interface: Reader,
    io: Io,
    script: []const u8,
    pos: usize,
    gates: []Gate,
    next_gate: usize,
    /// The event that ends the wait after the last octet.
    release: Io.Event,
    buf: [4096]u8,

    const vtable: Reader.VTable = .{ .stream = stream };

    pub fn init(io: Io, script: []const u8, gates: []Gate) ScriptReader {
        return .{
            .interface = .{ .vtable = &vtable, .buffer = &.{}, .seek = 0, .end = 0 },
            .io = io,
            .script = script,
            .pos = 0,
            .gates = gates,
            .next_gate = 0,
            .release = .unset,
            .buf = undefined,
        };
    }

    /// Points the reader at its own buffer. Call it after the value reached
    /// its final address.
    pub fn ready(self: *ScriptReader) *Reader {
        self.interface.buffer = &self.buf;
        return &self.interface;
    }

    fn stream(r: *Reader, w: *Writer, limit: Io.Limit) Reader.StreamError!usize {
        const self: *ScriptReader = @alignCast(@fieldParentPtr("interface", r));

        while (self.next_gate < self.gates.len and self.pos >= self.gates[self.next_gate].at) {
            self.gates[self.next_gate].event.wait(self.io) catch return error.ReadFailed;
            self.next_gate += 1;
        }

        const end = if (self.next_gate < self.gates.len)
            self.gates[self.next_gate].at
        else
            self.script.len;

        if (self.pos == end) {
            self.release.wait(self.io) catch return error.ReadFailed;
            return error.EndOfStream;
        }

        const n = try w.write(limit.sliceConst(self.script[self.pos..end]));
        self.pos += n;
        return n;
    }
};

/// A writer that counts the frames that pass through it and opens the gates of
/// a `ScriptReader`.
///
/// Every write path of the connection writes one frame and then flushes, so
/// each flush carries one whole frame.
pub const FrameCounter = struct {
    interface: Writer,
    io: Io,
    inner: *Writer,
    /// The number of frames that this writer saw.
    frames: std.atomic.Value(u32),
    gates: []Gate,

    const vtable: Writer.VTable = .{ .drain = drain, .flush = flush };

    pub fn init(io: Io, inner: *Writer, gates: []Gate, buf: []u8) FrameCounter {
        return .{
            .interface = .{ .vtable = &vtable, .buffer = buf },
            .io = io,
            .inner = inner,
            .frames = .init(0),
            .gates = gates,
        };
    }

    fn drain(w: *Writer, data: []const []const u8, splat: usize) Writer.Error!usize {
        const self: *FrameCounter = @alignCast(@fieldParentPtr("interface", w));
        try self.inner.writeAll(w.buffered());
        _ = w.consumeAll();
        return self.inner.writeSplat(data, splat);
    }

    fn flush(w: *Writer) Writer.Error!void {
        const self: *FrameCounter = @alignCast(@fieldParentPtr("interface", w));
        const bytes = w.buffered();
        if (bytes.len != 0) {
            const count = self.frames.fetchAdd(1, .monotonic) + 1;
            try self.inner.writeAll(bytes);
            _ = w.consumeAll();
            for (self.gates) |*gate| {
                if (gate.after_frames == count) gate.event.set(self.io);
            }
        }
        try self.inner.flush();
    }
};

/// The test double stack: a scripted reader, a frame counter, and the sink of
/// `MockTransport`.
///
/// The value must not move after `ready`.
pub const Peer = struct {
    io: Io,
    reader: ScriptReader,
    mock: transport.MockTransport,
    counter: FrameCounter,
    gates: []Gate,
    counter_buf: [4096]u8,

    pub fn init(gpa: Allocator, io: Io, script: []const u8, gates: []Gate) Peer {
        return .{
            .io = io,
            .reader = .init(io, script, gates),
            .mock = .init(gpa, ""),
            .counter = undefined,
            .gates = gates,
            .counter_buf = undefined,
        };
    }

    /// Finishes the setup and returns the stream for `Connection.open`.
    pub fn ready(self: *Peer) connection_mod.Stream {
        self.counter = .init(self.io, self.mock.writer(), self.gates, &self.counter_buf);
        return .{ .reader = self.reader.ready(), .writer = &self.counter.interface };
    }

    pub fn deinit(self: *Peer) void {
        self.mock.deinit();
    }

    /// Returns every octet that the code under test wrote.
    pub fn sent(self: *Peer) []const u8 {
        return self.mock.sent();
    }

    /// Returns the number of frames that the code under test wrote.
    pub fn frames(self: *Peer) u32 {
        return self.counter.frames.load(.monotonic);
    }
};

/// Stops the process when a test blocks. A hung test suite is worse than a
/// failing one.
fn watchdogRun(io: Io, seconds: i64) void {
    io.sleep(.fromSeconds(seconds), .awake) catch return;
    std.debug.panic("the test did not finish within {d} seconds", .{seconds});
}

/// Starts the watchdog of one test.
pub fn startWatchdog(io: Io) ?Io.Future(void) {
    return io.concurrent(watchdogRun, .{ io, watchdog_seconds }) catch null;
}

/// Stops the watchdog of one test.
pub fn stopWatchdog(io: Io, future: *?Io.Future(void)) void {
    if (future.*) |*f| f.cancel(io);
}

/// Appends one frame that the remote peer sends.
pub fn appendFrame(
    sink: *Writer.Allocating,
    channel: u16,
    body: framing.Body,
    payload: []const u8,
) !void {
    try framing.writeFrame(
        &sink.writer,
        channel,
        body,
        payload,
        connection_mod.default_max_frame_size,
    );
}

/// The open frame that the remote peer sends.
pub fn scriptOpen(sink: *Writer.Allocating, max_frame_size: ?u32) !void {
    try appendFrame(sink, 0, .{ .open = .{
        .container_id = "the-peer",
        .max_frame_size = max_frame_size,
    } }, "");
}

/// The begin frame that the remote peer answers with.
pub fn remoteBegin(channel: u16) framing.Body {
    return .{ .begin = .{
        .remote_channel = channel,
        .next_outgoing_id = 0,
        .incoming_window = 100,
        .outgoing_window = 200,
    } };
}

/// Returns the octets of frame `index` of `bytes`, header and all.
///
/// Section 2.3.1 puts the size of the frame in the first four octets, so the
/// walk needs no decoder. A golden byte test uses this to compare one frame.
pub fn frameBytes(bytes: []const u8, index: usize) []const u8 {
    var offset: usize = 0;
    var seen: usize = 0;
    while (offset + 4 <= bytes.len) {
        const size = std.mem.readInt(u32, bytes[offset..][0..4], .big);
        const end = offset + size;
        if (seen == index) return bytes[offset..end];
        seen += 1;
        offset = end;
    }
    return &.{};
}

/// One frame that the code under test wrote, with its own copy of the payload.
pub const SentFrame = struct {
    frame: Frame,
    payload: []u8,
};

/// The frames that the code under test wrote.
///
/// Each frame keeps its own copy of the payload, so a test can compare the
/// whole sequence of a split delivery.
pub const SentFrames = struct {
    gpa: Allocator,
    items: std.ArrayListUnmanaged(SentFrame),

    pub fn parse(gpa: Allocator, bytes: []const u8) !SentFrames {
        var self: SentFrames = .{ .gpa = gpa, .items = .empty };
        errdefer self.deinit();

        const buf = try gpa.alloc(u8, connection_mod.default_max_frame_size);
        defer gpa.free(buf);

        var reader: Reader = .fixed(bytes);
        while (true) {
            const frame = framing.readFrame(
                gpa,
                &reader,
                buf,
                connection_mod.default_max_frame_size,
            ) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            errdefer frame.deinit();
            const payload = try gpa.dupe(u8, frame.payload);
            errdefer gpa.free(payload);
            try self.items.append(gpa, .{ .frame = frame, .payload = payload });
        }
        return self;
    }

    pub fn deinit(self: *SentFrames) void {
        for (self.items.items) |item| {
            item.frame.deinit();
            self.gpa.free(item.payload);
        }
        self.items.deinit(self.gpa);
    }

    /// Returns the first frame whose body holds `tag`, or null.
    pub fn find(self: *SentFrames, comptime tag: std.meta.Tag(framing.Body)) ?Frame {
        for (self.items.items) |item| {
            const body = item.frame.body orelse continue;
            if (body == tag) return item.frame;
        }
        return null;
    }

    /// Returns the number of frames whose body holds `tag`.
    pub fn count(self: *SentFrames, comptime tag: std.meta.Tag(framing.Body)) usize {
        var found: usize = 0;
        for (self.items.items) |item| {
            const body = item.frame.body orelse continue;
            if (body == tag) found += 1;
        }
        return found;
    }

    /// Writes every frame whose body holds `tag` into `out`, in order.
    pub fn all(
        self: *SentFrames,
        comptime tag: std.meta.Tag(framing.Body),
        out: *std.ArrayListUnmanaged(SentFrame),
    ) !void {
        for (self.items.items) |item| {
            const body = item.frame.body orelse continue;
            if (body == tag) try out.append(self.gpa, item);
        }
    }
};
