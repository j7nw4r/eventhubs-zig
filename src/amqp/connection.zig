//! The AMQP 1.0 connection.
//!
//! This file owns the socket and the concurrency model. It negotiates the open
//! frame, it demultiplexes incoming frames to the sessions, it sends the
//! heartbeat, it serializes the outgoing frames, and it runs the close
//! handshake.
//!
//! Specification:
//! OASIS AMQP Version 1.0 Part 2: Transport, sections 2.4.1, 2.4.3, 2.4.5,
//! 2.7.1, and 2.7.9.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html
//!
//! # The tasks
//!
//! `open` starts two tasks in one `std.Io.Group`.
//!
//! The demultiplexer task owns the read side of the stream. It is the only
//! code that reads a frame after `open` returns. It answers the connection
//! performatives itself, and it pushes every other frame to the queue that a
//! session registered for the channel.
//!
//! The heartbeat task sends one empty frame every half of the idle timeout of
//! the remote peer. Section 2.4.5 gives the rule. `open` starts the task only
//! when the remote peer asks for an idle timeout.
//!
//! `deinit` cancels both tasks through the group, and it waits for them.
//!
//! # The locks
//!
//! `write_mutex` serializes the frame writes. The demultiplexer never holds it
//! across a read, so the heartbeat task and a caller never wait for a read.
//!
//! `channels_mutex` guards the channel table. Every holder of that lock does a
//! short table operation and then unlocks. No holder blocks, so no task waits
//! for a queue while it holds the table.
//!
//! # The frame lifetime
//!
//! `readFrame` gives a frame whose body lives in an arena that the frame owns,
//! and whose payload borrows the read buffer of the caller. The demultiplexer
//! reads every frame into one buffer, so a queued frame would lose its payload
//! as soon as the next read starts. The demultiplexer therefore copies the
//! payload into the arena of the frame before it pushes the frame to a queue.
//! After that copy the frame owns all of its memory, and it outlives the read
//! buffer.
//!
//! The reader of a queue owns each frame that it takes, and it calls
//! `Frame.deinit` on it.
//!
//! # The channel queues
//!
//! A session gives the memory of its queue to `registerChannel`, and that
//! memory must stay valid until `Connection.deinit` returns. The connection
//! closes every registered queue when it ends, so a blocked reader wakes with
//! `error.Closed` and then reads `failure` for the reason.
//!
//! A queue can still hold frames after the connection ends, because `get`
//! gives the buffered elements before it reports `error.Closed`. The owner of
//! the queue drains it and frees those frames.
//!
//! # The write buffer
//!
//! Every write path writes one frame and then flushes. The flush puts the
//! octets on the wire, and it also makes each frame a whole unit for a writer
//! that watches the stream.

const std = @import("std");

const framing = @import("framing.zig");
const performatives = @import("performatives.zig");
const transport = @import("transport.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Frame = framing.Frame;
const Io = std.Io;
const MapEntry = types.MapEntry;
const Reader = std.Io.Reader;
const Writer = std.Io.Writer;

// -------------------------------------------------------------------------
// The constants
// -------------------------------------------------------------------------

/// The maximum frame size that `Options` asks for, in octets.
pub const default_max_frame_size: u32 = 65536;

/// The largest channel number that `Options` accepts.
pub const default_channel_max: u16 = 255;

/// The idle timeout that `Options` asks the remote peer to respect, in
/// milliseconds.
pub const default_idle_time_out_ms: u32 = 60000;

/// The number of characters in a generated container id. The id holds the
/// hexadecimal form of 16 random octets.
pub const container_id_len: usize = 32;

/// The queue of one channel. A session gives one of these to
/// `registerChannel`, and the demultiplexer pushes the frames of the channel
/// to it.
pub const FrameQueue = Io.Queue(Frame);

// -------------------------------------------------------------------------
// The errors
// -------------------------------------------------------------------------

/// The reason that a connection ended. The value is sticky: the first reason
/// wins, and every later caller reads the same one.
pub const Error = error{
    /// The two peers closed the connection, and neither one reported an
    /// error. `deinit` also gives this reason.
    ConnectionClosed,
    /// The remote peer closed the connection with an error. Read `failure`
    /// for the condition symbol and the description text.
    RemoteError,
    /// A read or a write on the stream failed. Read `failure` for the name of
    /// the error.
    TransportFailure,
    /// The remote peer broke the protocol. It sent a second open, or a frame
    /// on a channel that no session registered, or octets that do not decode.
    /// Read `failure` for the detail.
    ProtocolError,
};

/// The errors of `Connection.open`.
pub const OpenError = Error || framing.ReadFrameError || framing.WriteFrameError ||
    Allocator.Error || Io.ConcurrentError || Io.Cancelable;

/// The errors of `Connection.send`.
pub const SendError = Error || framing.WriteFrameError || Io.Cancelable;

/// The errors of `Connection.close`.
pub const CloseError = SendError || Io.Timeout.Error;

/// The errors of `Connection.registerChannel`.
pub const RegisterError = Error || Allocator.Error || Io.Cancelable || error{
    /// Another session already holds the channel.
    ChannelInUse,
};

/// The reason that a connection ended, with the text that came with it.
///
/// The two slices live until `Connection.deinit` returns.
pub const Failure = struct {
    /// The reason.
    err: Error,
    /// The condition symbol of the remote peer, or null when the reason is
    /// local.
    condition: ?[]const u8,
    /// The description text of the remote peer, or the name of the local
    /// error, or null.
    description: ?[]const u8,
};

// -------------------------------------------------------------------------
// The stream
// -------------------------------------------------------------------------

/// The read side and the write side of one connected byte stream.
///
/// The connection does not own the stream. Close the transport after
/// `Connection.deinit` returns.
pub const Stream = struct {
    /// The reader that gives the octets of the remote peer.
    reader: *Reader,
    /// The writer that puts the octets on the wire when it flushes.
    writer: *Writer,

    /// Returns the stream of a `Transport` that finished its handshake.
    pub fn of(t: *transport.Transport) Stream {
        return .{ .reader = t.reader(), .writer = t.writer() };
    }
};

// -------------------------------------------------------------------------
// The options
// -------------------------------------------------------------------------

/// The arguments of `Connection.open` that the open frame carries.
pub const Options = struct {
    /// The name of the host that the remote peer serves.
    hostname: ?[]const u8 = null,
    /// The container id of this peer. `open` generates a random one when this
    /// field is null.
    container_id: ?[]const u8 = null,
    /// The largest frame that this peer accepts, in octets. The value must be
    /// at least `framing.min_max_frame_size`.
    max_frame_size: u32 = default_max_frame_size,
    /// The largest channel number that this peer accepts.
    channel_max: u16 = default_channel_max,
    /// The idle timeout that this peer asks the remote peer to respect, in
    /// milliseconds. Zero asks for no idle timeout.
    idle_time_out_ms: u32 = default_idle_time_out_ms,
    /// The properties of the connection.
    ///
    /// This module names no property. The caller supplies every entry, and the
    /// entries live until `open` returns.
    properties: ?[]const MapEntry = null,
    /// The time that `close` waits for the close frame of the remote peer.
    close_timeout: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } },
};

// -------------------------------------------------------------------------
// The connection
// -------------------------------------------------------------------------

/// One AMQP 1.0 connection.
///
/// Build it with `open` and free it with `deinit`. The value must not move,
/// because both tasks hold a pointer to it, so `open` puts it on the heap.
pub const Connection = struct {
    gpa: Allocator,
    io: Io,
    reader: *Reader,
    writer: *Writer,

    /// The container id that this peer sent. The connection owns the memory.
    container_id: []const u8,
    /// The container id that the remote peer sent, or null. The connection
    /// owns the memory.
    remote_container_id: ?[]const u8,

    /// The largest frame that this peer accepts, in octets.
    local_max_frame_size: u32,
    /// The largest frame that the remote peer accepts, in octets.
    remote_max_frame_size: u32,
    /// The largest channel number that the remote peer accepts.
    remote_channel_max: u16,
    /// The idle timeout of the remote peer in milliseconds, or null when the
    /// remote peer asked for none.
    remote_idle_time_out_ms: ?u32,

    /// The buffer of the demultiplexer. `open` reads the remote open frame
    /// into it, and then the demultiplexer owns it.
    read_buf: []u8,

    /// The lock that serializes the frame writes.
    write_mutex: Io.Mutex,
    /// True after a write failed. `write_mutex` guards it.
    write_dead: bool,
    /// True after this peer sent its close frame. `write_mutex` guards it.
    close_sent: bool,

    /// The lock that guards `channels`.
    channels_mutex: Io.Mutex,
    /// The queue of each channel that a session registered.
    channels: std.AutoHashMapUnmanaged(u16, *FrameQueue),

    /// The step of the terminal state. Read the note on `fail`.
    state: std.atomic.Value(u8),
    failure_err: Error,
    failure_condition: ?[]const u8,
    failure_description: ?[]const u8,
    /// The event that the connection sets when it reaches its terminal state.
    done: Io.Event,

    /// The group that holds the demultiplexer task and the heartbeat task.
    group: Io.Group,

    /// The time that `close` waits for the close frame of the remote peer.
    close_timeout: Io.Timeout,

    /// The connection runs.
    const state_running: u8 = 0;
    /// One task won the race to write the terminal state, and it writes now.
    const state_claimed: u8 = 1;
    /// The terminal state is readable.
    const state_ended: u8 = 2;

    // ---------------------------------------------------------------------
    // The open negotiation
    // ---------------------------------------------------------------------

    /// Opens the connection.
    ///
    /// The call sends the open frame of section 2.7.1, reads the open frame of
    /// the remote peer, and starts the two tasks.
    ///
    /// Section 2.4.1 fixes the maximum frame size at `min_max_frame_size`
    /// until the peers exchange their open frames, so a large properties map
    /// makes the call return `error.FrameTooLarge`.
    ///
    /// The call reads with `options.max_frame_size` as its limit, because the
    /// remote peer learns that size from the open frame that this call sends
    /// first.
    ///
    /// The result points to heap memory that the call took from `gpa`. Free it
    /// with `deinit`, and then close the transport.
    pub fn open(
        gpa: Allocator,
        io: Io,
        stream: Stream,
        options: Options,
    ) OpenError!*Connection {
        // `readFrame` needs a buffer of at least the size that it accepts, and
        // section 2.8.19 puts the floor at MIN-MAX-FRAME-SIZE.
        const max_frame_size = @max(options.max_frame_size, framing.min_max_frame_size);

        var id_buf: [container_id_len]u8 = undefined;
        const container_id = try gpa.dupe(
            u8,
            options.container_id orelse generateContainerId(io, &id_buf),
        );
        errdefer gpa.free(container_id);

        const read_buf = try gpa.alloc(u8, max_frame_size);
        errdefer gpa.free(read_buf);

        const self = try gpa.create(Connection);
        errdefer gpa.destroy(self);

        self.* = .{
            .gpa = gpa,
            .io = io,
            .reader = stream.reader,
            .writer = stream.writer,
            .container_id = container_id,
            .remote_container_id = null,
            .local_max_frame_size = max_frame_size,
            // Section 2.7.1 gives 4294967295 as the default maximum frame size
            // of a peer that sends no value.
            .remote_max_frame_size = std.math.maxInt(u32),
            .remote_channel_max = std.math.maxInt(u16),
            .remote_idle_time_out_ms = null,
            .read_buf = read_buf,
            .write_mutex = .init,
            .write_dead = false,
            .close_sent = false,
            .channels_mutex = .init,
            .channels = .empty,
            .state = .init(state_running),
            .failure_err = error.ConnectionClosed,
            .failure_condition = null,
            .failure_description = null,
            .done = .unset,
            .group = .init,
            .close_timeout = options.close_timeout,
        };

        // Section 2.4.1: the maximum frame size is MIN-MAX-FRAME-SIZE until
        // the two peers exchange their open frames.
        try framing.writeFrame(self.writer, 0, .{ .open = .{
            .container_id = container_id,
            .hostname = options.hostname,
            .max_frame_size = max_frame_size,
            .channel_max = options.channel_max,
            .idle_time_out = options.idle_time_out_ms,
            .properties = options.properties,
        } }, "", framing.min_max_frame_size);
        try self.writer.flush();

        // The frame borrows `read_buf` for its payload, so it must die before
        // the demultiplexer starts and reuses the buffer.
        {
            var frame = try self.readRemoteOpen();
            defer frame.deinit();

            const remote = frame.body.?.open;
            if (remote.container_id) |id| {
                self.remote_container_id = try gpa.dupe(u8, id);
            }
            if (remote.max_frame_size) |size| self.remote_max_frame_size = size;
            if (remote.channel_max) |max| self.remote_channel_max = max;
            self.remote_idle_time_out_ms = remote.idle_time_out;
        }
        errdefer if (self.remote_container_id) |id| gpa.free(id);

        try self.group.concurrent(io, demultiplex, .{self});
        errdefer self.group.cancel(io);

        // Section 2.4.5: a peer that asks for no idle timeout needs no
        // heartbeat, and neither does a peer that asks for zero.
        if (self.remote_idle_time_out_ms) |timeout_ms| {
            if (timeout_ms > 0) {
                try self.group.concurrent(io, heartbeat, .{ self, @max(timeout_ms / 2, 1) });
            }
        }
        return self;
    }

    /// Reads frames until the open frame of the remote peer arrives.
    ///
    /// Section 2.4.5 lets a peer send an empty frame at any time, so the loop
    /// steps over one. Any other frame before the open breaks section 2.4.1.
    fn readRemoteOpen(self: *Connection) OpenError!Frame {
        while (true) {
            var frame = try framing.readFrame(
                self.gpa,
                self.reader,
                self.read_buf,
                self.local_max_frame_size,
            );
            errdefer frame.deinit();

            if (frame.frame_type != .amqp) return error.ProtocolError;
            const body = frame.body orelse {
                frame.deinit();
                continue;
            };
            if (body != .open) return error.ProtocolError;
            if (frame.channel != 0) return error.ProtocolError;
            return frame;
        }
    }

    /// Frees the connection.
    ///
    /// The call cancels both tasks, waits for them, closes every registered
    /// queue, and frees every allocation, including `self`. It does not close
    /// the transport, because the connection does not own it.
    pub fn deinit(self: *Connection) void {
        const gpa = self.gpa;
        // Fail first, so that a caller who blocks on a queue wakes before the
        // cancel waits for the tasks.
        self.fail(error.ConnectionClosed, null, null);
        self.group.cancel(self.io);

        self.channels.deinit(gpa);
        gpa.free(self.read_buf);
        gpa.free(self.container_id);
        if (self.remote_container_id) |id| gpa.free(id);
        if (self.failure_condition) |c| gpa.free(c);
        if (self.failure_description) |d| gpa.free(d);
        gpa.destroy(self);
    }

    // ---------------------------------------------------------------------
    // The terminal state
    // ---------------------------------------------------------------------

    /// Returns the reason that the connection ended, or null while it runs.
    ///
    /// The reason is sticky. The first task to end the connection writes it,
    /// and every later caller reads the same value.
    pub fn failure(self: *Connection) ?Failure {
        if (self.state.load(.acquire) != state_ended) return null;
        return .{
            .err = self.failure_err,
            .condition = self.failure_condition,
            .description = self.failure_description,
        };
    }

    /// Ends the connection with a reason, and wakes every blocked caller.
    ///
    /// The function takes the first reason and drops every later one. It moves
    /// the state in two steps: the winner claims the state, fills the fields,
    /// and only then publishes them, so a reader that sees `state_ended` sees
    /// the fields too.
    fn fail(
        self: *Connection,
        err: Error,
        condition: ?[]const u8,
        description: ?[]const u8,
    ) void {
        if (self.state.cmpxchgStrong(
            state_running,
            state_claimed,
            .acq_rel,
            .acquire,
        ) != null) return;

        self.failure_err = err;
        // A failed copy leaves the text out. The reason still reaches the
        // caller, and the connection is already ending.
        if (condition) |c| self.failure_condition = self.gpa.dupe(u8, c) catch null;
        if (description) |d| self.failure_description = self.gpa.dupe(u8, d) catch null;
        self.state.store(state_ended, .release);

        self.done.set(self.io);
        self.closeChannels();
    }

    /// Closes every registered queue, so that a blocked reader wakes.
    fn closeChannels(self: *Connection) void {
        // The lock is uncancelable, because a canceled task must still wake
        // the readers of the queues.
        self.channels_mutex.lockUncancelable(self.io);
        defer self.channels_mutex.unlock(self.io);

        var it = self.channels.valueIterator();
        while (it.next()) |queue| queue.*.close(self.io);
    }

    // ---------------------------------------------------------------------
    // The channel table
    // ---------------------------------------------------------------------

    /// Registers the queue that receives the frames of `channel`.
    ///
    /// The memory of `queue` must stay valid until `deinit` returns, because
    /// the demultiplexer can hold a pointer to it while it pushes a frame.
    pub fn registerChannel(
        self: *Connection,
        channel: u16,
        queue: *FrameQueue,
    ) RegisterError!void {
        if (self.failure()) |f| return f.err;

        try self.channels_mutex.lock(self.io);
        defer self.channels_mutex.unlock(self.io);

        const entry = try self.channels.getOrPut(self.gpa, channel);
        if (entry.found_existing) return error.ChannelInUse;
        entry.value_ptr.* = queue;

        // The connection can end between the test above and this insert.
        // `fail` publishes the reason before it closes the queues, and it
        // waits for this lock, so this second test always sees that reason.
        // Close the new queue here, or its reader waits for a frame that no
        // task can send.
        if (self.failure()) |f| {
            _ = self.channels.remove(channel);
            queue.close(self.io);
            return f.err;
        }
    }

    /// Removes the queue of `channel` and closes it.
    ///
    /// The demultiplexer can still hold the pointer, so the memory of the
    /// queue must stay valid until `deinit` returns. A push to the closed
    /// queue reports `error.Closed`, and the demultiplexer drops the frame.
    pub fn unregisterChannel(self: *Connection, channel: u16) void {
        const queue = blk: {
            self.channels_mutex.lockUncancelable(self.io);
            defer self.channels_mutex.unlock(self.io);
            break :blk self.channels.fetchRemove(channel) orelse return;
        };
        queue.value.close(self.io);
    }

    /// Returns the queue of `channel`, or null when no session registered one.
    fn lookupChannel(self: *Connection, channel: u16) ?*FrameQueue {
        self.channels_mutex.lockUncancelable(self.io);
        defer self.channels_mutex.unlock(self.io);
        return self.channels.get(channel);
    }

    // ---------------------------------------------------------------------
    // The write path
    // ---------------------------------------------------------------------

    /// Writes one frame to the stream and flushes.
    ///
    /// The call blocks until the write lock is free. It never waits for a
    /// read, because no code holds the write lock across a read.
    pub fn send(
        self: *Connection,
        channel: u16,
        body: framing.Body,
        payload: []const u8,
    ) SendError!void {
        if (self.failure()) |f| return f.err;

        try self.write_mutex.lock(self.io);
        defer self.write_mutex.unlock(self.io);

        if (self.failure()) |f| return f.err;
        if (self.write_dead) return error.TransportFailure;
        return self.writeLocked(channel, body, payload);
    }

    /// Writes one frame. The caller holds `write_mutex`.
    fn writeLocked(
        self: *Connection,
        channel: u16,
        body: framing.Body,
        payload: []const u8,
    ) framing.WriteFrameError!void {
        errdefer self.write_dead = true;
        try framing.writeFrame(self.writer, channel, body, payload, self.remote_max_frame_size);
        try self.writer.flush();
    }

    /// Sends the close frame of section 2.7.9 once.
    ///
    /// The lock is uncancelable, because the close handshake must still run
    /// after a cancel request reached the task.
    fn sendClose(
        self: *Connection,
        condition: ?performatives.ErrorCondition,
    ) framing.WriteFrameError!void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);

        if (self.write_dead or self.close_sent) return;
        self.close_sent = true;
        return self.writeLocked(0, .{ .close = .{ .error_condition = condition } }, "");
    }

    // ---------------------------------------------------------------------
    // The close handshake
    // ---------------------------------------------------------------------

    /// Runs the close handshake of section 2.4.3.
    ///
    /// The call sends the close frame, and then it waits for the demultiplexer
    /// to read the close frame of the remote peer. Give `condition` when this
    /// peer closes because of an error, and null for a clean close.
    ///
    /// The call returns `error.Timeout` when the remote peer sends no close
    /// within `Options.close_timeout`. It ends the connection either way, so a
    /// blocked reader always wakes.
    ///
    /// The call is safe from more than one task. The second caller sends no
    /// second frame, and it waits for the same answer.
    pub fn close(
        self: *Connection,
        condition: ?performatives.ErrorCondition,
    ) CloseError!void {
        self.sendClose(condition) catch |err| {
            self.fail(error.TransportFailure, null, @errorName(err));
            return err;
        };

        self.done.waitTimeout(self.io, self.close_timeout) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Timeout => {
                // The remote peer never answered. End the connection anyway,
                // so that no caller waits for a frame that cannot arrive.
                self.fail(error.ConnectionClosed, null, "the remote peer sent no close frame");
                return error.Timeout;
            },
        };

        const f = self.failure().?;
        if (f.err == error.ConnectionClosed) return;
        return f.err;
    }

    // ---------------------------------------------------------------------
    // The demultiplexer task
    // ---------------------------------------------------------------------

    /// Reads frames and routes them until the connection ends.
    ///
    /// The task owns `read_buf`. It gives each frame its own copy of the
    /// payload before it pushes the frame, so the next read cannot overwrite a
    /// frame that a session still holds.
    fn demultiplex(self: *Connection) void {
        while (true) {
            const frame = framing.readFrame(
                self.gpa,
                self.reader,
                self.read_buf,
                self.local_max_frame_size,
            ) catch |err| {
                switch (err) {
                    error.ReadFailed, error.EndOfStream => self.fail(
                        error.TransportFailure,
                        null,
                        @errorName(err),
                    ),
                    else => self.fail(error.ProtocolError, null, @errorName(err)),
                }
                return;
            };
            switch (self.route(frame)) {
                .keep_reading => {},
                .stop => return,
            }
        }
    }

    /// What the demultiplexer does after one frame.
    const Routing = enum { keep_reading, stop };

    /// Routes one frame, and takes its memory.
    ///
    /// The function frees the frame, or it gives the frame to a queue. The
    /// caller never touches the frame again.
    fn route(self: *Connection, frame: Frame) Routing {
        var owned = frame;

        // Section 5.3.1: the SASL layer ends before the AMQP layer starts.
        if (owned.frame_type != .amqp) {
            owned.deinit();
            self.fail(error.ProtocolError, null, "a SASL frame arrived after the handshake");
            return .stop;
        }

        // Section 2.4.5: an empty frame is the heartbeat of the remote peer.
        const body = owned.body orelse {
            owned.deinit();
            return .keep_reading;
        };

        switch (body) {
            .open => {
                owned.deinit();
                self.fail(error.ProtocolError, null, "a second open frame arrived");
                return .stop;
            },
            .close => |performative| {
                self.handleRemoteClose(performative);
                owned.deinit();
                return .stop;
            },
            else => {},
        }

        const queue = self.lookupChannel(owned.channel) orelse {
            owned.deinit();
            self.fail(
                error.ProtocolError,
                null,
                "a frame arrived on a channel that no session registered",
            );
            return .stop;
        };

        // The payload borrows `read_buf`, and the next read overwrites it, so
        // the frame takes its own copy before it leaves this task.
        adoptPayload(&owned) catch {
            owned.deinit();
            self.fail(error.ProtocolError, null, @errorName(error.OutOfMemory));
            return .stop;
        };

        queue.putOne(self.io, owned) catch |err| {
            owned.deinit();
            switch (err) {
                // The session unregistered its channel, or the connection
                // already ended. Neither one is a protocol error.
                error.Closed => return if (self.failure() == null) .keep_reading else .stop,
                error.Canceled => return .stop,
            }
        };
        return .keep_reading;
    }

    /// Answers the close frame of the remote peer and ends the connection.
    fn handleRemoteClose(self: *Connection, performative: performatives.Close) void {
        // Section 2.4.3: a peer answers a close frame with a close frame.
        self.sendClose(null) catch {};

        const condition_error = performative.error_condition orelse {
            self.fail(error.ConnectionClosed, null, null);
            return;
        };
        const condition = if (condition_error.condition) |symbol| symbol.text else null;
        self.fail(error.RemoteError, condition, condition_error.description);
    }

    // ---------------------------------------------------------------------
    // The heartbeat task
    // ---------------------------------------------------------------------

    /// Sends one empty frame every `interval_ms` until the connection ends.
    ///
    /// Section 2.4.5 makes the empty frame the traffic that holds the idle
    /// timeout of the remote peer open.
    fn heartbeat(self: *Connection, interval_ms: u32) void {
        while (true) {
            // A cancel request ends the sleep, and the task stops.
            self.io.sleep(.fromMilliseconds(interval_ms), .awake) catch return;
            if (self.failure() != null) return;

            self.write_mutex.lock(self.io) catch return;
            defer self.write_mutex.unlock(self.io);

            // The transport is gone, or this peer already said goodbye.
            if (self.write_dead or self.close_sent) return;
            framing.writeEmptyFrame(self.writer, 0) catch {
                self.write_dead = true;
                return;
            };
            self.writer.flush() catch {
                self.write_dead = true;
                return;
            };
        }
    }
};

// -------------------------------------------------------------------------
// The helpers
// -------------------------------------------------------------------------

/// Moves the payload of a frame into the arena of the frame.
///
/// `readFrame` leaves the payload in the buffer of the caller. This copy makes
/// the frame own every octet, so it can outlive the buffer.
fn adoptPayload(frame: *Frame) Allocator.Error!void {
    if (frame.payload.len == 0) return;

    var arena = frame.arena_state.promote(frame.child);
    const copy = arena.allocator().dupe(u8, frame.payload);
    frame.arena_state = arena.state;
    frame.payload = try copy;
}

/// Writes a random container id into `buf` and returns it.
///
/// The id is the hexadecimal form of 16 random octets, which is short enough
/// for a frame and wide enough that two containers do not collide.
fn generateContainerId(io: Io, buf: *[container_id_len]u8) []const u8 {
    var raw: [container_id_len / 2]u8 = undefined;
    io.random(&raw);
    buf.* = std.fmt.bytesToHex(raw, .lower);
    return buf;
}

// -------------------------------------------------------------------------
// The test doubles
// -------------------------------------------------------------------------

const testing = std.testing;

/// The number of seconds that a test may take before the watchdog stops the
/// process. A test that blocks must fail, and it must never hold CI.
const watchdog_seconds: i64 = 20;

/// The octets of one empty frame. Sections 2.3.1 and 2.4.5.
const empty_frame_bytes: [framing.frame_header_size]u8 =
    .{ 0, 0, 0, 8, framing.min_data_offset, 0, 0, 0 };

/// A reader that gives scripted octets to the code under test.
///
/// The reader stops at `gate_at` until something sets `gate`, and it tells the
/// test that it waits there with `at_gate`. After the last octet it either
/// reports the end of the stream or it waits, so that a test can hold the
/// demultiplexer inside a read. A cancel request ends that wait, and the read
/// then fails.
const ScriptReader = struct {
    interface: Reader,
    io: Io,
    script: []const u8,
    pos: usize,
    tail: Tail,
    gate_at: usize,
    /// The event that opens the gate.
    gate: Io.Event,
    /// The event that the reader sets when it reaches the gate.
    at_gate: Io.Event,
    /// The event that the reader sets when it runs out of octets.
    blocked: Io.Event,
    /// The event that ends the wait after the last octet.
    release: Io.Event,
    buf: [4096]u8,

    /// What the reader does after the last octet of the script.
    const Tail = enum { end_of_stream, wait };

    const vtable: Reader.VTable = .{ .stream = stream };

    fn init(io: Io, script: []const u8, tail: Tail) ScriptReader {
        return .{
            .interface = .{ .vtable = &vtable, .buffer = &.{}, .seek = 0, .end = 0 },
            .io = io,
            .script = script,
            .pos = 0,
            .tail = tail,
            .gate_at = std.math.maxInt(usize),
            .gate = .unset,
            .at_gate = .unset,
            .blocked = .unset,
            .release = .unset,
            .buf = undefined,
        };
    }

    /// Points the reader at its own buffer. Call it after the value reached
    /// its final address.
    fn ready(self: *ScriptReader) *Reader {
        self.interface.buffer = &self.buf;
        return &self.interface;
    }

    fn stream(r: *Reader, w: *Writer, limit: Io.Limit) Reader.StreamError!usize {
        const self: *ScriptReader = @alignCast(@fieldParentPtr("interface", r));

        if (self.pos >= self.gate_at and !self.gate.isSet()) {
            self.at_gate.set(self.io);
            self.gate.wait(self.io) catch return error.ReadFailed;
        }
        if (self.pos == self.script.len) switch (self.tail) {
            .end_of_stream => return error.EndOfStream,
            .wait => {
                self.blocked.set(self.io);
                self.release.wait(self.io) catch return error.ReadFailed;
                return error.EndOfStream;
            },
        };

        // The reader never gives octets from beyond the gate, so a test can
        // hold the whole tail of the script back.
        const end = if (self.pos < self.gate_at)
            @min(self.gate_at, self.script.len)
        else
            self.script.len;
        const n = try w.write(limit.sliceConst(self.script[self.pos..end]));
        self.pos += n;
        return n;
    }
};

/// A writer that counts the empty frames that pass through it.
///
/// The connection writes one frame and then flushes, so each flush carries one
/// whole frame. The writer can also open the gate of a `ScriptReader` after a
/// given number of flushes, so that the scripted peer answers only after this
/// peer wrote.
const CountingWriter = struct {
    interface: Writer,
    io: Io,
    inner: *Writer,
    /// The number of empty frames that this writer saw.
    empty_frames: std.atomic.Value(u32),
    /// The number of frames that this writer saw.
    frames: std.atomic.Value(u32),
    /// The event that the writer sets for the first empty frame.
    first_empty: Io.Event,
    /// The gate that the writer opens after `release_after` frames.
    release: ?*Io.Event,
    release_after: u32,

    const vtable: Writer.VTable = .{ .drain = drain, .flush = flush };

    fn init(io: Io, inner: *Writer, buf: []u8) CountingWriter {
        return .{
            .interface = .{ .vtable = &vtable, .buffer = buf },
            .io = io,
            .inner = inner,
            .empty_frames = .init(0),
            .frames = .init(0),
            .first_empty = .unset,
            .release = null,
            .release_after = 0,
        };
    }

    fn drain(w: *Writer, data: []const []const u8, splat: usize) Writer.Error!usize {
        const self: *CountingWriter = @alignCast(@fieldParentPtr("interface", w));
        try self.inner.writeAll(w.buffered());
        _ = w.consumeAll();
        return self.inner.writeSplat(data, splat);
    }

    fn flush(w: *Writer) Writer.Error!void {
        const self: *CountingWriter = @alignCast(@fieldParentPtr("interface", w));
        const bytes = w.buffered();
        if (bytes.len != 0) {
            if (std.mem.eql(u8, bytes, &empty_frame_bytes)) {
                _ = self.empty_frames.fetchAdd(1, .monotonic);
                self.first_empty.set(self.io);
            }
            const count = self.frames.fetchAdd(1, .monotonic) + 1;
            try self.inner.writeAll(bytes);
            _ = w.consumeAll();
            if (self.release) |gate| {
                if (count == self.release_after) gate.set(self.io);
            }
        }
        try self.inner.flush();
    }
};

/// The test double stack: a scripted reader, a counting writer, and the sink
/// of `MockTransport`.
///
/// The value must not move after `ready`.
const Peer = struct {
    io: Io,
    reader: ScriptReader,
    mock: transport.MockTransport,
    counter: CountingWriter,
    counter_buf: [4096]u8,

    fn init(gpa: Allocator, io: Io, script: []const u8, tail: ScriptReader.Tail) Peer {
        return .{
            .io = io,
            .reader = .init(io, script, tail),
            // The mock gives the sink. The read side comes from `reader`.
            .mock = .init(gpa, ""),
            .counter = undefined,
            .counter_buf = undefined,
        };
    }

    /// Finishes the setup and returns the stream for `Connection.open`.
    fn ready(self: *Peer) Stream {
        self.counter = .init(self.io, self.mock.writer(), &self.counter_buf);
        return .{ .reader = self.reader.ready(), .writer = &self.counter.interface };
    }

    fn deinit(self: *Peer) void {
        self.mock.deinit();
    }

    /// Returns every octet that the code under test wrote.
    fn sent(self: *Peer) []const u8 {
        return self.mock.sent();
    }
};

/// Stops the process when a test blocks. A hung test suite is worse than a
/// failing one.
fn watchdogRun(io: Io, seconds: i64) void {
    io.sleep(.fromSeconds(seconds), .awake) catch return;
    std.debug.panic("the test did not finish within {d} seconds", .{seconds});
}

fn startWatchdog(io: Io) ?Io.Future(void) {
    return io.concurrent(watchdogRun, .{ io, watchdog_seconds }) catch null;
}

fn stopWatchdog(io: Io, future: *?Io.Future(void)) void {
    if (future.*) |*f| f.cancel(io);
}

/// Appends one frame that the remote peer sends.
fn appendFrame(
    sink: *Writer.Allocating,
    channel: u16,
    body: framing.Body,
    payload: []const u8,
) !void {
    try framing.writeFrame(&sink.writer, channel, body, payload, default_max_frame_size);
}

/// Builds the octets of one open frame that the remote peer sends.
fn scriptOpen(gpa: Allocator, open_performative: performatives.Open) ![]u8 {
    var sink: Writer.Allocating = .init(gpa);
    errdefer sink.deinit();
    try appendFrame(&sink, 0, .{ .open = open_performative }, "");
    return sink.toOwnedSlice();
}

/// A reader of one channel queue. It counts the frames that it took, and it
/// records the reason that woke it.
const QueueReader = struct {
    connection: *Connection,
    queue: *FrameQueue,
    io: Io,
    /// The number of frames that the task took before it woke.
    received: u32 = 0,
    /// The reason that the queue gave, or null while the task still reads.
    woke_with: ?Error = null,

    fn run(self: *QueueReader) void {
        while (true) {
            var frame = self.queue.getOne(self.io) catch {
                self.woke_with = if (self.connection.failure()) |f|
                    f.err
                else
                    error.ConnectionClosed;
                return;
            };
            frame.deinit();
            self.received += 1;
        }
    }
};

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

test "open sends the open frame and records the parameters of the remote peer" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{
        .container_id = "the-peer",
        .max_frame_size = 4096,
        .channel_max = 7,
        .idle_time_out = 0,
    });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .end_of_stream);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{
        .hostname = "host.example",
        .container_id = "the-client",
    });
    defer connection.deinit();

    try testing.expectEqualStrings("the-peer", connection.remote_container_id.?);
    try testing.expectEqual(@as(u32, 4096), connection.remote_max_frame_size);
    try testing.expectEqual(@as(u16, 7), connection.remote_channel_max);
    try testing.expectEqual(@as(?u32, 0), connection.remote_idle_time_out_ms);

    var want: Writer.Allocating = .init(gpa);
    defer want.deinit();
    try framing.writeFrame(&want.writer, 0, .{ .open = .{
        .container_id = "the-client",
        .hostname = "host.example",
        .max_frame_size = default_max_frame_size,
        .channel_max = default_channel_max,
        .idle_time_out = default_idle_time_out_ms,
    } }, "", framing.min_max_frame_size);
    try testing.expectEqualSlices(u8, want.written(), peer.sent());
}

test "open generates a container id when the caller gives none" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{ .container_id = "the-peer" });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .end_of_stream);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    try testing.expectEqual(container_id_len, connection.container_id.len);
    for (connection.container_id) |c| try testing.expect(std.ascii.isHex(c));
}

test "the close handshake sends one close frame and ends the connection" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    // The peer answers with its own close, but only after this peer wrote its
    // open frame and then its close frame.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try appendFrame(&sink, 0, .{ .open = .{ .container_id = "the-peer" } }, "");
    const gate_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .close = .{} }, "");

    var peer: Peer = .init(gpa, io, sink.written(), .end_of_stream);
    defer peer.deinit();
    const stream = peer.ready();
    peer.reader.gate_at = gate_at;
    peer.counter.release = &peer.reader.gate;
    peer.counter.release_after = 2;

    const connection = try Connection.open(gpa, io, stream, .{ .container_id = "the-client" });
    defer connection.deinit();

    try connection.close(null);
    try testing.expectEqual(Error.ConnectionClosed, connection.failure().?.err);

    // The wire holds the open frame and exactly one close frame.
    var want: Writer.Allocating = .init(gpa);
    defer want.deinit();
    try framing.writeFrame(&want.writer, 0, .{ .open = .{
        .container_id = "the-client",
        .max_frame_size = default_max_frame_size,
        .channel_max = default_channel_max,
        .idle_time_out = default_idle_time_out_ms,
    } }, "", framing.min_max_frame_size);
    try framing.writeFrame(&want.writer, 0, .{ .close = .{} }, "", default_max_frame_size);
    try testing.expectEqualSlices(u8, want.written(), peer.sent());
}

test "the connection sends a heartbeat under a scripted remote idle timeout" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    // The interval is half of the idle timeout, so 20 milliseconds.
    const script = try scriptOpen(gpa, .{ .container_id = "the-peer", .idle_time_out = 40 });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    try testing.expectEqual(@as(?u32, 40), connection.remote_idle_time_out_ms);
    try peer.counter.first_empty.wait(io);
    try testing.expect(peer.counter.empty_frames.load(.monotonic) >= 1);
}

test "the connection sends no heartbeat when the remote peer asks for none" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{ .container_id = "the-peer" });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    try testing.expectEqual(@as(?u32, null), connection.remote_idle_time_out_ms);
    // Only the open frame reached the wire, and no task can add to it.
    try testing.expectEqual(@as(u32, 1), peer.counter.frames.load(.monotonic));
}

test "the heartbeat stops after the close handshake" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    // The interval is 10 milliseconds.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try appendFrame(&sink, 0, .{ .open = .{
        .container_id = "the-peer",
        .idle_time_out = 20,
    } }, "");
    const gate_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .close = .{} }, "");

    var peer: Peer = .init(gpa, io, sink.written(), .end_of_stream);
    defer peer.deinit();
    const stream = peer.ready();
    peer.reader.gate_at = gate_at;

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    // Let the heartbeat run, and then close.
    try peer.counter.first_empty.wait(io);
    peer.reader.gate.set(io);
    try connection.close(null);

    // The absence of a write needs a window of time. Two samples that agree
    // show that the heartbeat task stopped.
    try io.sleep(.fromMilliseconds(60), .awake);
    const first = peer.counter.frames.load(.monotonic);
    try io.sleep(.fromMilliseconds(60), .awake);
    try testing.expectEqual(first, peer.counter.frames.load(.monotonic));

    // A caller cannot write to the closed transport either.
    try testing.expectError(
        Error.ConnectionClosed,
        connection.send(0, .{ .end = .{} }, ""),
    );
}

test "the demultiplexer routes frames to two separate channels" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try appendFrame(&sink, 0, .{ .open = .{ .container_id = "the-peer" } }, "");
    try appendFrame(&sink, 1, .{ .begin = .{ .remote_channel = 11 } }, "");
    try appendFrame(&sink, 2, .{ .begin = .{ .remote_channel = 22 } }, "");

    var peer: Peer = .init(gpa, io, sink.written(), .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    var buf_one: [4]Frame = undefined;
    var buf_two: [4]Frame = undefined;
    var queue_one: FrameQueue = .init(&buf_one);
    var queue_two: FrameQueue = .init(&buf_two);
    try connection.registerChannel(1, &queue_one);
    try connection.registerChannel(2, &queue_two);

    var first = try queue_one.getOne(io);
    defer first.deinit();
    var second = try queue_two.getOne(io);
    defer second.deinit();

    try testing.expectEqual(@as(u16, 1), first.channel);
    try testing.expectEqual(@as(?u16, 11), first.body.?.begin.remote_channel);
    try testing.expectEqual(@as(u16, 2), second.channel);
    try testing.expectEqual(@as(?u16, 22), second.body.?.begin.remote_channel);
}

test "a channel accepts one session only" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{ .container_id = "the-peer" });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    var buf_one: [1]Frame = undefined;
    var buf_two: [1]Frame = undefined;
    var queue_one: FrameQueue = .init(&buf_one);
    var queue_two: FrameQueue = .init(&buf_two);
    try connection.registerChannel(3, &queue_one);
    try testing.expectError(error.ChannelInUse, connection.registerChannel(3, &queue_two));
}

test "a queued frame keeps its payload when the next read reuses the buffer" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const filler = "F" ** 512;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try appendFrame(&sink, 0, .{ .open = .{ .container_id = "the-peer" } }, "");
    try appendFrame(&sink, 1, .{ .transfer = .{ .handle = 0 } }, "the-message");
    try appendFrame(&sink, 2, .{ .transfer = .{ .handle = 1 } }, filler);

    var peer: Peer = .init(gpa, io, sink.written(), .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    var buf_one: [4]Frame = undefined;
    var buf_two: [4]Frame = undefined;
    var queue_one: FrameQueue = .init(&buf_one);
    var queue_two: FrameQueue = .init(&buf_two);
    try connection.registerChannel(1, &queue_one);
    try connection.registerChannel(2, &queue_two);

    // The second frame arrives only after the demultiplexer read it into the
    // one buffer that it owns, so the buffer no longer holds the first
    // payload.
    var second = try queue_two.getOne(io);
    defer second.deinit();
    try testing.expectEqualStrings(filler, second.payload);

    var first = try queue_one.getOne(io);
    defer first.deinit();
    try testing.expectEqualStrings("the-message", first.payload);
}

test "a socket failure wakes a blocked reader with the remote condition" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try appendFrame(&sink, 0, .{ .open = .{ .container_id = "the-peer" } }, "");
    try appendFrame(&sink, 1, .{ .begin = .{ .remote_channel = 11 } }, "");
    const gate_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .close = .{ .error_condition = .{
        .condition = .of("amqp:internal-error"),
        .description = "the peer gave up",
    } } }, "");

    var peer: Peer = .init(gpa, io, sink.written(), .end_of_stream);
    defer peer.deinit();
    const stream = peer.ready();
    peer.reader.gate_at = gate_at;

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    // The queue holds nothing, so the demultiplexer hands the begin frame over
    // only when the reader task waits for it.
    var empty: [0]Frame = .{};
    var queue: FrameQueue = .init(&empty);
    try connection.registerChannel(1, &queue);

    var reader: QueueReader = .{ .connection = connection, .queue = &queue, .io = io };
    var task = io.concurrent(QueueReader.run, .{&reader}) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return error.SkipZigTest,
    };

    // The reader task took the begin frame, and the demultiplexer now waits at
    // the gate. Let the close frame of the peer through.
    try peer.reader.at_gate.wait(io);
    peer.reader.gate.set(io);
    task.await(io);

    try testing.expectEqual(@as(u32, 1), reader.received);
    try testing.expectEqual(Error.RemoteError, reader.woke_with.?);

    const failure = connection.failure().?;
    try testing.expectEqual(Error.RemoteError, failure.err);
    try testing.expectEqualStrings("amqp:internal-error", failure.condition.?);
    try testing.expectEqualStrings("the peer gave up", failure.description.?);

    // The reason is sticky. A second caller reads the same one, and it never
    // blocks.
    var later: QueueReader = .{ .connection = connection, .queue = &queue, .io = io };
    later.run();
    try testing.expectEqual(@as(u32, 0), later.received);
    try testing.expectEqual(Error.RemoteError, later.woke_with.?);
    try testing.expectEqual(Error.RemoteError, connection.failure().?.err);

    // A later close call reports the same reason, and it does not block.
    try testing.expectError(Error.RemoteError, connection.close(null));
    try testing.expectEqual(Error.RemoteError, connection.failure().?.err);
}

test "the first reason wins, and a later reason does not replace it" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{ .container_id = "the-peer" });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    connection.fail(error.RemoteError, "amqp:internal-error", "the first reason");
    connection.fail(error.TransportFailure, "amqp:decode-error", "the second reason");

    const failure = connection.failure().?;
    try testing.expectEqual(Error.RemoteError, failure.err);
    try testing.expectEqualStrings("amqp:internal-error", failure.condition.?);
    try testing.expectEqualStrings("the first reason", failure.description.?);
}

test "a truncated stream ends the connection with a transport failure" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try appendFrame(&sink, 0, .{ .open = .{ .container_id = "the-peer" } }, "");
    const gate_at = sink.written().len;
    try appendFrame(&sink, 1, .{ .begin = .{ .remote_channel = 11 } }, "");

    // The peer cuts the stream in the middle of the second frame.
    const script = try gpa.dupe(u8, sink.written()[0 .. sink.written().len - 3]);
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .end_of_stream);
    defer peer.deinit();
    const stream = peer.ready();
    peer.reader.gate_at = gate_at;

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    var empty: [0]Frame = .{};
    var queue: FrameQueue = .init(&empty);
    try connection.registerChannel(1, &queue);

    var reader: QueueReader = .{ .connection = connection, .queue = &queue, .io = io };
    var task = io.concurrent(QueueReader.run, .{&reader}) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return error.SkipZigTest,
    };

    peer.reader.gate.set(io);
    task.await(io);

    try testing.expectEqual(@as(u32, 0), reader.received);
    try testing.expectEqual(Error.TransportFailure, reader.woke_with.?);
    try testing.expectEqualStrings("EndOfStream", connection.failure().?.description.?);
}

test "a frame on a channel that no session registered is a protocol error" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try appendFrame(&sink, 0, .{ .open = .{ .container_id = "the-peer" } }, "");
    try appendFrame(&sink, 9, .{ .begin = .{ .remote_channel = 9 } }, "");

    var peer: Peer = .init(gpa, io, sink.written(), .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    try connection.done.wait(io);
    const failure = connection.failure().?;
    try testing.expectEqual(Error.ProtocolError, failure.err);
    try testing.expect(std.mem.indexOf(u8, failure.description.?, "channel") != null);
}

test "a second open frame is a protocol error" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try appendFrame(&sink, 0, .{ .open = .{ .container_id = "the-peer" } }, "");
    try appendFrame(&sink, 0, .{ .open = .{ .container_id = "the-peer" } }, "");

    var peer: Peer = .init(gpa, io, sink.written(), .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    try connection.done.wait(io);
    try testing.expectEqual(Error.ProtocolError, connection.failure().?.err);
}

test "deinit cancels the demultiplexer while it waits inside a read" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{ .container_id = "the-peer", .idle_time_out = 40 });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});

    // The demultiplexer now waits inside a read, and the heartbeat task waits
    // for its next interval. `deinit` must end both, and it must return.
    try peer.reader.blocked.wait(io);
    connection.deinit();
}

test "deinit wakes a reader that waits on a channel queue" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{ .container_id = "the-peer" });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});

    var empty: [0]Frame = .{};
    var queue: FrameQueue = .init(&empty);
    try connection.registerChannel(1, &queue);

    var reader: QueueReader = .{ .connection = connection, .queue = &queue, .io = io };
    var task = io.concurrent(QueueReader.run, .{&reader}) catch |err| switch (err) {
        error.ConcurrencyUnavailable => {
            connection.deinit();
            return error.SkipZigTest;
        },
    };

    try peer.reader.blocked.wait(io);
    connection.deinit();
    task.await(io);

    try testing.expectEqual(@as(u32, 0), reader.received);
    try testing.expectEqual(Error.ConnectionClosed, reader.woke_with.?);
}

test "a caller writes while the demultiplexer waits inside a read" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{ .container_id = "the-peer", .idle_time_out = 40 });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    // The write lock never waits for a read, so both the caller and the
    // heartbeat task reach the wire.
    try peer.reader.blocked.wait(io);
    try connection.send(1, .{ .begin = .{ .remote_channel = 1 } }, "");
    try peer.counter.first_empty.wait(io);
    try testing.expect(peer.counter.frames.load(.monotonic) >= 3);
}

test "close reports a timeout when the remote peer sends no close frame" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{ .container_id = "the-peer" });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{
        .close_timeout = .{ .duration = .{ .raw = .fromMilliseconds(50), .clock = .awake } },
    });
    defer connection.deinit();

    var empty: [0]Frame = .{};
    var queue: FrameQueue = .init(&empty);
    try connection.registerChannel(1, &queue);

    try testing.expectError(error.Timeout, connection.close(null));

    // The connection still ends, so no caller waits for a frame that cannot
    // arrive.
    try testing.expectEqual(Error.ConnectionClosed, connection.failure().?.err);
    var reader: QueueReader = .{ .connection = connection, .queue = &queue, .io = io };
    reader.run();
    try testing.expectEqual(Error.ConnectionClosed, reader.woke_with.?);
}

test "unregisterChannel closes the queue and stops the routing" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{ .container_id = "the-peer" });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    var buf: [1]Frame = undefined;
    var queue: FrameQueue = .init(&buf);
    try connection.registerChannel(4, &queue);
    connection.unregisterChannel(4);

    try testing.expectError(error.Closed, queue.getOne(io));
    // The channel is free again, and the connection still runs.
    var other: [1]Frame = undefined;
    var second: FrameQueue = .init(&other);
    try connection.registerChannel(4, &second);
}

test "the open frame must fit in the size that section 2.4.1 allows" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const script = try scriptOpen(gpa, .{ .container_id = "the-peer" });
    defer gpa.free(script);

    var peer: Peer = .init(gpa, io, script, .end_of_stream);
    defer peer.deinit();
    const stream = peer.ready();

    const long_name = "n" ** 600;
    try testing.expectError(error.FrameTooLarge, Connection.open(gpa, io, stream, .{
        .container_id = long_name,
    }));
}
