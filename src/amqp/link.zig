//! The shared plumbing of an AMQP 1.0 link.
//!
//! A link is one path for messages between two nodes. Section 2.6 gives every
//! link the same attach handshake, the same detach handshake, and the same
//! error report, and the role of the endpoint changes only the frames that
//! flow after the attach. This file holds the parts that both roles share.
//!
//! Specification:
//! OASIS AMQP Version 1.0 Part 2: Transport, sections 2.6.1 to 2.6.6, 2.7.3,
//! and 2.7.7.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html
//!
//! This file names no vendor concept. The caller supplies the node address,
//! the link properties, and the capabilities.
//!
//! # The tasks
//!
//! `Link.start` puts one router task in an `std.Io.Group`. The router task is
//! the only code that takes a frame from the queue of the link. It reads the
//! attach frame and the detach frame itself, and it gives every other frame to
//! the `Handler` that the concrete link supplies.
//!
//! # The order of the calls
//!
//! **Call `Link.deinit` before `Session.deinit`.** The session pushes a frame
//! to the queue of the link, so it must not hold a pointer to a queue that the
//! link already freed. `Link.deinit` removes the link from the session.
//!
//! The memory of the frame queue must stay valid until `Session.deinit`
//! returns. `Storage` holds that memory, and the owner of the link owns the
//! `Storage`.
//!
//! # The locks
//!
//! This file takes no lock of its own. The terminal state is an atomic value
//! that one task claims, and the two events publish the moments that a caller
//! waits for. A concrete link adds the locks that its own state needs.

const std = @import("std");

const connection_mod = @import("connection.zig");
const framing = @import("framing.zig");
const performatives = @import("performatives.zig");
const session_mod = @import("session.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Frame = framing.Frame;
const FrameQueue = connection_mod.FrameQueue;
const Io = std.Io;
const MapEntry = types.MapEntry;
const Session = session_mod.Session;
const Symbol = performatives.Symbol;

// -------------------------------------------------------------------------
// The constants
// -------------------------------------------------------------------------

/// The number of frames that the queue of a link holds by default.
///
/// The queue takes the `attach`, the `flow`, the `disposition`, and the
/// `detach` frames of one link. A session pushes to it, and a full queue holds
/// the router task of the session, so the value must cover a burst.
pub const default_queue_capacity: usize = 64;

/// The number of characters in a generated link name. The name holds the
/// hexadecimal form of 16 random octets.
pub const name_len: usize = 32;

/// The time that `Link.awaitAttached` waits for the attach frame of the remote
/// peer.
pub const default_attach_timeout: Io.Timeout =
    .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } };

/// The time that `Link.detach` waits for the detach frame of the remote peer.
pub const default_detach_timeout: Io.Timeout =
    .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } };

// -------------------------------------------------------------------------
// The errors
// -------------------------------------------------------------------------

/// The reason that a link ended. The value is sticky: the first reason wins,
/// and every later caller reads the same one.
///
/// Zig holds one namespace for every error name, so these names carry the word
/// `Link`. A caller can then tell which layer failed.
pub const Error = error{
    /// The remote peer detached the link, and it reported no error.
    /// `Link.deinit` also gives this reason.
    LinkDetached,
    /// The remote peer detached the link with an error. Read `Link.failure`
    /// for the condition symbol and the description text.
    LinkRemoteError,
    /// The session under the link ended. Read `Session.failure` for the
    /// reason.
    LinkSessionFailed,
    /// The remote peer refused the link. Section 2.6.3: a peer that will not
    /// create a terminus answers with a null local terminus and then detaches.
    /// The local terminus of the remote peer is the `target` for a receiving
    /// endpoint and the `source` for a sending one.
    LinkRefused,
    /// A write failed part way through a delivery, and the abort of that
    /// delivery failed too. The remote peer holds an open delivery that no
    /// frame can close. The link refuses every later send, because section
    /// 2.6.14 forbids a new delivery inside an open one, and a new delivery id
    /// on a continuation frame is an error under section 2.7.5.
    LinkPartialDelivery,
};

/// The reason that a link ended, with the text that came with it.
///
/// This is the value that surfaces a remote `detach` that carries an error.
/// Section 2.8.14 gives the error a mandatory condition symbol and an optional
/// description text, and both appear here.
///
/// The two slices live until `Link.deinit` returns.
pub const Failure = struct {
    /// The reason.
    err: Error,
    /// The condition symbol, or null when the reason carries none.
    condition: ?[]const u8,
    /// The description text, or null.
    description: ?[]const u8,
};

/// The errors of `Link.register`.
pub const RegisterError = Error || Allocator.Error || Io.Cancelable ||
    session_mod.AttachError;

/// The errors of the send paths of a link.
pub const SendError = Error || session_mod.SendError;

/// The errors of `Link.awaitAttached`.
pub const AttachError = SendError || Io.Timeout.Error;

/// The errors of `Link.detach`.
pub const DetachError = SendError || Io.Timeout.Error;

// -------------------------------------------------------------------------
// The deadline
// -------------------------------------------------------------------------

/// One deadline, read from the clock one time.
///
/// `std.Io.Event.waitTimeout` reports a spurious wakeup with the same error as
/// a real timeout, so every wait of this module reads the clock itself before
/// it gives up. A `Deadline` holds the moment, so a wait that runs again does
/// not start the clock again.
pub const Deadline = struct {
    /// The moment that the wait ends, or null for a wait without an end.
    at: ?Io.Clock.Timestamp,

    /// Returns the deadline of `limit`, from the clock of this moment.
    pub fn start(io: Io, limit: Io.Timeout) Deadline {
        return .{ .at = limit.toTimestamp(io) };
    }

    /// Returns the timeout that one wait call takes.
    pub fn timeout(self: Deadline) Io.Timeout {
        const at = self.at orelse return .none;
        return .{ .deadline = at };
    }

    /// Returns true when the moment passed.
    pub fn passed(self: Deadline, io: Io) bool {
        const at = self.at orelse return false;
        return at.untilNow(io).raw.nanoseconds >= 0;
    }
};

// -------------------------------------------------------------------------
// The options
// -------------------------------------------------------------------------

/// The arguments that the attach frame of every link carries.
///
/// The caller owns every slice, and every slice must live until the attach
/// call returns. This module names no capability, no property, and no address:
/// the caller supplies all three.
pub const Options = struct {
    /// The address of the node at the far end of the link.
    ///
    /// Section 2.7.3 puts it on the `target` of a sending endpoint and on the
    /// `source` of a receiving endpoint.
    address: []const u8,
    /// The name of the link. Section 2.6.1 makes the name the identity of the
    /// link. The attach call generates a random name when this field is null.
    name: ?[]const u8 = null,
    /// The properties of the link. The caller supplies every entry.
    properties: ?[]const MapEntry = null,
    /// The extension capabilities that this endpoint wants.
    desired_capabilities: ?[]const Symbol = null,
    /// The extension capabilities that this endpoint supports.
    offered_capabilities: ?[]const Symbol = null,
    /// The number of frames that the queue of the link holds.
    queue_capacity: usize = default_queue_capacity,
    /// The time that the attach call waits for the attach frame of the remote
    /// peer.
    attach_timeout: Io.Timeout = default_attach_timeout,
    /// The time that `detach` waits for the detach frame of the remote peer.
    detach_timeout: Io.Timeout = default_detach_timeout,
};

/// The arguments of `Link.detach`.
pub const DetachOptions = struct {
    /// True when this endpoint closes the link. Section 2.6.6 destroys both
    /// link endpoints for a closing detach, and section 2.6.4 lets a
    /// non-closing detach reattach later.
    closed: bool = true,
    /// The error that caused the detach, or null for a clean detach.
    error_condition: ?performatives.ErrorCondition = null,
};

// -------------------------------------------------------------------------
// The queue storage
// -------------------------------------------------------------------------

/// The frame queue of one link, and the memory that holds it.
///
/// The owner of the link owns this value, because its memory must stay valid
/// until `Session.deinit` returns. Read the order of the calls at the top of
/// this file.
///
/// The value must not move after `Link.register`, because the session and the
/// link both hold a pointer to the queue.
pub const Storage = struct {
    /// The slots of the queue.
    frames: []Frame,
    /// The queue that the router task of the session pushes to.
    queue: FrameQueue,

    /// Allocates a queue that holds `capacity` frames.
    ///
    /// The caller frees it with `deinit`, after `Session.deinit` returns.
    pub fn init(gpa: Allocator, capacity: usize) Allocator.Error!Storage {
        const frames = try gpa.alloc(Frame, capacity);
        return .{ .frames = frames, .queue = .init(frames) };
    }

    /// Frees the slots. Call it after `Session.deinit` returns.
    pub fn deinit(self: *Storage, gpa: Allocator) void {
        gpa.free(self.frames);
        self.* = undefined;
    }
};

// -------------------------------------------------------------------------
// The handler
// -------------------------------------------------------------------------

/// The hook that a concrete link gives to the shared router task.
///
/// The router task reads the `attach` frame and the `detach` frame itself,
/// because both roles answer them in the same way. Every other frame goes to
/// `onFrame`.
pub const Handler = struct {
    /// The concrete link. The two functions cast it back to their own type.
    context: *anyopaque,
    /// Takes one frame that the shared state machine does not read, and takes
    /// its memory with it. The function must call `Frame.deinit`.
    onFrame: *const fn (context: *anyopaque, frame: Frame) void,
    /// Wakes every task that waits inside the concrete link. The link called
    /// `fail` just before this, so `Link.failure` already reports the reason.
    /// The router task calls it one time.
    onEnd: *const fn (context: *anyopaque) void,
};

/// A handler that does nothing. A link that reads the attach handshake alone
/// can use it.
pub const null_handler: Handler = .{
    .context = undefined,
    .onFrame = dropFrame,
    .onEnd = doNothing,
};

fn dropFrame(_: *anyopaque, frame: Frame) void {
    frame.deinit();
}

fn doNothing(_: *anyopaque) void {}

// -------------------------------------------------------------------------
// The remote endpoint
// -------------------------------------------------------------------------

/// The fields of the attach frame of the remote peer that a caller reads.
///
/// The router task writes them before it sets the attached event, so a caller
/// that returned from `awaitAttached` reads a complete value.
pub const Remote = struct {
    /// The handle that the remote peer chose. Section 2.6.2 calls it the input
    /// handle of this endpoint.
    handle: ?u32 = null,
    /// The role of the remote endpoint. It is the opposite of the role of this
    /// endpoint.
    role: ?performatives.Role = null,
    /// The settlement policy that the remote peer applies as a sender.
    snd_settle_mode: ?performatives.SenderSettleMode = null,
    /// The settlement policy that the remote peer applies as a receiver.
    rcv_settle_mode: ?performatives.ReceiverSettleMode = null,
    /// The delivery count that a remote sending endpoint starts from.
    initial_delivery_count: ?u32 = null,
    /// The largest message that the remote peer accepts, in octets.
    ///
    /// Section 2.7.3: "If this field is zero or unset, there is no maximum
    /// size imposed by the link endpoint." This field holds the value that the
    /// frame carried, so a caller reads a zero as "no limit" too.
    max_message_size: ?u64 = null,
    /// True when the remote peer sent no source. Section 2.7.3 makes a null
    /// source the answer of a peer that refuses the link.
    source_absent: bool = true,
    /// True when the remote peer sent no target.
    target_absent: bool = true,
};

// -------------------------------------------------------------------------
// The link
// -------------------------------------------------------------------------

/// The registration arguments of `Link.register`.
pub const Registration = struct {
    /// The session that carries the link.
    session: *Session,
    /// The queue that receives the frames of the link. The memory must stay
    /// valid until `Session.deinit` returns.
    queue: *FrameQueue,
    /// The role of this endpoint. Section 2.8.1.
    role: performatives.Role,
    /// The name of the link. `register` copies it.
    name: []const u8,
    /// The hook of the concrete link.
    handler: Handler,
};

/// One link endpoint, without the state that its role adds.
///
/// The value must not move after `register`, because the router task holds a
/// pointer to it. Embed it in a concrete link that lives on the heap.
pub const Link = struct {
    gpa: Allocator,
    io: Io,
    session: *Session,
    /// The queue of the link. The owner of the link owns the memory.
    queue: *FrameQueue,
    /// The hook of the concrete link.
    handler: Handler,

    /// The name of the link. The link owns the memory. Section 2.6.1 makes the
    /// name the identity of the link, and the two handles are shorthands.
    name: []const u8,
    /// The handle that this endpoint chose. Section 2.6.2 calls it the output
    /// handle.
    handle: u32,
    /// The role of this endpoint.
    role: performatives.Role,

    /// The attach frame of the remote peer.
    remote: Remote,

    /// The event that the router task sets when the attach handshake ends,
    /// with an attach frame or with a failure.
    attached: Io.Event,
    /// The event that the link sets when it reaches its terminal state.
    ended: Io.Event,
    /// True after the router task read the detach frame of the remote peer.
    remote_detach_seen: std.atomic.Value(bool),
    /// True after this endpoint sent its detach frame.
    detach_sent: std.atomic.Value(bool),

    /// The step of the terminal state. Read the note on `fail`.
    state: std.atomic.Value(u8),
    failure_err: Error,
    failure_condition: ?[]const u8,
    failure_description: ?[]const u8,

    /// The group that holds the router task.
    group: Io.Group,
    /// True after `register` took an output handle from the session.
    registered: bool,

    /// The link runs.
    const state_running: u8 = 0;
    /// One task won the race to write the terminal state, and it writes now.
    const state_claimed: u8 = 1;
    /// The terminal state is readable.
    const state_ended: u8 = 2;

    /// The value of a link before `register` fills it.
    pub const empty: Link = .{
        .gpa = undefined,
        .io = undefined,
        .session = undefined,
        .queue = undefined,
        .handler = null_handler,
        .name = &.{},
        .handle = 0,
        .role = .sender,
        .remote = .{},
        .attached = .unset,
        .ended = .unset,
        .remote_detach_seen = .init(false),
        .detach_sent = .init(false),
        .state = .init(state_running),
        .failure_err = error.LinkDetached,
        .failure_condition = null,
        .failure_description = null,
        .group = .init,
        .registered = false,
    };

    // ---------------------------------------------------------------------
    // The setup
    // ---------------------------------------------------------------------

    /// Registers the link with its session and takes an output handle.
    ///
    /// The call copies `args.name`, and the copy lives until `deinit` returns.
    /// It sends no frame. Call `start`, then `sendAttach`, then
    /// `awaitAttached`.
    ///
    /// The call takes the allocator and the `Io` of the session, so that every
    /// layer shares both.
    pub fn register(self: *Link, args: Registration) RegisterError!void {
        const session = args.session;
        self.* = .{
            .gpa = session.gpa,
            .io = session.io,
            .session = session,
            .queue = args.queue,
            .handler = args.handler,
            .name = &.{},
            .handle = 0,
            .role = args.role,
            .remote = .{},
            .attached = .unset,
            .ended = .unset,
            .remote_detach_seen = .init(false),
            .detach_sent = .init(false),
            .state = .init(state_running),
            .failure_err = error.LinkDetached,
            .failure_condition = null,
            .failure_description = null,
            .group = .init,
            .registered = false,
        };

        self.name = try session.gpa.dupe(u8, args.name);
        errdefer session.gpa.free(self.name);

        self.handle = try session.attachLink(self.name, args.queue);
        self.registered = true;
    }

    /// Starts the router task of the link.
    ///
    /// Call it before `sendAttach`, so that the answer of the remote peer
    /// cannot arrive before a task reads the queue.
    pub fn start(self: *Link) Io.ConcurrentError!void {
        return self.group.concurrent(self.io, route, .{self});
    }

    /// Frees the link.
    ///
    /// The call ends the link, stops the router task, waits for it, gives the
    /// output handle back to the session, drains the queue, and frees the name
    /// and the failure text. It does not free the memory of `Storage`.
    ///
    /// Call it before `Session.deinit`.
    pub fn deinit(self: *Link) void {
        if (!self.registered) return;

        // Wake every task that waits inside the concrete link before the
        // router task stops, or such a task waits for a frame that no task can
        // send.
        self.fail(error.LinkDetached, null, null);

        // Close the queue before the cancel, so that a router task inside a
        // `getOne` wakes. The session then drops the later frames of the link.
        self.queue.close(self.io);
        self.group.cancel(self.io);

        self.session.detachLink(self.handle);
        drainQueue(self.io, self.queue);

        self.gpa.free(self.name);
        if (self.failure_condition) |text| self.gpa.free(text);
        if (self.failure_description) |text| self.gpa.free(text);
        self.registered = false;
    }

    // ---------------------------------------------------------------------
    // The terminal state
    // ---------------------------------------------------------------------

    /// Returns the reason that the link ended, or null while it runs.
    ///
    /// The reason is sticky. The first task to end the link writes it, and
    /// every later caller reads the same value. The two slices live until
    /// `deinit` returns.
    pub fn failure(self: *Link) ?Failure {
        if (self.state.load(.acquire) != state_ended) return null;
        return .{
            .err = self.failure_err,
            .condition = self.failure_condition,
            .description = self.failure_description,
        };
    }

    /// Returns true while the link runs and the remote attach frame arrived.
    pub fn isAttached(self: *Link) bool {
        return self.attached.isSet() and self.failure() == null;
    }

    /// Ends the link with a reason, and wakes every task that waits on it.
    ///
    /// The function takes the first reason and drops every later one. It moves
    /// the state in two steps: the winner claims the state, fills the fields,
    /// and only then publishes them, so a reader that sees `state_ended` sees
    /// the fields too.
    pub fn fail(
        self: *Link,
        err: Error,
        failure_condition: ?[]const u8,
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
        // caller, and the link is already ending.
        if (failure_condition) |text| self.failure_condition = self.gpa.dupe(u8, text) catch null;
        if (description) |text| self.failure_description = self.gpa.dupe(u8, text) catch null;
        self.state.store(state_ended, .release);

        // A task that waits for the attach frame must wake too, because the
        // frame can no longer arrive.
        self.attached.set(self.io);
        self.ended.set(self.io);
        self.handler.onEnd(self.handler.context);
    }

    // ---------------------------------------------------------------------
    // The attach handshake
    // ---------------------------------------------------------------------

    /// Sends the attach frame of section 2.7.3.
    ///
    /// The concrete link builds the performative, because the fields that it
    /// carries depend on the role. The caller owns every slice of
    /// `performative`, and the slices must live until this call returns.
    pub fn sendAttach(self: *Link, performative: performatives.Attach) SendError!void {
        return self.sendFrame(.{ .attach = performative });
    }

    /// Sends one frame of this link on the session under it.
    ///
    /// Use it for a frame that carries no window of section 2.5.6, such as a
    /// `disposition`. The call ends the link when the session refuses the
    /// frame, so that no task waits for an answer that cannot arrive.
    ///
    /// The caller owns every slice of `body`, and the slices must live until
    /// this call returns.
    pub fn sendFrame(self: *Link, body: framing.Body) SendError!void {
        if (self.failure()) |f| return f.err;
        self.session.send(body, "") catch |err| {
            self.noteSessionError(err);
            return err;
        };
    }

    /// Sends one flow frame that carries the state of this link.
    ///
    /// Section 2.7.4 puts the session state on every flow frame, so the send
    /// goes through the session and the session fills those four fields in.
    pub fn sendFlow(self: *Link, state: session_mod.LinkFlow) SendError!void {
        if (self.failure()) |f| return f.err;
        self.session.sendFlow(state) catch |err| {
            self.noteSessionError(err);
            return err;
        };
    }

    /// Waits for the attach frame of the remote peer.
    ///
    /// The call returns `error.Timeout` when no answer arrives within
    /// `timeout`. It reports a link that ended under the wait with the reason
    /// of the link.
    pub fn awaitAttached(self: *Link, timeout: Io.Timeout) AttachError!void {
        const deadline: Deadline = .start(self.io, timeout);
        while (!self.attached.isSet()) {
            self.attached.waitTimeout(self.io, deadline.timeout()) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                // The call reports a spurious wakeup as a timeout too, so the
                // loop reads the clock itself.
                error.Timeout => if (deadline.passed(self.io)) return error.Timeout,
            };
        }
        if (self.failure()) |f| return f.err;
    }

    /// Returns the largest message that the remote peer accepts, in octets, or
    /// null when the remote peer set no limit.
    ///
    /// Section 2.7.3 reads a zero and an absent field as "no maximum size", so
    /// this call folds both into null.
    pub fn maxMessageSize(self: *Link) ?u64 {
        const size = self.remote.max_message_size orelse return null;
        if (size == 0) return null;
        return size;
    }

    // ---------------------------------------------------------------------
    // The detach handshake
    // ---------------------------------------------------------------------

    /// Runs the detach handshake of sections 2.6.4 and 2.6.6.
    ///
    /// The call sends the detach frame, and then it waits for the detach frame
    /// of the remote peer. Give `options.closed` as true to close the link,
    /// which destroys both link endpoints.
    ///
    /// The call returns `error.Timeout` when the remote peer sends no detach
    /// within `timeout`. It ends the link either way, so no task waits for a
    /// frame that cannot arrive.
    ///
    /// The caller owns every slice of `options.error_condition`, and the
    /// slices must live until this call returns.
    ///
    /// The call keeps the output handle. `deinit` gives it back to the
    /// session, so a caller that wants the number free again frees the link.
    pub fn detach(
        self: *Link,
        options: DetachOptions,
        timeout: Io.Timeout,
    ) DetachError!void {
        self.sendDetach(options) catch |err| {
            self.fail(error.LinkSessionFailed, null, @errorName(err));
            self.group.cancel(self.io);
            return err;
        };

        const deadline: Deadline = .start(self.io, timeout);
        var timed_out = false;
        // `fail` sets the event, so the loop ends on the timeout path too.
        while (!self.ended.isSet()) {
            self.ended.waitTimeout(self.io, deadline.timeout()) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                error.Timeout => if (deadline.passed(self.io)) {
                    // The remote peer never answered. End the link anyway, so
                    // that no task waits for a frame that cannot arrive.
                    self.fail(error.LinkDetached, null, "the remote peer sent no detach frame");
                    timed_out = true;
                },
            };
        }

        self.group.cancel(self.io);
        if (timed_out) return error.Timeout;

        const f = self.failure().?;
        if (f.err == error.LinkDetached) return;
        return f.err;
    }

    /// Writes the detach frame one time.
    fn sendDetach(self: *Link, options: DetachOptions) SendError!void {
        if (self.detach_sent.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;
        self.session.send(.{ .detach = .{
            .handle = self.handle,
            .closed = options.closed,
            .error_condition = options.error_condition,
        } }, "") catch |err| {
            self.noteSessionError(err);
            return err;
        };
    }

    // ---------------------------------------------------------------------
    // The router task
    // ---------------------------------------------------------------------

    /// What the router task does after one frame.
    const Routing = enum { keep_reading, stop };

    /// Takes frames from the queue of the link and routes them until the link
    /// ends.
    fn route(self: *Link) void {
        while (true) {
            const frame = self.queue.getOne(self.io) catch |err| {
                switch (err) {
                    // `deinit` closed the queue, or the session ended. The
                    // session is alive here, because the owner of the link
                    // frees it only after `Link.deinit` returns.
                    error.Closed => {
                        const reason = self.session.failure();
                        self.fail(
                            error.LinkSessionFailed,
                            if (reason) |f| f.condition else null,
                            if (reason) |f| f.description else null,
                        );
                    },
                    error.Canceled => {},
                }
                return;
            };
            switch (self.dispatch(frame)) {
                .keep_reading => {},
                .stop => return,
            }
        }
    }

    /// Routes one frame, and takes its memory.
    fn dispatch(self: *Link, frame: Frame) Routing {
        var owned = frame;

        const body = owned.body orelse {
            owned.deinit();
            return .keep_reading;
        };

        switch (body) {
            .attach => |performative| {
                self.receiveAttach(performative);
                owned.deinit();
                return .keep_reading;
            },
            .detach => |performative| {
                self.receiveDetach(performative);
                owned.deinit();
                return .stop;
            },
            else => {
                self.handler.onFrame(self.handler.context, owned);
                return .keep_reading;
            },
        }
    }

    /// Records the attach frame of the remote peer and ends the handshake.
    ///
    /// The session routes an incoming attach by the link name of section
    /// 2.6.1, so this frame belongs to this link.
    fn receiveAttach(self: *Link, performative: performatives.Attach) void {
        // Section 2.6.1 gives a link one sender endpoint and one receiver
        // endpoint, so the answer of the remote peer always names the opposite
        // role. An answer that names our own role is not a link that this
        // endpoint can use, and the handshake must not report success for it.
        if (performative.role) |role| {
            if (role == self.role) {
                self.fail(
                    error.LinkRemoteError,
                    session_mod.condition.not_allowed,
                    "the attach frame of the remote peer named our own role",
                );
                return;
            }
        }

        self.remote = .{
            .handle = performative.handle,
            .role = performative.role,
            .snd_settle_mode = performative.snd_settle_mode,
            .rcv_settle_mode = performative.rcv_settle_mode,
            .initial_delivery_count = performative.initial_delivery_count,
            .max_message_size = performative.max_message_size,
            .source_absent = performative.source == null,
            .target_absent = performative.target == null,
        };
        self.attached.set(self.io);
    }

    /// Answers the detach frame of the remote peer and ends the link.
    ///
    /// Section 2.6.6: a peer that reads a closing detach "will destroy the
    /// corresponding link endpoint, and reply with its own detach frame with
    /// the closed flag set to true". Section 2.6.4 asks for the same answer
    /// for a detach that does not close.
    fn receiveDetach(self: *Link, performative: performatives.Detach) void {
        self.remote_detach_seen.store(true, .release);
        self.sendDetach(.{ .closed = performative.closed orelse false }) catch {};

        const condition_error = performative.error_condition orelse {
            self.fail(error.LinkDetached, null, null);
            return;
        };
        const symbol = if (condition_error.condition) |text| text.text else null;
        self.fail(error.LinkRemoteError, symbol, condition_error.description);
    }

    /// Ends the link because the session under it refused a frame.
    fn noteSessionError(self: *Link, err: session_mod.SendError) void {
        const reason = self.session.failure();
        self.fail(
            error.LinkSessionFailed,
            if (reason) |f| f.condition else null,
            if (reason) |f| f.description else @errorName(err),
        );
    }
};

// -------------------------------------------------------------------------
// The helpers
// -------------------------------------------------------------------------

/// Writes a random link name into `buf` and returns it.
///
/// Section 2.6.1 makes the name the identity of the link between two
/// containers, so a link that the caller does not name takes a random one. The
/// name is the hexadecimal form of 16 random octets.
pub fn generateName(io: Io, buf: *[name_len]u8) []const u8 {
    var raw: [name_len / 2]u8 = undefined;
    io.random(&raw);
    buf.* = std.fmt.bytesToHex(raw, .lower);
    return buf;
}

/// Frees every frame that a queue still holds.
///
/// The call must not stop at a cancel request, because the caller frees the
/// frames of the queue and nothing else can.
fn drainQueue(io: Io, queue: *FrameQueue) void {
    while (true) {
        const frame = queue.getOneUncancelable(io) catch return;
        frame.deinit();
    }
}

// -------------------------------------------------------------------------
// The tests
// -------------------------------------------------------------------------

const testing = std.testing;

test "a generated name holds the hexadecimal form of 16 octets" {
    var buf: [name_len]u8 = undefined;
    const name = generateName(testing.io, &buf);
    try testing.expectEqual(name_len, name.len);
    for (name) |byte| try testing.expect(std.ascii.isHex(byte));
}

test "two generated names differ" {
    var one: [name_len]u8 = undefined;
    var two: [name_len]u8 = undefined;
    try testing.expect(!std.mem.eql(
        u8,
        generateName(testing.io, &one),
        generateName(testing.io, &two),
    ));
}

test "a deadline without an end never passes" {
    const deadline: Deadline = .start(testing.io, .none);
    try testing.expect(!deadline.passed(testing.io));
    try testing.expect(deadline.timeout() == .none);
}

test "a deadline of zero has already passed" {
    const deadline: Deadline = .start(testing.io, .{
        .duration = .{ .raw = .fromNanoseconds(0), .clock = .awake },
    });
    try testing.expect(deadline.passed(testing.io));
    try testing.expect(deadline.timeout() == .deadline);
}

test "a deadline far ahead has not passed" {
    const deadline: Deadline = .start(testing.io, .{
        .duration = .{ .raw = .fromSeconds(3600), .clock = .awake },
    });
    try testing.expect(!deadline.passed(testing.io));
}

test "the maximum message size folds a zero into no limit" {
    var link: Link = .empty;
    try testing.expectEqual(@as(?u64, null), link.maxMessageSize());
    link.remote.max_message_size = 0;
    try testing.expectEqual(@as(?u64, null), link.maxMessageSize());
    link.remote.max_message_size = 1024;
    try testing.expectEqual(@as(?u64, 1024), link.maxMessageSize());
}

test "an attach answer that names our own role fails the handshake" {
    const gpa = testing.allocator;
    var link: Link = .empty;
    link.gpa = gpa;
    link.io = testing.io;
    link.role = .sender;

    // Section 2.6.1 gives a link one sender endpoint and one receiver
    // endpoint, so an answer that names our own role is not a link that this
    // endpoint can use.
    link.receiveAttach(.{ .name = "the-link", .handle = 3, .role = .sender });

    // The waiter must wake, and it must read the failure. A guard that only
    // returned early would leave every caller of `attach` waiting.
    try testing.expect(link.attached.isSet());
    try testing.expect(!link.isAttached());
    try testing.expectError(error.LinkRemoteError, link.awaitAttached(.none));

    const reason = link.failure().?;
    try testing.expectEqual(Error.LinkRemoteError, reason.err);
    try testing.expectEqualStrings("amqp:not-allowed", reason.condition.?);

    // `deinit` skips a link that no session registered, so free the two
    // strings here.
    if (link.failure_condition) |text| gpa.free(text);
    if (link.failure_description) |text| gpa.free(text);
}
