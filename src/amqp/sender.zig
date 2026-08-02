//! The sending endpoint of an AMQP 1.0 link.
//!
//! A sender attaches to a session, waits for the credit that the receiver
//! grants, and puts one message on the wire as one or more `transfer` frames.
//! It then waits for the `disposition` frame that reports the outcome of the
//! delivery.
//!
//! Specification:
//! OASIS AMQP Version 1.0 Part 2: Transport, sections 2.6.7, 2.6.12, 2.6.14,
//! 2.7.3, 2.7.5, and 2.7.6.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html
//! OASIS AMQP Version 1.0 Part 3: Messaging, sections 3.4.2 to 3.4.5.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html
//!
//! This file names no vendor concept. The caller supplies the node address,
//! the link properties, and the capabilities.
//!
//! # The tasks
//!
//! `attach` starts the router task of the link. That task is the only code
//! that reads the queue of the link. It updates the credit from a `flow`
//! frame, and it wakes the `send` that owns the delivery of a `disposition`
//! frame.
//!
//! # The order of the calls
//!
//! **Call `Sender.deinit` before `Session.deinit`.** The router task of the
//! session pushes to the queue of the sender, so the memory of that queue must
//! stay valid until `Session.deinit` returns. `Sender.deinit` frees it, so it
//! must run first. The defer statements of a caller give that order for free
//! when the caller attaches the sender after it begins the session.
//!
//! # The locks
//!
//! Three locks guard three separate things, and a task takes them in this
//! order and no other:
//!
//! 1. `send_mutex` serializes the deliveries of this link. Section 2.6.14
//!    forbids interleaved messages on one link, so one delivery holds this
//!    lock across every transfer frame that it writes.
//! 2. `deliveries_mutex` guards the table of deliveries that wait for a
//!    disposition. A `send` holds it across the write of the first transfer
//!    frame, because that write assigns the delivery id, and the disposition
//!    of the remote peer must not arrive before the table holds the entry.
//! 3. `credit_mutex` guards the flow control state of section 2.6.7, and
//!    `credit_ready` waits on it.
//!
//! The router task takes `deliveries_mutex` and `credit_mutex`. It never takes
//! `send_mutex`, and it never takes a lock of the session, so no cycle exists.

const std = @import("std");

const connection_mod = @import("connection.zig");
const framing = @import("framing.zig");
const link_mod = @import("link.zig");
const performatives = @import("performatives.zig");
const session_mod = @import("session.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Deadline = link_mod.Deadline;
const Frame = framing.Frame;
const Io = std.Io;
const Link = link_mod.Link;
const Session = session_mod.Session;
const Writer = std.Io.Writer;

// -------------------------------------------------------------------------
// The constants
// -------------------------------------------------------------------------

/// The time that `send` waits for the credit and then for the disposition.
pub const default_send_timeout: Io.Timeout =
    .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } };

/// The largest delivery tag, in octets. Section 2.8.7: "A delivery-tag can be
/// up to 32 octets of binary data."
pub const max_delivery_tag_len: usize = 32;

// -------------------------------------------------------------------------
// The errors
// -------------------------------------------------------------------------

/// The errors of `Sender.attach`.
pub const AttachError = link_mod.RegisterError || link_mod.AttachError ||
    Io.ConcurrentError || Allocator.Error;

/// The errors of `Sender.send`.
pub const SendError = link_mod.SendError || session_mod.TransferError ||
    Allocator.Error || Io.ConcurrentError || Io.Timeout.Error || error{
    /// The payload is larger than the `max-message-size` that the remote peer
    /// named in its attach frame. Section 2.7.3 makes a larger delivery a
    /// `message-size-exceeded` link error, so this endpoint refuses it before
    /// it reaches the wire.
    MessageSizeExceeded,
    /// The negotiated maximum frame size leaves no room for message data after
    /// the frame header and the transfer performative. Raise
    /// `ConnectionOptions.max_frame_size`, or shorten the delivery tag.
    FrameTooSmall,
};

/// The errors of `Sender.detach`.
pub const DetachError = link_mod.DetachError;

// -------------------------------------------------------------------------
// The options
// -------------------------------------------------------------------------

/// The arguments of `Sender.attach`.
pub const Options = struct {
    /// The arguments that the attach frame of every link carries. `address`
    /// becomes the `target.address` of the attach frame.
    link: link_mod.Options,
    /// The delivery count that this endpoint starts from. Section 2.7.3 makes
    /// the field mandatory for a sending endpoint, and section 2.6.7 lets the
    /// sender pick any starting value.
    initial_delivery_count: u32 = 0,
    /// The largest message that this endpoint accepts, in octets. A sender
    /// receives no message, so the field normally stays null.
    max_message_size: ?u64 = null,
    /// The time that `send` waits when the caller gives no timeout of its own.
    send_timeout: Io.Timeout = default_send_timeout,
};

/// The arguments of `Sender.send` that are not the payload.
///
/// The specification gives `message-format` the default 0 and gives `send` a
/// timeout. Zig has no default argument, so the two live here.
pub const SendOptions = struct {
    /// The format of the message. Section 2.8.11 gives 0 to the message format
    /// of section 3.2 of Part 3, and a caller supplies any other value itself.
    message_format: u32 = 0,
    /// The time that the call waits for the credit and then for the
    /// disposition. It takes `Options.send_timeout` when it is null.
    timeout: ?Io.Timeout = null,
};

// -------------------------------------------------------------------------
// The outcome
// -------------------------------------------------------------------------

/// The terminal state that the receiver reported for one delivery.
///
/// Section 3.4 of Part 3 defines the four outcomes. The `received` state of
/// section 3.4.1 is not terminal, so `send` keeps waiting when it arrives and
/// this union does not hold it.
pub const Outcome = union(enum) {
    /// Section 3.4.2. The receiver took the message.
    accepted: performatives.Accepted,
    /// Section 3.4.3. The receiver refused the message. The payload names the
    /// error.
    rejected: performatives.Rejected,
    /// Section 3.4.4. The receiver did not process the message, and it made no
    /// judgement about it.
    released: performatives.Released,
    /// Section 3.4.5. The receiver did not process the message, and it asks
    /// the sender to change the message before another attempt.
    modified: performatives.Modified,
    /// The receiver settled the delivery and named no state.
    none,
};

/// The result of one `send`.
///
/// The value owns the memory that `outcome` points into, so a `rejected`
/// description and a `modified` annotation map stay readable after the frame
/// that carried them is gone. Free it with `deinit`.
pub const Result = struct {
    /// The delivery id that the session assigned. Section 2.6.12.
    delivery_id: u32,
    /// True when the receiver settled the delivery.
    settled: bool,
    /// The outcome that the receiver reported.
    outcome: Outcome,
    /// The arena that holds every slice of `outcome`.
    arena_state: std.heap.ArenaAllocator.State,
    /// The allocator that the arena takes its memory from.
    child: Allocator,

    /// Frees the arena, and thus every slice of `outcome`.
    pub fn deinit(self: Result) void {
        self.arena_state.promote(self.child).deinit();
    }
};

// -------------------------------------------------------------------------
// The pending delivery
// -------------------------------------------------------------------------

/// One delivery that waits for a disposition.
///
/// The `send` that owns the delivery holds this value on its own stack, and it
/// removes the entry from the table before it returns, so the router task
/// never writes to a stack frame that is gone.
const Pending = struct {
    /// The event that the router task sets when the outcome arrives, or when
    /// the link ends.
    done: Io.Event = .unset,
    /// The result, or null while the delivery waits.
    result: ?Result = null,
    /// True when the router task could not copy the outcome.
    out_of_memory: bool = false,
};

// -------------------------------------------------------------------------
// The sender
// -------------------------------------------------------------------------

/// One sending link endpoint.
///
/// Build it with `attach` and free it with `deinit`. The value must not move,
/// because the router task and the session both hold a pointer into it, so
/// `attach` puts it on the heap.
pub const Sender = struct {
    gpa: Allocator,
    io: Io,
    /// The shared link plumbing: the attach handshake, the detach handshake,
    /// and the terminal state.
    link: Link,
    /// The memory of the frame queue of the link.
    storage: link_mod.Storage,
    /// The time that `send` waits when the caller gives no timeout.
    send_timeout: Io.Timeout,
    /// The time that `detach` waits for the detach frame of the remote peer.
    detach_timeout: Io.Timeout,
    /// The delivery count that the attach frame of this endpoint carried.
    /// Section 2.6.7 makes it the value that a flow without a delivery count
    /// falls back to.
    initial_delivery_count: u32,

    /// The lock that serializes the deliveries of this link. Section 2.6.14.
    send_mutex: Io.Mutex,

    /// The lock that guards `pending`.
    deliveries_mutex: Io.Mutex,
    /// The deliveries that wait for a disposition, by delivery id.
    pending: std.AutoHashMapUnmanaged(u32, *Pending),

    /// The lock that guards the four fields below and `credit_ready`.
    credit_mutex: Io.Mutex,
    /// The condition that a `send` without credit waits on.
    credit_ready: Io.Condition,
    /// The delivery count of section 2.6.7. It counts messages, not frames.
    delivery_count: u32,
    /// The credit of section 2.6.7. It is the number of messages that this
    /// endpoint may still send.
    link_credit: u32,
    /// The number of messages that this endpoint has ready. Section 2.6.7 lets
    /// the sender advertise it, and this module holds it at zero.
    available: u32,
    /// The drain flag that the receiver last sent. Section 2.6.7.
    drain: bool,

    /// The next delivery tag. Section 2.6.12 asks for a tag that is unique
    /// among the deliveries that either end can hold unsettled.
    next_tag: u64,

    // ---------------------------------------------------------------------
    // The attach handshake
    // ---------------------------------------------------------------------

    /// Attaches a sending link to `session`.
    ///
    /// The call takes an output handle, sends the attach frame of section
    /// 2.7.3 with role sender, snd-settle-mode unsettled, and rcv-settle-mode
    /// first, and then it waits for the attach frame of the remote peer.
    ///
    /// `options.link.address` becomes the `target.address` of the frame, and
    /// the name of the link becomes the `source.address`. The properties and
    /// the capabilities come from the caller, and this module adds none.
    ///
    /// The call takes the allocator and the `Io` of the session, so that every
    /// layer shares both. The caller owns every slice of `options`, and the
    /// slices must live until this call returns.
    ///
    /// The result points to heap memory. Free it with `deinit`, before
    /// `Session.deinit`.
    pub fn attach(session: *Session, options: Options) AttachError!*Sender {
        const gpa = session.gpa;
        const io = session.io;

        const self = try gpa.create(Sender);
        errdefer gpa.destroy(self);

        var name_buf: [link_mod.name_len]u8 = undefined;
        const name = options.link.name orelse link_mod.generateName(io, &name_buf);

        self.* = .{
            .gpa = gpa,
            .io = io,
            .link = .empty,
            .storage = try .init(gpa, options.link.queue_capacity),
            .send_timeout = options.send_timeout,
            .detach_timeout = options.link.detach_timeout,
            .initial_delivery_count = options.initial_delivery_count,
            .send_mutex = .init,
            .deliveries_mutex = .init,
            .pending = .empty,
            .credit_mutex = .init,
            .credit_ready = .init,
            // Section 2.6.7: "The link-credit and available variables are
            // initialized to zero. The drain flag is initialized to false."
            .delivery_count = options.initial_delivery_count,
            .link_credit = 0,
            .available = 0,
            .drain = false,
            .next_tag = 0,
        };
        errdefer self.storage.deinit(gpa);
        errdefer self.pending.deinit(gpa);

        try self.link.register(.{
            .session = session,
            .queue = &self.storage.queue,
            .role = .sender,
            .name = name,
            .handler = .{
                .context = self,
                .onFrame = onFrame,
                .onEnd = onEnd,
            },
        });
        errdefer self.link.deinit();

        try self.link.start();
        try self.link.sendAttach(.{
            .name = self.link.name,
            .handle = self.link.handle,
            .role = .sender,
            // Section 2.8.2: unsettled is 0. The sender sends every delivery
            // unsettled, so the receiver reports an outcome for each one.
            .snd_settle_mode = .unsettled,
            // Section 2.8.3: first is 0. The receiver settles a delivery as
            // soon as it arrives.
            .rcv_settle_mode = .first,
            .source = .{ .address = .{ .string = self.link.name } },
            .target = .{ .address = .{ .string = options.link.address } },
            // Section 2.7.3: the field "MUST NOT be null if role is sender".
            .initial_delivery_count = options.initial_delivery_count,
            .max_message_size = options.max_message_size,
            .offered_capabilities = options.link.offered_capabilities,
            .desired_capabilities = options.link.desired_capabilities,
            .properties = options.link.properties,
        });
        try self.link.awaitAttached(options.link.attach_timeout);
        return self;
    }

    /// Frees the sender.
    ///
    /// The call ends the link, stops the router task, waits for it, gives the
    /// output handle back to the session, and frees every allocation,
    /// including `self`. It sends no detach frame, so call `detach` first for
    /// an orderly close.
    ///
    /// Call it before `Session.deinit`.
    ///
    /// No other call on this sender may run at the same time. A `send`, an
    /// `attach`, or a `detach` that is still in flight reads memory that this
    /// call frees. Let every such call return first.
    pub fn deinit(self: *Sender) void {
        const gpa = self.gpa;
        self.link.deinit();
        self.pending.deinit(gpa);
        self.storage.deinit(gpa);
        gpa.destroy(self);
    }

    // ---------------------------------------------------------------------
    // The state that a caller reads
    // ---------------------------------------------------------------------

    /// Returns the reason that the link ended, or null while it runs.
    ///
    /// The two slices of the result live until `deinit` returns.
    pub fn failure(self: *Sender) ?link_mod.Failure {
        return self.link.failure();
    }

    /// Returns the largest message that the remote peer accepts, in octets, or
    /// null when the remote peer set no limit.
    ///
    /// The value comes from the `max-message-size` field of the attach frame
    /// of the remote peer. Section 2.7.3 reads a zero and an absent field as
    /// "no maximum size", so this call folds both into null.
    pub fn maxMessageSize(self: *Sender) ?u64 {
        return self.link.maxMessageSize();
    }

    /// Returns the credit that the receiver granted and this endpoint has not
    /// used. Section 2.6.7.
    pub fn credit(self: *Sender) u32 {
        self.credit_mutex.lockUncancelable(self.io);
        defer self.credit_mutex.unlock(self.io);
        return self.link_credit;
    }

    /// Returns the delivery count of section 2.6.7.
    pub fn deliveryCount(self: *Sender) u32 {
        self.credit_mutex.lockUncancelable(self.io);
        defer self.credit_mutex.unlock(self.io);
        return self.delivery_count;
    }

    /// Runs the detach handshake of sections 2.6.4 and 2.6.6.
    ///
    /// The caller owns every slice of `options.error_condition`, and the
    /// slices must live until this call returns.
    pub fn detach(self: *Sender, options: link_mod.DetachOptions) DetachError!void {
        return self.link.detach(options, self.detach_timeout);
    }

    // ---------------------------------------------------------------------
    // The send path
    // ---------------------------------------------------------------------

    /// Sends one message and waits for its outcome.
    ///
    /// The call blocks until the receiver grants credit, then it assigns a
    /// delivery id and a delivery tag, writes the payload as one or more
    /// `transfer` frames, and waits for the `disposition` frame that reports
    /// the outcome. Section 2.6.14 sets `more` on every frame but the last one
    /// when the payload does not fit in one frame.
    ///
    /// The delivery goes unsettled, which section 2.8.2 makes the meaning of
    /// the snd-settle-mode that `attach` sends.
    ///
    /// `payload` holds the encoded message sections of Part 3. The call reads
    /// it and copies nothing, so the caller can free it after the call
    /// returns.
    ///
    /// The result owns its own memory. Free it with `Result.deinit`.
    ///
    /// The call returns `error.Timeout` when the credit or the disposition
    /// does not arrive within the timeout. A timeout leaves the delivery on
    /// the wire, so the receiver can still report an outcome that no task
    /// reads.
    pub fn send(
        self: *Sender,
        payload: []const u8,
        options: SendOptions,
    ) SendError!Result {
        if (self.link.failure()) |f| return f.err;
        if (self.maxMessageSize()) |limit| {
            if (payload.len > limit) return error.MessageSizeExceeded;
        }

        const deadline: Deadline = .start(self.io, options.timeout orelse self.send_timeout);
        try self.reserveCredit(deadline);

        var pending: Pending = .{};
        const delivery_id = try self.transmit(payload, options.message_format, &pending);
        return self.awaitOutcome(delivery_id, &pending, deadline);
    }

    /// Takes one unit of credit, and waits for it when the link has none.
    ///
    /// Section 2.6.7: "If the link-credit is less than or equal to zero, ...
    /// a sender MUST NOT send more messages." The same section makes the
    /// sender decrease the credit by the amount that it increases the delivery
    /// count by, so this call moves both.
    fn reserveCredit(self: *Sender, deadline: Deadline) SendError!void {
        // The timer runs until this call returns. The cancel must follow the
        // unlock, because the timer task takes the same lock, so this defer
        // comes first and therefore runs last.
        var timer: ?Io.Future(void) = null;
        defer if (timer) |*future| future.cancel(self.io);

        try self.credit_mutex.lock(self.io);
        defer self.credit_mutex.unlock(self.io);

        while (true) {
            if (self.link.failure()) |f| return f.err;
            if (self.link_credit > 0) {
                self.link_credit -= 1;
                self.delivery_count +%= 1;
                return;
            }
            if (deadline.passed(self.io)) return error.Timeout;
            // `std.Io.Condition` has no wait with a timeout, so one task wakes
            // every waiter when the deadline of this call passes. Each waiter
            // then reads its own deadline.
            if (timer == null and deadline.at != null) {
                timer = try self.io.concurrent(wakeAtDeadline, .{ self, deadline });
            }
            try self.credit_ready.wait(self.io, &self.credit_mutex);
        }
    }

    /// Gives one unit of credit back after a delivery that never reached the
    /// wire.
    fn releaseCredit(self: *Sender) void {
        self.credit_mutex.lockUncancelable(self.io);
        defer self.credit_mutex.unlock(self.io);
        self.link_credit += 1;
        self.delivery_count -%= 1;
    }

    /// Wakes every task that waits for credit, after the deadline passes.
    fn wakeAtDeadline(self: *Sender, deadline: Deadline) void {
        deadline.timeout().sleep(self.io) catch return;
        self.credit_mutex.lock(self.io) catch return;
        defer self.credit_mutex.unlock(self.io);
        self.credit_ready.broadcast(self.io);
    }

    /// Writes every transfer frame of one delivery, and returns the delivery
    /// id that the session assigned.
    ///
    /// Section 2.6.14: "messages transferred along a single link MUST NOT be
    /// interleaved", so the call holds `send_mutex` across every frame.
    fn transmit(
        self: *Sender,
        payload: []const u8,
        message_format: u32,
        pending: *Pending,
    ) SendError!u32 {
        try self.send_mutex.lock(self.io);
        defer self.send_mutex.unlock(self.io);

        var tag_buf: [max_delivery_tag_len]u8 = undefined;
        const tag = self.takeTag(&tag_buf);

        const first: performatives.Transfer = .{
            .handle = self.link.handle,
            .delivery_tag = .of(tag),
            .message_format = message_format,
            // Section 2.7.5: the sender sends every delivery unsettled, so the
            // receiver reports its outcome in a disposition frame.
            .settled = false,
        };
        const budget = try self.payloadBudget(first);
        const split = payload.len > budget.only;

        var head = first;
        head.more = if (split) true else null;
        const chunk = if (split) payload[0..budget.first] else payload;

        const delivery_id = self.sendFirst(head, chunk, pending) catch |err| {
            // No octet of the delivery reached the wire, so the credit goes
            // back to the link.
            self.releaseCredit();
            return err;
        };
        if (!split) return delivery_id;
        errdefer self.forget(delivery_id, pending);

        var offset = budget.first;
        while (offset < payload.len) {
            const room = payload.len - offset;
            const last = room <= budget.final;
            const take = if (last) room else budget.next;
            const body: performatives.Transfer = .{
                .handle = self.link.handle,
                .more = if (last) null else true,
            };
            self.link.session.sendTransfer(body, payload[offset..][0..take]) catch |err| {
                // Part of the delivery is on the wire, so the remote peer sits
                // in the SENDING state of figure 2.58. Section 2.6.14 lets the
                // sender abort the delivery, and the receiver then discards
                // every octet that arrived.
                //
                // An abort that fails leaves that delivery open at the remote
                // peer, and no later frame can close it. A second delivery
                // would then start inside the open one, which section 2.6.14
                // forbids, and its delivery id would land on a continuation
                // frame, which section 2.7.5 names an error. The link
                // therefore ends here, so that no later send interleaves.
                self.abort() catch {
                    self.link.fail(
                        error.LinkPartialDelivery,
                        session_mod.condition.internal_error,
                        "a delivery stopped part way and the abort failed",
                    );
                };
                return err;
            };
            offset += take;
        }
        return delivery_id;
    }

    /// Writes the first transfer frame of a delivery and records the delivery.
    ///
    /// The table must hold the entry before the frame reaches the wire, or the
    /// disposition of the remote peer arrives with no task to take it. The
    /// router task takes the same lock, so it cannot run between the write and
    /// the insert.
    fn sendFirst(
        self: *Sender,
        head: performatives.Transfer,
        chunk: []const u8,
        pending: *Pending,
    ) SendError!u32 {
        try self.deliveries_mutex.lock(self.io);
        defer self.deliveries_mutex.unlock(self.io);

        try self.pending.ensureUnusedCapacity(self.gpa, 1);
        const id = try self.link.session.sendFirstTransfer(head, chunk);
        self.pending.putAssumeCapacity(id, pending);
        return id;
    }

    /// The number of payload octets that each frame of one delivery takes.
    ///
    /// Section 2.7.5 puts the delivery id, the delivery tag, and the message
    /// format on the first transfer frame of a delivery alone, so a later
    /// frame carries a shorter performative and holds more message data.
    const Budget = struct {
        /// The room in the first frame, when more frames follow it.
        first: usize,
        /// The room in the first frame, when it carries the whole delivery.
        only: usize,
        /// The room in a later frame, when more frames follow it.
        next: usize,
        /// The room in the last frame of a delivery that takes more than one.
        final: usize,
    };

    /// Returns the payload room of every frame of one delivery.
    ///
    /// The encoding of a transfer performative does not depend on the payload,
    /// so the call sizes the performative one time for each of the four shapes
    /// that a delivery uses.
    fn payloadBudget(self: *Sender, head: performatives.Transfer) SendError!Budget {
        const limit: usize = self.link.session.connection.remote_max_frame_size;
        const overhead: usize = framing.frame_header_size;

        const later: performatives.Transfer = .{ .handle = self.link.handle };

        // `Session.sendFirstTransfer` stamps the delivery id after this call,
        // so `head` still carries a null there. A null encodes as one octet,
        // and a real id encodes as two octets up to 255 and five octets above
        // it. Sizing against the null therefore builds a first frame that
        // passes the negotiated limit for every delivery after the first one
        // of the session. Size against the largest id instead.
        var head_sized = head;
        head_sized.delivery_id = std.math.maxInt(u32);
        var head_more = head_sized;
        head_more.more = true;
        var later_more = later;
        later_more.more = true;

        const sizes = [_]usize{
            overhead + try head_more.encodedSize(),
            overhead + try head_sized.encodedSize(),
            overhead + try later_more.encodedSize(),
            overhead + try later.encodedSize(),
        };
        for (sizes) |size| {
            if (limit <= size) return error.FrameTooSmall;
        }
        return .{
            .first = limit - sizes[0],
            .only = limit - sizes[1],
            .next = limit - sizes[2],
            .final = limit - sizes[3],
        };
    }

    /// Writes the delivery tag of the next delivery into `buf`.
    ///
    /// Section 2.6.12 asks for a tag that no unsettled delivery of either end
    /// of the link uses. A counter that never repeats gives that, because the
    /// link name of section 2.6.1 already separates this link from every other
    /// one. The caller holds `send_mutex`.
    fn takeTag(self: *Sender, buf: *[max_delivery_tag_len]u8) []const u8 {
        const tag = self.next_tag;
        self.next_tag +%= 1;
        std.mem.writeInt(u64, buf[0..8], tag, .big);
        return buf[0..8];
    }

    /// Sends one transfer frame that aborts the delivery in flight. Section
    /// 2.6.14.
    fn abort(self: *Sender) session_mod.TransferError!void {
        return self.link.session.sendTransfer(.{
            .handle = self.link.handle,
            .aborted = true,
        }, "");
    }

    /// Removes one delivery from the table of the deliveries that wait.
    ///
    /// The call also frees an outcome that the router task filled in after the
    /// caller gave up, because no other task reads that memory.
    fn forget(self: *Sender, delivery_id: u32, pending: *Pending) void {
        self.deliveries_mutex.lockUncancelable(self.io);
        defer self.deliveries_mutex.unlock(self.io);

        _ = self.pending.remove(delivery_id);
        if (pending.result) |result| {
            result.deinit();
            pending.result = null;
        }
    }

    /// Waits for the disposition of one delivery.
    fn awaitOutcome(
        self: *Sender,
        delivery_id: u32,
        pending: *Pending,
        deadline: Deadline,
    ) SendError!Result {
        while (true) {
            switch (self.pollDelivery(delivery_id, pending, deadline)) {
                .ready => |result| return result,
                .failed => |err| return err,
                .waiting => {},
            }
            pending.done.waitTimeout(self.io, deadline.timeout()) catch |err| switch (err) {
                error.Canceled => {
                    self.forget(delivery_id, pending);
                    return error.Canceled;
                },
                // The call reports a spurious wakeup as a timeout too, so the
                // loop reads the clock itself.
                error.Timeout => {},
            };
        }
    }

    /// What one look at the state of a delivery found.
    const Poll = union(enum) {
        /// The outcome arrived, and the caller owns it now.
        ready: Result,
        /// The delivery still waits for its outcome.
        waiting,
        /// The wait ended, and no outcome reached the caller.
        failed: SendError,
    };

    /// Reads the state of one delivery, and takes the delivery out of the
    /// table when the wait ends.
    ///
    /// The call decides while it holds `deliveries_mutex`, which the router
    /// task takes too. An outcome that arrives at the moment of a timeout
    /// therefore either reaches the caller or never leaves the router task, so
    /// no arena goes unread and unfreed.
    fn pollDelivery(
        self: *Sender,
        delivery_id: u32,
        pending: *Pending,
        deadline: Deadline,
    ) Poll {
        self.deliveries_mutex.lockUncancelable(self.io);
        defer self.deliveries_mutex.unlock(self.io);

        if (pending.result) |result| {
            pending.result = null;
            _ = self.pending.remove(delivery_id);
            return .{ .ready = result };
        }

        const reason: SendError = if (pending.out_of_memory)
            error.OutOfMemory
        else if (self.link.failure()) |f|
            f.err
        else if (deadline.passed(self.io))
            error.Timeout
        else
            return .waiting;

        _ = self.pending.remove(delivery_id);
        return .{ .failed = reason };
    }

    // ---------------------------------------------------------------------
    // The router task
    // ---------------------------------------------------------------------

    /// Reads one frame that the shared link plumbing does not read.
    fn onFrame(context: *anyopaque, frame: Frame) void {
        const self: *Sender = @ptrCast(@alignCast(context));
        defer frame.deinit();

        const body = frame.body orelse return;
        switch (body) {
            // A drain answer is best effort. The session reports its own
            // failure to every caller of `send`, so a lost answer here needs
            // no second report.
            //
            // A cancel is the one error that this call must not drop. The
            // signal fires one time, so a `error.Canceled` that stops here
            // leaves the router with no pending signal, and the router then
            // parks in `getOne` forever. `Link.detach` cancels the group and
            // waits for the task, so it would never return. `recancel` arms
            // the request again, and the next wait then ends the router.
            .flow => |performative| if (self.receiveFlow(performative)) |answer| {
                self.link.session.sendFlow(answer) catch |err| switch (err) {
                    error.Canceled => self.io.recancel(),
                    else => {},
                };
            },
            .disposition => |performative| self.receiveDisposition(performative),
            else => {},
        }
    }

    /// Wakes every task that waits inside the sender, because the link ended.
    fn onEnd(context: *anyopaque) void {
        const self: *Sender = @ptrCast(@alignCast(context));

        {
            self.deliveries_mutex.lockUncancelable(self.io);
            defer self.deliveries_mutex.unlock(self.io);
            var it = self.pending.valueIterator();
            while (it.next()) |pending| pending.*.done.set(self.io);
        }

        self.credit_mutex.lockUncancelable(self.io);
        defer self.credit_mutex.unlock(self.io);
        self.credit_ready.broadcast(self.io);
    }

    /// Takes the flow control state of section 2.6.7 from a flow frame.
    ///
    /// The section fixes the credit of the sender with one formula:
    /// `link-credit(snd) := delivery-count(rcv) + link-credit(rcv) -
    /// delivery-count(snd)`. A receiver that does not know the delivery count
    /// leaves the field out, and the sender then reads it as the delivery
    /// count of its own attach frame.
    ///
    /// The call returns the flow state to send when the receiver asks for a
    /// drain, and null in every other case. The caller sends it after the
    /// credit lock is free, because the send takes the lock of the session.
    fn receiveFlow(self: *Sender, performative: performatives.Flow) ?session_mod.LinkFlow {
        self.credit_mutex.lockUncancelable(self.io);
        defer self.credit_mutex.unlock(self.io);

        // Section 2.6.7: a receiver that does not yet know the delivery count
        // leaves the field out, and "the sender MUST assume that the
        // delivery-count(rcv) is ... the delivery-count(snd) specified in the
        // flow state carried by the initial attach frame".
        const remote_count = performative.delivery_count orelse self.initial_delivery_count;
        const remote_credit = performative.link_credit orelse 0;
        const limit = remote_count +% remote_credit;
        const room = limit -% self.delivery_count;

        // The formula uses RFC 1982 serial arithmetic, so a limit behind the
        // delivery count gives no credit and not a very large one.
        self.link_credit = if (room >= serial_negative) 0 else room;

        // Section 2.7.4 gives the drain field the default false, so a flow
        // that leaves it out means false and not "keep the last value". A
        // value that latched would make every later grant drain itself, and
        // the link would never send again.
        self.drain = performative.drain orelse false;

        if (self.link_credit > 0) self.credit_ready.broadcast(self.io);

        // Section 2.6.7: "If the sender's drain flag is set and there are no
        // available messages, the sender MUST advance its delivery-count until
        // link-credit is zero, and send its updated flow state to the
        // receiver." This sender holds no queue, so it has no available
        // message at this moment. A receiver that runs a timed get waits for
        // this answer, and it waits forever without it.
        if (self.drain and self.link_credit > 0) {
            self.delivery_count +%= self.link_credit;
            self.link_credit = 0;
            return self.linkFlowLocked();
        }

        // Section 2.7.4: "If set to true then the receiver SHOULD send its
        // state at the earliest convenient opportunity." A flow that names
        // this handle asks for the state of this link, and the session hands
        // that frame here.
        if (performative.echo orelse false) return self.linkFlowLocked();
        return null;
    }

    /// Returns the flow state of this link. The caller holds `credit_mutex`.
    fn linkFlowLocked(self: *Sender) session_mod.LinkFlow {
        return .{
            .handle = self.link.handle,
            .delivery_count = self.delivery_count,
            .link_credit = self.link_credit,
            .available = self.available,
            .drain = self.drain,
        };
    }

    /// Gives the outcome of a disposition frame to every delivery that it
    /// names.
    ///
    /// Section 2.7.6 makes the frame cover the delivery ids from `first` to
    /// `last`, and it gives the frame no handle, so the session hands one copy
    /// to each attached link and each link picks the ids that it owns.
    fn receiveDisposition(self: *Sender, performative: performatives.Disposition) void {
        // Section 2.7.6: the role names the endpoint that the frame reports
        // on. A sender reads the frames of the receiver.
        if (performative.role) |role| {
            if (role != .receiver) return;
        }
        const first = performative.first orelse return;
        // Section 2.7.6: `last` "is taken to be the same as first" when it is
        // absent.
        const last = performative.last orelse first;

        if (performative.state) |value| {
            // Section 3.4.1 of Part 3: `received` is a state and not an
            // outcome, so the delivery is still in flight.
            if (value == .received) return;
        } else if (!(performative.settled orelse false)) {
            // The frame carries no outcome and no settlement, so it changes
            // nothing that a `send` waits for.
            return;
        }

        self.deliveries_mutex.lockUncancelable(self.io);
        defer self.deliveries_mutex.unlock(self.io);

        // The ids of the set use RFC 1982 serial arithmetic, so the test reads
        // the distance from `first` and a wrap costs nothing. The table holds
        // one entry for each delivery that waits, and a session carries few of
        // them, so the walk is cheaper than a walk of the id range.
        const width = last -% first;
        var it = self.pending.iterator();
        while (it.next()) |entry| {
            const id = entry.key_ptr.*;
            if (id -% first > width) continue;
            self.complete(id, entry.value_ptr.*, performative);
        }
    }

    /// Fills the result of one delivery and wakes the task that waits for it.
    fn complete(
        self: *Sender,
        delivery_id: u32,
        pending: *Pending,
        performative: performatives.Disposition,
    ) void {
        if (pending.result != null) return;

        const settled = performative.settled orelse false;
        const state = performative.state orelse {
            pending.result = .{
                .delivery_id = delivery_id,
                .settled = settled,
                .outcome = .none,
                .arena_state = .init,
                .child = self.gpa,
            };
            pending.done.set(self.io);
            return;
        };

        const owned = cloneState(self.gpa, state) catch {
            pending.out_of_memory = true;
            pending.done.set(self.io);
            return;
        };
        pending.result = .{
            .delivery_id = delivery_id,
            .settled = settled,
            .outcome = switch (owned.value) {
                .accepted => |body| .{ .accepted = body },
                .rejected => |body| .{ .rejected = body },
                .released => |body| .{ .released = body },
                .modified => |body| .{ .modified = body },
                // `receiveDisposition` already turned a `received` state away.
                .received => .none,
            },
            .arena_state = owned.arena_state,
            .child = owned.child,
        };
        pending.done.set(self.io);
    }
};

// -------------------------------------------------------------------------
// The helpers
// -------------------------------------------------------------------------

/// The distance at which an RFC 1982 serial difference counts as negative.
///
/// Section 2.6.7 applies RFC 1982 serial number arithmetic to the delivery
/// count, so a difference of this size or more means that the value is behind.
const serial_negative: u32 = 1 << 31;

/// Returns a delivery state that owns its own memory.
///
/// A frame holds its body in an arena, and the slices of the body point into
/// that arena, so a field by field copy would share the memory of the frame.
/// The function therefore encodes the state and reads it back into a new
/// arena.
fn cloneState(
    gpa: Allocator,
    state: performatives.DeliveryState,
) !performatives.Decoded(performatives.DeliveryState) {
    const size = try state.encodedSize();
    const bytes = try gpa.alloc(u8, size);
    defer gpa.free(bytes);

    var writer: Writer = .fixed(bytes);
    try state.encode(&writer);
    return performatives.DeliveryState.decode(gpa, bytes);
}

// -------------------------------------------------------------------------
// The test doubles
// -------------------------------------------------------------------------

const testing = std.testing;
const test_peer = @import("test_peer.zig");
const Connection = connection_mod.Connection;
const Gate = test_peer.Gate;
const Peer = test_peer.Peer;
const SentFrame = test_peer.SentFrame;
const SentFrames = test_peer.SentFrames;
const appendFrame = test_peer.appendFrame;
const remoteBegin = test_peer.remoteBegin;
const scriptOpen = test_peer.scriptOpen;
const startWatchdog = test_peer.startWatchdog;
const stopWatchdog = test_peer.stopWatchdog;

/// Runs `Sender.send` in its own task, so that a test can watch a call that
/// waits for credit.
const SendTask = struct {
    sender: *Sender,
    payload: []const u8,
    options: SendOptions = .{},
    /// The result, or null when the call failed.
    result: ?Result = null,
    /// The error of the call, or null when the call returned a result.
    err: ?anyerror = null,
    /// True after the call returned.
    done: std.atomic.Value(bool) = .init(false),

    fn run(self: *SendTask) void {
        if (self.sender.send(self.payload, self.options)) |result| {
            self.result = result;
        } else |err| {
            self.err = err;
        }
        self.done.store(true, .release);
    }

    fn deinit(self: *SendTask) void {
        if (self.result) |result| result.deinit();
    }
};

/// The attach frame that the remote peer answers with.
fn remoteAttach(name: []const u8, handle: u32, max_message_size: ?u64) framing.Body {
    return .{ .attach = .{
        .name = name,
        .handle = handle,
        .role = .receiver,
        .snd_settle_mode = .unsettled,
        .rcv_settle_mode = .first,
        .source = .{ .address = .{ .string = name } },
        .target = .{ .address = .{ .string = "the-node" } },
        .max_message_size = max_message_size,
    } };
}

/// A flow frame that grants `credit` to the link of `handle`.
fn remoteFlow(handle: u32, credit_value: u32, delivery_count: ?u32) framing.Body {
    return .{ .flow = .{
        .next_incoming_id = 0,
        .incoming_window = 100,
        .next_outgoing_id = 0,
        .outgoing_window = 200,
        .handle = handle,
        .delivery_count = delivery_count,
        .link_credit = credit_value,
    } };
}

/// A disposition frame that reports one outcome for one delivery id.
fn remoteDisposition(first: u32, state: ?performatives.DeliveryState) framing.Body {
    return .{ .disposition = .{
        .role = .receiver,
        .first = first,
        .settled = true,
        .state = state,
    } };
}

// -------------------------------------------------------------------------
// The tests of the attach handshake
// -------------------------------------------------------------------------

test "attach sends the attach frame of a sender and reads the answer" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, 1024), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const capabilities = [_]performatives.Symbol{.of("a-capability")};
    const properties = [_]types.MapEntry{.{
        .key = .{ .symbol = "a-property" },
        .value = .{ .string = "a-value" },
    }};

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
        .desired_capabilities = &capabilities,
        .properties = &properties,
    } });
    defer snd.deinit();

    var frames: SentFrames = try .parse(gpa, peer.sent());
    defer frames.deinit();

    try testing.expectEqual(@as(usize, 1), frames.count(.attach));
    const attach = frames.find(.attach).?.body.?.attach;
    try testing.expectEqualStrings("link-one", attach.name.?);
    try testing.expectEqual(@as(?u32, 0), attach.handle);
    try testing.expectEqual(performatives.Role.sender, attach.role.?);
    // Section 2.8.2: unsettled is 0. Section 2.8.3: first is 0.
    try testing.expectEqual(performatives.SenderSettleMode.unsettled, attach.snd_settle_mode.?);
    try testing.expectEqual(performatives.ReceiverSettleMode.first, attach.rcv_settle_mode.?);
    try testing.expectEqualStrings("link-one", attach.source.?.address.?.string);
    try testing.expectEqualStrings("the-node", attach.target.?.address.?.string);
    // Section 2.7.3: the field "MUST NOT be null if role is sender".
    try testing.expectEqual(@as(?u32, 0), attach.initial_delivery_count);
    try testing.expectEqualStrings("a-capability", attach.desired_capabilities.?[0].text);
    try testing.expectEqualStrings("a-property", attach.properties.?[0].key.symbol);

    try testing.expect(snd.link.isAttached());
    try testing.expectEqual(@as(?u64, 1024), snd.maxMessageSize());
    try testing.expectEqual(@as(?u32, 7), snd.link.remote.handle);
    try testing.expectEqual(performatives.Role.receiver, snd.link.remote.role.?);
    try testing.expectEqual(@as(u32, 0), snd.credit());
}

test "attach reports a timeout when the peer never answers" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");

    var gates = [_]Gate{.{ .at = begin_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    try testing.expectError(error.Timeout, Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
        .attach_timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } },
    } }));
}

// -------------------------------------------------------------------------
// The tests of the credit
// -------------------------------------------------------------------------

test "send waits without credit and proceeds when a flow grants it" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    const flow_at = sink.written().len;
    try appendFrame(&sink, 0, remoteFlow(7, 1, 0), "");
    const disposition_at = sink.written().len;
    try appendFrame(&sink, 0, remoteDisposition(0, .{ .accepted = .{} }), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = flow_at, .after_frames = Gate.manual },
        .{ .at = disposition_at, .after_frames = 4 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    // Section 2.6.7: "The link-credit and available variables are initialized
    // to zero."
    try testing.expectEqual(@as(u32, 0), snd.credit());
    try testing.expectEqual(@as(u32, 3), peer.frames());

    var task: SendTask = .{ .sender = snd, .payload = "a-message" };
    defer task.deinit();
    var future = try io.concurrent(SendTask.run, .{&task});

    // The call has no credit, so it must write no transfer frame. The wait is
    // long enough that a send which ignores the credit reaches the wire.
    try io.sleep(.fromMilliseconds(50), .awake);
    try testing.expect(!task.done.load(.acquire));
    try testing.expectEqual(@as(u32, 3), peer.frames());

    gates[2].event.set(io);
    future.await(io);

    try testing.expectEqual(@as(?anyerror, null), task.err);
    const result = task.result.?;
    try testing.expectEqual(@as(u32, 0), result.delivery_id);
    try testing.expect(result.outcome == .accepted);
    // Section 2.6.7: the sender decreases the credit by the amount that it
    // increases the delivery count by.
    try testing.expectEqual(@as(u32, 0), snd.credit());
    try testing.expectEqual(@as(u32, 1), snd.deliveryCount());
}

test "send reports a timeout when no flow grants credit" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    try testing.expectError(error.Timeout, snd.send("a-message", .{
        .timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } },
    }));
    try testing.expectEqual(@as(u32, 3), peer.frames());
}

test "the credit formula of section 2.6.7 takes the delivery count of the receiver" {
    const gpa = testing.allocator;
    const io = testing.io;

    var snd: Sender = undefined;
    snd.io = io;
    snd.gpa = gpa;
    snd.credit_mutex = .init;
    snd.credit_ready = .init;
    snd.link = .empty;
    snd.initial_delivery_count = 0;
    snd.delivery_count = 4;
    snd.link_credit = 0;
    snd.available = 0;
    snd.drain = false;

    // link-credit(snd) := delivery-count(rcv) + link-credit(rcv) -
    // delivery-count(snd).
    _ = snd.receiveFlow(.{ .delivery_count = 4, .link_credit = 10 });
    try testing.expectEqual(@as(u32, 10), snd.link_credit);

    _ = snd.receiveFlow(.{ .delivery_count = 2, .link_credit = 10 });
    try testing.expectEqual(@as(u32, 8), snd.link_credit);

    // A limit behind the delivery count gives no credit, because the section
    // uses RFC 1982 serial arithmetic.
    _ = snd.receiveFlow(.{ .delivery_count = 2, .link_credit = 1 });
    try testing.expectEqual(@as(u32, 0), snd.link_credit);

    // A flow without a delivery count falls back to the value of the attach
    // frame of this endpoint.
    _ = snd.receiveFlow(.{ .link_credit = 7 });
    try testing.expectEqual(@as(u32, 3), snd.link_credit);

    // Section 2.6.7: "If the sender's drain flag is set and there are no
    // available messages, the sender MUST advance its delivery-count until
    // link-credit is zero, and send its updated flow state to the receiver."
    // The formula gives this flow a credit of 2, and the drain then takes that
    // credit onto the delivery count and asks for a flow frame in answer.
    const answer = snd.receiveFlow(.{
        .delivery_count = 4,
        .link_credit = 2,
        .drain = true,
        .available = 5,
    });
    try testing.expectEqual(@as(u32, 0), snd.link_credit);
    try testing.expectEqual(@as(u32, 6), snd.delivery_count);
    try testing.expect(snd.drain);
    // Section 2.6.7 lets only the sender set `available`, and the receiver
    // merely echoes the last value that it saw. This module holds no queue, so
    // the value stays at zero.
    try testing.expectEqual(@as(u32, 0), snd.available);

    try testing.expect(answer != null);
    try testing.expectEqual(@as(?u32, 6), answer.?.delivery_count);
    try testing.expectEqual(@as(?u32, 0), answer.?.link_credit);
    try testing.expectEqual(@as(?bool, true), answer.?.drain);
}

test "the credit formula wraps with serial arithmetic" {
    const gpa = testing.allocator;
    const io = testing.io;

    var snd: Sender = undefined;
    snd.io = io;
    snd.gpa = gpa;
    snd.credit_mutex = .init;
    snd.credit_ready = .init;
    snd.link = .empty;
    snd.initial_delivery_count = 0;
    // This endpoint counted one delivery past the wrap.
    snd.delivery_count = 1;
    snd.link_credit = 0;
    snd.available = 0;
    snd.drain = false;

    // The receiver still sits two deliveries before the wrap, and it grants 5,
    // so the limit is three past the wrap and two past this endpoint.
    _ = snd.receiveFlow(.{ .delivery_count = std.math.maxInt(u32) - 1, .link_credit = 5 });
    try testing.expectEqual(@as(u32, 2), snd.link_credit);
}

// -------------------------------------------------------------------------
// The tests of the transfer split
// -------------------------------------------------------------------------

test "a payload larger than the frame size splits into a sequence of transfers" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    // 1200 octets do not fit in a frame of 512 octets, so the delivery takes
    // three frames.
    var payload: [1200]u8 = undefined;
    for (&payload, 0..) |*byte, index| byte.* = @truncate(index);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, 512);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, remoteFlow(7, 1, 0), "");
    const disposition_at = sink.written().len;
    try appendFrame(&sink, 0, remoteDisposition(0, .{ .accepted = .{} }), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        // The delivery takes three frames, so the disposition follows frame 6.
        .{ .at = disposition_at, .after_frames = 6 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    const result = try snd.send(&payload, .{});
    defer result.deinit();
    try testing.expect(result.outcome == .accepted);

    var frames: SentFrames = try .parse(gpa, peer.sent());
    defer frames.deinit();

    var sent: std.ArrayListUnmanaged(SentFrame) = .empty;
    defer sent.deinit(gpa);
    try frames.all(.transfer, &sent);
    try testing.expectEqual(@as(usize, 3), sent.items.len);

    // Frame 1. Section 2.7.5 puts the delivery id, the delivery tag, and the
    // message format on the first transfer of a delivery.
    const first = sent.items[0].frame.body.?.transfer;
    try testing.expectEqual(@as(?u32, 0), first.handle);
    try testing.expectEqual(@as(?u32, 0), first.delivery_id);
    try testing.expectEqual(@as(usize, 8), first.delivery_tag.?.bytes.len);
    try testing.expectEqual(@as(?u32, 0), first.message_format);
    try testing.expectEqual(@as(?bool, false), first.settled);
    try testing.expectEqual(@as(?bool, true), first.more);
    // The first frame reserves room for the largest delivery id, because the
    // session stamps the id after the budget is computed.
    try testing.expectEqual(@as(usize, 479), sent.items[0].payload.len);

    // Frame 2. Section 2.7.5 lets a continuation transfer omit all three.
    const middle = sent.items[1].frame.body.?.transfer;
    try testing.expectEqual(@as(?u32, 0), middle.handle);
    try testing.expectEqual(@as(?u32, null), middle.delivery_id);
    try testing.expectEqual(@as(?performatives.Binary, null), middle.delivery_tag);
    try testing.expectEqual(@as(?u32, null), middle.message_format);
    try testing.expectEqual(@as(?bool, true), middle.more);
    try testing.expectEqual(@as(usize, 492), sent.items[1].payload.len);

    // Frame 3. Section 2.6.14 sets `more` on every frame but the last one.
    const last = sent.items[2].frame.body.?.transfer;
    try testing.expectEqual(@as(?u32, 0), last.handle);
    try testing.expectEqual(@as(?u32, null), last.delivery_id);
    try testing.expectEqual(@as(?bool, null), last.more);
    try testing.expectEqual(@as(usize, 229), sent.items[2].payload.len);

    // Every frame fits the negotiated maximum frame size, and the three
    // payloads rebuild the message.
    var joined: std.ArrayListUnmanaged(u8) = .empty;
    defer joined.deinit(gpa);
    for (sent.items) |item| {
        const body_size = try item.frame.body.?.encodedSize();
        try testing.expect(framing.frame_header_size + body_size + item.payload.len <= 512);
        try joined.appendSlice(gpa, item.payload);
    }
    try testing.expectEqualSlices(u8, &payload, joined.items);
}

test "a payload that fits stays in one transfer frame" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    // 480 octets is the largest payload of a single frame of 512 octets. The
    // frame reserves room for the largest delivery id, because the session
    // stamps the id after the budget is computed.
    var payload: [480]u8 = undefined;
    for (&payload, 0..) |*byte, index| byte.* = @truncate(index);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, 512);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, remoteFlow(7, 1, 0), "");
    const disposition_at = sink.written().len;
    try appendFrame(&sink, 0, remoteDisposition(0, .{ .accepted = .{} }), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = disposition_at, .after_frames = 4 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    const result = try snd.send(&payload, .{});
    defer result.deinit();

    var frames: SentFrames = try .parse(gpa, peer.sent());
    defer frames.deinit();

    var sent: std.ArrayListUnmanaged(SentFrame) = .empty;
    defer sent.deinit(gpa);
    try frames.all(.transfer, &sent);
    try testing.expectEqual(@as(usize, 1), sent.items.len);
    try testing.expectEqual(@as(?bool, null), sent.items[0].frame.body.?.transfer.more);
    try testing.expectEqualSlices(u8, &payload, sent.items[0].payload);
}

test "a payload larger than the maximum message size never reaches the wire" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, 4), "");
    try appendFrame(&sink, 0, remoteFlow(7, 5, 0), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    try testing.expectEqual(@as(?u64, 4), snd.maxMessageSize());

    // The message is one octet too large, so no frame reaches the wire.
    try testing.expectError(error.MessageSizeExceeded, snd.send("12345", .{}));
    try testing.expectEqual(@as(u32, 3), peer.frames());

    // A message at the limit passes, and the call then waits for a
    // disposition that the script never sends.
    try testing.expectError(error.Timeout, snd.send("1234", .{
        .timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } },
    }));
    try testing.expectEqual(@as(u32, 4), peer.frames());
}

// -------------------------------------------------------------------------
// The tests of the outcome
// -------------------------------------------------------------------------

/// Runs one delivery against a scripted disposition and returns the result.
fn runOutcome(
    gpa: Allocator,
    io: Io,
    state: ?performatives.DeliveryState,
    settled: bool,
) !Result {
    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, remoteFlow(7, 1, 0), "");
    const disposition_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .disposition = .{
        .role = .receiver,
        .first = 0,
        .settled = settled,
        .state = state,
    } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = disposition_at, .after_frames = 4 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    return snd.send("a-message", .{});
}

test "an accepted disposition ends the send with the accepted outcome" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const result = try runOutcome(gpa, io, .{ .accepted = .{} }, true);
    defer result.deinit();

    try testing.expect(result.outcome == .accepted);
    try testing.expect(result.settled);
    try testing.expectEqual(@as(u32, 0), result.delivery_id);
}

test "a rejected disposition carries the condition and the description" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const result = try runOutcome(gpa, io, .{ .rejected = .{ .error_condition = .{
        .condition = .of("amqp:not-allowed"),
        .description = "the node refused the message",
    } } }, true);
    defer result.deinit();

    try testing.expect(result.outcome == .rejected);
    // The frame that carried the text is gone, so the result must own its own
    // copy of both slices.
    const condition_error = result.outcome.rejected.error_condition.?;
    try testing.expectEqualStrings("amqp:not-allowed", condition_error.condition.?.text);
    try testing.expectEqualStrings("the node refused the message", condition_error.description.?);
}

test "a released disposition ends the send with the released outcome" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const result = try runOutcome(gpa, io, .{ .released = .{} }, true);
    defer result.deinit();
    try testing.expect(result.outcome == .released);
}

test "a modified disposition carries its flags" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const result = try runOutcome(gpa, io, .{ .modified = .{
        .delivery_failed = true,
        .undeliverable_here = false,
    } }, false);
    defer result.deinit();

    try testing.expect(result.outcome == .modified);
    try testing.expectEqual(@as(?bool, true), result.outcome.modified.delivery_failed);
    try testing.expectEqual(@as(?bool, false), result.outcome.modified.undeliverable_here);
    try testing.expect(!result.settled);
}

// -------------------------------------------------------------------------
// The tests of the detach handshake
// -------------------------------------------------------------------------

/// Waits until the router task of a link reaches its terminal state. The
/// watchdog bounds the wait.
fn waitEnded(io: Io, snd: *Sender) !void {
    while (snd.link.failure() == null) {
        try io.sleep(.fromMilliseconds(1), .awake);
    }
}

test "a remote detach with an error surfaces the condition and the description" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, .{ .detach = .{
        .handle = 7,
        .closed = true,
        .error_condition = .{
            .condition = .of("amqp:link:detach-forced"),
            .description = "the node went away",
        },
    } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    try waitEnded(io, snd);
    const reason = snd.failure().?;
    try testing.expectEqual(link_mod.Error.LinkRemoteError, reason.err);
    try testing.expectEqualStrings("amqp:link:detach-forced", reason.condition.?);
    try testing.expectEqualStrings("the node went away", reason.description.?);

    // Section 2.6.6: the peer that reads a closing detach answers with its own
    // closing detach.
    var frames: SentFrames = try .parse(gpa, peer.sent());
    defer frames.deinit();
    try testing.expectEqual(@as(usize, 1), frames.count(.detach));
    const answer = frames.find(.detach).?.body.?.detach;
    try testing.expectEqual(@as(?u32, 0), answer.handle);
    try testing.expectEqual(@as(?bool, true), answer.closed);

    // A send on a link that ended reports the reason of the link.
    try testing.expectError(error.LinkRemoteError, snd.send("a-message", .{}));
}

test "the detach handshake sends one closing detach and waits for the answer" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    const detach_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .detach = .{ .handle = 7, .closed = true } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = detach_at, .after_frames = 4 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    try snd.detach(.{ .closed = true });
    try testing.expectEqual(link_mod.Error.LinkDetached, snd.failure().?.err);

    var frames: SentFrames = try .parse(gpa, peer.sent());
    defer frames.deinit();
    // The answer of the remote peer must draw no second detach frame.
    try testing.expectEqual(@as(usize, 1), frames.count(.detach));
    const sent_detach = frames.find(.detach).?.body.?.detach;
    try testing.expectEqual(@as(?u32, 0), sent_detach.handle);
    try testing.expectEqual(@as(?bool, true), sent_detach.closed);
}

test "detach reports a timeout when the peer sends no detach" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
        .detach_timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } },
    } });
    defer snd.deinit();

    try testing.expectError(error.Timeout, snd.detach(.{}));
    try testing.expectEqual(link_mod.Error.LinkDetached, snd.failure().?.err);
}

test "two deliveries on one link take two delivery ids" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, remoteFlow(7, 4, 0), "");
    const first_at = sink.written().len;
    try appendFrame(&sink, 0, remoteDisposition(0, .{ .accepted = .{} }), "");
    const second_at = sink.written().len;
    try appendFrame(&sink, 0, remoteDisposition(1, .{ .released = .{} }), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = first_at, .after_frames = 4 },
        .{ .at = second_at, .after_frames = 5 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    const first = try snd.send("one", .{});
    defer first.deinit();
    try testing.expectEqual(@as(u32, 0), first.delivery_id);
    try testing.expect(first.outcome == .accepted);

    const second = try snd.send("two", .{});
    defer second.deinit();
    try testing.expectEqual(@as(u32, 1), second.delivery_id);
    try testing.expect(second.outcome == .released);

    try testing.expectEqual(@as(u32, 2), snd.deliveryCount());
    try testing.expectEqual(@as(u32, 2), snd.credit());

    var frames: SentFrames = try .parse(gpa, peer.sent());
    defer frames.deinit();
    var sent: std.ArrayListUnmanaged(SentFrame) = .empty;
    defer sent.deinit(gpa);
    try frames.all(.transfer, &sent);
    try testing.expectEqual(@as(usize, 2), sent.items.len);
    // Section 2.6.12 asks for a delivery tag that no other unsettled delivery
    // of the link uses.
    try testing.expect(!std.mem.eql(
        u8,
        sent.items[0].frame.body.?.transfer.delivery_tag.?.bytes,
        sent.items[1].frame.body.?.transfer.delivery_tag.?.bytes,
    ));
}

test "a received state leaves the delivery in flight" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, remoteFlow(7, 1, 0), "");
    const disposition_at = sink.written().len;
    // Section 3.4.1 of Part 3 makes `received` a state and not an outcome, so
    // the delivery is still in flight and the terminal state follows it.
    try appendFrame(&sink, 0, .{ .disposition = .{
        .role = .receiver,
        .first = 0,
        .settled = false,
        .state = .{ .received = .{ .section_number = 0, .section_offset = 4 } },
    } }, "");
    try appendFrame(&sink, 0, remoteDisposition(0, .{ .accepted = .{} }), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = disposition_at, .after_frames = 4 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    const result = try snd.send("a-message", .{});
    defer result.deinit();

    // The `received` frame must not end the call, so the accepted frame that
    // follows it is the one that the call reports.
    try testing.expect(result.outcome == .accepted);
    try testing.expect(result.settled);
}

test "a disposition without a state and without settlement changes nothing" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, remoteFlow(7, 1, 0), "");
    const disposition_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .disposition = .{
        .role = .receiver,
        .first = 0,
        .settled = false,
    } }, "");
    // A disposition of the sender role reports on a sending endpoint of the
    // remote peer, so this link must step over it.
    try appendFrame(&sink, 0, .{ .disposition = .{
        .role = .sender,
        .first = 0,
        .settled = true,
        .state = .{ .accepted = .{} },
    } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = disposition_at, .after_frames = 4 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    try testing.expectError(error.Timeout, snd.send("a-message", .{
        .timeout = .{ .duration = .{ .raw = .fromMilliseconds(60), .clock = .awake } },
    }));
}

test "a remote detach wakes a send that waits for its disposition" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, remoteFlow(7, 1, 0), "");
    const detach_at = sink.written().len;
    // The peer detaches instead of reporting an outcome, so the call must end
    // with the reason of the link and not with a timeout.
    try appendFrame(&sink, 0, .{ .detach = .{
        .handle = 7,
        .closed = true,
        .error_condition = .{
            .condition = .of("amqp:link:detach-forced"),
            .description = "the node went away",
        },
    } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = detach_at, .after_frames = 4 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    // The timeout is far longer than the detach takes, so a call that waited
    // for the clock instead of the link would fail this test.
    try testing.expectError(error.LinkRemoteError, snd.send("a-message", .{
        .timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } },
    }));
    try testing.expectEqualStrings("amqp:link:detach-forced", snd.failure().?.condition.?);
}

test "the session under a link wakes a send that waits for its disposition" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, remoteFlow(7, 1, 0), "");
    const end_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .end = .{ .error_condition = .{
        .condition = .of("amqp:internal-error"),
        .description = "the session broke",
    } } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = end_at, .after_frames = 4 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    // The timeout is far longer than the end frame takes, so a call that
    // waited for the clock instead of the session would fail this test.
    const started: Io.Clock.Timestamp = .now(io, .awake);
    try testing.expectError(error.LinkSessionFailed, snd.send("a-message", .{
        .timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } },
    }));
    // The end frame must wake the call at once. A call that ran down its
    // timeout instead would take the full five seconds.
    const elapsed = started.untilNow(io).raw.nanoseconds;
    try testing.expect(elapsed < std.time.ns_per_s * 2);

    const reason = snd.failure().?;
    try testing.expectEqualStrings("amqp:internal-error", reason.condition.?);
}

test "the shared plumbing runs the attach handshake without a concrete link" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var link_storage: link_mod.Storage = try .init(gpa, 8);
    defer link_storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    // The session binds the input handle from the attach frame, so the peer
    // sends one and then detaches without a second frame in between.
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, .{ .detach = .{ .handle = 7, .closed = true } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    // The shared plumbing works without a concrete link, so a caller that
    // wants the handshake alone drives `Link` itself.
    var bare: Link = .empty;
    defer bare.deinit();
    try bare.register(.{
        .session = session,
        .queue = &link_storage.queue,
        .role = .sender,
        .name = "link-one",
        .handler = link_mod.null_handler,
    });
    try bare.start();
    try bare.sendAttach(.{
        .name = bare.name,
        .handle = bare.handle,
        .role = .sender,
        .initial_delivery_count = 0,
        .target = .{ .address = .{ .string = "the-node" } },
    });
    // The attach frame arrives first, so the handshake ends, and the detach
    // frame that follows it ends the link.
    try bare.awaitAttached(.{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try testing.expectEqual(@as(?u32, 7), bare.remote.handle);
}

test "a session that ends under the attach handshake ends the wait" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const end_at = sink.written().len;
    // The peer answers the attach frame with an end frame, so the attach frame
    // of the remote peer can never arrive.
    try appendFrame(&sink, 0, .{ .end = .{ .error_condition = .{
        .condition = .of("amqp:internal-error"),
        .description = "the session broke",
    } } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = end_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    // The timeout is far longer than the end frame takes, so a call that waited
    // for the clock instead of the link would fail this test.
    try testing.expectError(error.LinkSessionFailed, Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
        .attach_timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } },
    } }));
}
test "a split delivery with a delivery id above zero still fits the frame size" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var payload: [1200]u8 = undefined;
    for (&payload, 0..) |*byte, index| byte.* = @truncate(index);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, 512);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, remoteFlow(7, 2, 0), "");
    const first_at = sink.written().len;
    try appendFrame(&sink, 0, remoteDisposition(0, .{ .accepted = .{} }), "");
    const second_at = sink.written().len;
    try appendFrame(&sink, 0, remoteDisposition(1, .{ .accepted = .{} }), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = first_at, .after_frames = 4 },
        // The split delivery takes three frames, so its disposition follows
        // frame 7.
        .{ .at = second_at, .after_frames = 7 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    const first = try snd.send("one", .{});
    defer first.deinit();
    try testing.expectEqual(@as(u32, 0), first.delivery_id);

    // The second delivery takes the delivery id 1, which encodes as two
    // octets where a null takes one. The budget must leave room for it.
    const second = try snd.send(&payload, .{});
    defer second.deinit();
    try testing.expectEqual(@as(u32, 1), second.delivery_id);
    try testing.expect(second.outcome == .accepted);
}

test "a split delivery that the remote window stops ends the link" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: session_mod.Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var payload: [1200]u8 = undefined;
    for (&payload, 0..) |*byte, index| byte.* = @truncate(index);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, 512);
    const begin_at = sink.written().len;
    // The remote peer opens an incoming window of two frames, and the delivery
    // takes three. The third transfer therefore stops, and the abort of
    // section 2.6.14 stops for the same reason.
    try appendFrame(&sink, 0, .{ .begin = .{
        .remote_channel = 0,
        .next_outgoing_id = 0,
        .incoming_window = 2,
        .outgoing_window = 200,
    } }, "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach("link-one", 7, null), "");
    try appendFrame(&sink, 0, .{ .flow = .{
        .next_incoming_id = 0,
        .incoming_window = 2,
        .next_outgoing_id = 0,
        .outgoing_window = 200,
        .handle = 7,
        .delivery_count = 0,
        .link_credit = 5,
    } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const snd = try Sender.attach(session, .{ .link = .{
        .name = "link-one",
        .address = "the-node",
    } });
    defer snd.deinit();

    try testing.expectError(error.RemoteWindowClosed, snd.send(&payload, .{}));

    // The remote peer holds an open delivery that no frame can close, so the
    // link refuses every later send. A link that stayed usable would start a
    // second delivery inside the open one.
    const reason = snd.link.failure();
    try testing.expect(reason != null);
    try testing.expectEqual(link_mod.Error.LinkPartialDelivery, reason.?.err);
    try testing.expectError(error.LinkPartialDelivery, snd.send("later", .{}));
}

test "a credit grant after a drain restores the credit" {
    const gpa = testing.allocator;
    const io = testing.io;

    var snd: Sender = undefined;
    snd.io = io;
    snd.gpa = gpa;
    snd.credit_mutex = .init;
    snd.credit_ready = .init;
    snd.link = .empty;
    snd.initial_delivery_count = 0;
    snd.delivery_count = 0;
    snd.link_credit = 0;
    snd.available = 0;
    snd.drain = false;

    // A drain takes the credit that it granted, and it asks for an answer.
    const drained = snd.receiveFlow(.{ .delivery_count = 0, .link_credit = 2, .drain = true });
    try testing.expect(drained != null);
    try testing.expectEqual(@as(u32, 0), snd.link_credit);
    try testing.expectEqual(@as(u32, 2), snd.delivery_count);

    // Section 2.7.4 gives the drain field the default false, so this grant
    // carries no drain. A drain value that latched would take this credit too,
    // and the link would never send again.
    const granted = snd.receiveFlow(.{ .delivery_count = 2, .link_credit = 5 });
    try testing.expect(granted == null);
    try testing.expect(!snd.drain);
    try testing.expectEqual(@as(u32, 5), snd.link_credit);
    try testing.expectEqual(@as(u32, 2), snd.delivery_count);
}
