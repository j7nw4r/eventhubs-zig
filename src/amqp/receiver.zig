//! The receiving endpoint of an AMQP 1.0 link.
//!
//! A receiver attaches to a session, grants credit to the remote sender, joins
//! the `transfer` frames of one delivery into one message, and hands that
//! message to the caller. It then reports the outcome of the delivery in a
//! `disposition` frame, unless the sender settled the delivery itself.
//!
//! Specification:
//! OASIS AMQP Version 1.0 Part 2: Transport, sections 2.6.3, 2.6.7, 2.6.9,
//! 2.6.12, 2.6.14, 2.7.3, 2.7.4, 2.7.5, and 2.7.6.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html
//! OASIS AMQP Version 1.0 Part 3: Messaging, sections 3.4.2 to 3.4.5, 3.5.3,
//! and 3.5.8.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html
//!
//! This file names no vendor concept. The caller supplies the node address, the
//! link properties, the capabilities, and every source filter. `StringFilter`
//! builds one filter entry from a descriptor symbol and a string value, and it
//! names no filter itself.
//!
//! # The tasks
//!
//! `attach` starts the router task of the link. That task is the only code that
//! reads the queue of the link. It joins the transfer frames of one delivery,
//! puts the finished delivery in an `Io.Queue`, tops the credit up, and sends
//! the automatic accept.
//!
//! # The backpressure
//!
//! The queue of deliveries holds `prefetch` messages. The router task waits
//! when that queue is full, and it therefore grants no more credit until a
//! caller takes a delivery out. The wait moves back through the queue of the
//! link and the queue of the session to the socket, which is where the remote
//! peer reads it.
//!
//! # The order of the calls
//!
//! **Call `Receiver.deinit` before `Session.deinit`.** The router task of the
//! session pushes to the queue of the receiver, so the memory of that queue
//! must stay valid until `Session.deinit` returns. `Receiver.deinit` frees it,
//! so it must run first. The defer statements of a caller give that order for
//! free when the caller attaches the receiver after it begins the session.
//!
//! # The locks
//!
//! Two locks guard two separate things, and a task takes them in this order and
//! no other:
//!
//! 1. `receive_mutex` guards the wait of `receive`, and `arrived` waits on it.
//! 2. `credit_mutex` guards the flow control state of section 2.6.7.
//!
//! The router task takes `receive_mutex` only after it put a delivery in the
//! queue, and it never holds one of the two locks while it waits, so no cycle
//! exists.

const std = @import("std");

const connection_mod = @import("connection.zig");
const framing = @import("framing.zig");
const link_mod = @import("link.zig");
const message_mod = @import("message.zig");
const performatives = @import("performatives.zig");
const session_mod = @import("session.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Deadline = link_mod.Deadline;
const Frame = framing.Frame;
const Io = std.Io;
const Link = link_mod.Link;
const MapEntry = types.MapEntry;
const Session = session_mod.Session;
const Value = types.Value;

// -------------------------------------------------------------------------
// The constants
// -------------------------------------------------------------------------

/// The number of messages that a receiver asks for when the caller names no
/// prefetch. Section 2.6.9 calls the value the target amount of link credit.
pub const default_prefetch: u32 = 100;

/// The error condition symbols that this module sends. Sections 2.8.15 and
/// 2.8.18 define them.
pub const condition = struct {
    /// The peer sent a delivery larger than the maximum message size of this
    /// endpoint.
    pub const message_size_exceeded = "amqp:link:message-size-exceeded";
    /// The peer sent octets that this endpoint cannot decode.
    pub const decode_error = "amqp:decode-error";
};

/// The distance at which an RFC 1982 serial difference counts as negative.
///
/// Section 2.6.7 applies RFC 1982 serial number arithmetic to the delivery
/// count, so a difference of this size or more means that the value is behind.
const serial_negative: u32 = 1 << 31;

// -------------------------------------------------------------------------
// The errors
// -------------------------------------------------------------------------

/// The errors of `Receiver.attach`.
pub const AttachError = link_mod.RegisterError || link_mod.AttachError ||
    Io.ConcurrentError || Allocator.Error;

/// The errors of `Receiver.receive`.
pub const ReceiveError = link_mod.Error || Allocator.Error || Io.Cancelable ||
    Io.ConcurrentError;

/// The errors of the four disposition calls.
pub const DispositionError = link_mod.SendError;

/// The errors of `Receiver.detach`.
pub const DetachError = link_mod.DetachError;

// -------------------------------------------------------------------------
// The source filter
// -------------------------------------------------------------------------

/// One entry of the `filter-set` of section 3.5.8, with a string value.
///
/// Section 3.5.8: "every key in the map MUST be of type symbol, every value
/// MUST be either null or of a described type which provides the archetype
/// filter". This helper builds such an entry from a descriptor symbol and a
/// string value, and the entry takes the descriptor symbol for its key too,
/// which is the common convention. A caller that wants another key builds the
/// `MapEntry` itself.
///
/// The helper names no filter. The caller supplies the descriptor and the
/// value, so the vendor filter names live in the layer above this one.
///
/// The helper allocates nothing. The caller owns the two slices, and the value
/// must not move after `entry`, because the entry holds pointers into it. Both
/// must live until the attach call returns.
///
/// ```
/// var filter: StringFilter = .init("a.org:a-filter:string", "a > 1");
/// const filters = [_]MapEntry{filter.entry()};
/// ```
pub const StringFilter = struct {
    /// The descriptor of the described value, as a symbol.
    descriptor: Value,
    /// The described value, as a string.
    value: Value,

    /// Returns a filter that holds `descriptor` and `value`.
    pub fn init(descriptor: []const u8, value: []const u8) StringFilter {
        return .{
            .descriptor = .{ .symbol = descriptor },
            .value = .{ .string = value },
        };
    }

    /// Returns the map entry of this filter. The result points into `self`, so
    /// `self` must not move after this call.
    pub fn entry(self: *const StringFilter) MapEntry {
        return .{
            .key = self.descriptor,
            .value = .{ .described = .{
                .descriptor = &self.descriptor,
                .value = &self.value,
            } },
        };
    }
};

// -------------------------------------------------------------------------
// The options
// -------------------------------------------------------------------------

/// The arguments of `Receiver.attach`.
pub const Options = struct {
    /// The arguments that the attach frame of every link carries. `address`
    /// becomes the `source.address` of the attach frame.
    link: link_mod.Options,
    /// The settlement policy that this endpoint asks the remote sender for.
    ///
    /// Section 2.7.3: "When set at the receiver this indicates the desired
    /// value for the settlement mode at the sender." A sender that supports the
    /// mode should respect it, and it reports the mode that it really uses in
    /// its own attach frame. Read `senderSettleMode` for that answer.
    ///
    /// Give `settled` for a peer that pre-settles every delivery, and
    /// `unsettled` for a peer that waits for a disposition.
    snd_settle_mode: performatives.SenderSettleMode = .unsettled,
    /// The filters of the source. The caller supplies every entry, and
    /// `StringFilter` builds one. The entries live until this call returns.
    filter: ?[]const MapEntry = null,
    /// The number of messages that this endpoint asks for. Section 2.6.9 calls
    /// it the target amount of link credit. A prefetch of zero grants no credit
    /// at all, and no message then arrives.
    prefetch: u32 = default_prefetch,
    /// True when the receiver accepts each unsettled delivery as it arrives.
    /// Section 2.7.5 lets a receiver with rcv-settle-mode first settle a
    /// delivery as soon as it has arrived.
    auto_accept: bool = true,
    /// The largest message that this endpoint accepts, in octets. Section 2.7.3
    /// makes a larger delivery a `message-size-exceeded` link error, and this
    /// module ends the link with that condition when a delivery passes the
    /// limit. A null value sets no limit.
    max_message_size: ?u64 = null,
};

/// The arguments of `Receiver.modify`. Specification section 3.4.5.
pub const ModifyOptions = struct {
    /// True when the delivery attempt failed.
    delivery_failed: ?bool = null,
    /// True when this receiver cannot take the message again.
    undeliverable_here: ?bool = null,
    /// The annotations that the sender adds to the message before it tries
    /// again. The caller owns every entry, and the entries live until the call
    /// returns.
    message_annotations: ?[]const MapEntry = null,
};

// -------------------------------------------------------------------------
// The delivery
// -------------------------------------------------------------------------

/// One message that arrived on the link.
///
/// The value owns its memory. Free it with `deinit`, and free it after the
/// disposition call that reports its outcome, because that call reads the
/// delivery id.
///
/// `bytes` holds the payload octets of every transfer frame of the delivery,
/// joined in order. `message` is the same octets, read into the message model
/// of section 3.2 of Part 3, and its slices point into memory of this value and
/// not into `bytes`.
pub const Delivery = struct {
    /// The delivery id that the remote peer gave the delivery. Section 2.6.12.
    delivery_id: u32,
    /// True when the delivery arrived settled, which section 2.7.5 makes the
    /// meaning of the `settled` flag of its transfer frames. A settled delivery
    /// needs no disposition.
    settled: bool,
    /// The joined payload octets of the delivery. The value owns them.
    bytes: []const u8,
    /// The decoded message.
    message: message_mod.Message,
    /// The arena that holds every slice of `message`.
    arena_state: std.heap.ArenaAllocator.State,
    /// The allocator that holds `bytes` and the arena.
    gpa: Allocator,

    /// Frees the raw octets and the decoded message.
    pub fn deinit(self: Delivery) void {
        self.gpa.free(self.bytes);
        self.arena_state.promote(self.gpa).deinit();
    }
};

/// The delivery that the router task is joining now.
///
/// Section 2.6.14 forbids interleaved deliveries on one link, so one link joins
/// one delivery at a time. The router task is the only user of this value.
const Assembly = struct {
    /// True between the first transfer frame of a delivery and its last one.
    active: bool = false,
    /// The delivery id of the first transfer frame.
    delivery_id: u32 = 0,
    /// True when a transfer frame of this delivery carried `settled`.
    settled: bool = false,
    /// True when this delivery reaches no caller. An aborted delivery and a
    /// resumed delivery both take this path.
    dropped: bool = false,
    /// The payload octets that arrived so far.
    bytes: std.ArrayListUnmanaged(u8) = .empty,
};

// -------------------------------------------------------------------------
// The receiver
// -------------------------------------------------------------------------

/// One receiving link endpoint.
///
/// Build it with `attach` and free it with `deinit`. The value must not move,
/// because the router task and the session both hold a pointer into it, so
/// `attach` puts it on the heap.
pub const Receiver = struct {
    /// The allocator of the session. Every delivery comes from it.
    gpa: Allocator,
    /// The `Io` of the session. Every task and every lock uses it.
    io: Io,
    /// The shared link plumbing: the attach handshake, the detach handshake,
    /// and the terminal state.
    link: Link,
    /// The memory of the frame queue of the link.
    storage: link_mod.Storage,
    /// The time that `detach` waits for the detach frame of the remote peer.
    detach_timeout: Io.Timeout,

    /// The number of messages that this endpoint asks for. Section 2.6.9.
    prefetch: u32,
    /// True when the receiver accepts each unsettled delivery on receipt.
    auto_accept: bool,
    /// The largest message that this endpoint accepts, in octets, or null.
    max_message_size: ?u64,

    /// The slots of `deliveries`.
    delivery_slots: []Delivery,
    /// The deliveries that arrived and that no caller took yet.
    deliveries: Io.Queue(Delivery),

    /// The lock that guards the wait of `receive`.
    receive_mutex: Io.Mutex,
    /// The condition that a `receive` without a delivery waits on.
    arrived: Io.Condition,
    /// True after the router task failed to hold a delivery. The next
    /// `receive` reports it and clears it.
    out_of_memory: std.atomic.Value(bool),

    /// The lock that guards the three fields below.
    credit_mutex: Io.Mutex,
    /// The delivery count of section 2.6.7. It counts messages, not frames.
    delivery_count: u32,
    /// The credit of section 2.6.7. It is the number of messages that the
    /// remote sender may still send.
    link_credit: u32,
    /// The number of messages that the remote sender reported as ready.
    available: u32,

    /// The delivery that the router task is joining now.
    assembly: Assembly,

    // ---------------------------------------------------------------------
    // The attach handshake
    // ---------------------------------------------------------------------

    /// Attaches a receiving link to `session`.
    ///
    /// The call takes an output handle, sends the attach frame of section 2.7.3
    /// with role receiver, the requested snd-settle-mode of the caller, and
    /// rcv-settle-mode first, and then it waits for the attach frame of the
    /// remote peer. It then sends the first `flow` frame, which grants
    /// `options.prefetch` credit.
    ///
    /// `options.link.address` becomes the `source.address` of the frame, and the
    /// name of the link becomes the `target.address`. The filter, the
    /// properties, and the capabilities come from the caller, and this module
    /// adds none.
    ///
    /// The call returns `error.LinkRefused` when the remote peer answers with no
    /// source. Section 2.6.3 makes a null local terminus the answer of a peer
    /// that refuses the link, and the source is the local terminus of the
    /// remote sender.
    ///
    /// The call takes the allocator and the `Io` of the session, so that every
    /// layer shares both. The caller owns every slice of `options`, and the
    /// slices must live until this call returns.
    ///
    /// The result points to heap memory. Free it with `deinit`, before
    /// `Session.deinit`.
    pub fn attach(session: *Session, options: Options) AttachError!*Receiver {
        const gpa = session.gpa;
        const io = session.io;

        const self = try gpa.create(Receiver);
        errdefer gpa.destroy(self);

        var name_buf: [link_mod.name_len]u8 = undefined;
        const name = options.link.name orelse link_mod.generateName(io, &name_buf);

        // The queue holds one window of messages, so the sender can use every
        // unit of credit that this endpoint grants before a caller reads one.
        const slots = try gpa.alloc(Delivery, @max(options.prefetch, 1));
        errdefer gpa.free(slots);

        self.* = .{
            .gpa = gpa,
            .io = io,
            .link = .empty,
            .storage = try .init(gpa, options.link.queue_capacity),
            .detach_timeout = options.link.detach_timeout,
            .prefetch = options.prefetch,
            .auto_accept = options.auto_accept,
            .max_message_size = options.max_message_size,
            .delivery_slots = slots,
            .deliveries = .init(slots),
            .receive_mutex = .init,
            .arrived = .init,
            .out_of_memory = .init(false),
            .credit_mutex = .init,
            // Section 2.6.7: "The link-credit and available variables are
            // initialized to zero." The delivery count of a receiver comes from
            // the attach frame of the sender, which has not arrived yet.
            .delivery_count = 0,
            .link_credit = 0,
            .available = 0,
            .assembly = .{},
        };
        errdefer self.storage.deinit(gpa);
        // The router task runs from `start` below, so a peer that sends a
        // transfer frame before it reads the flow frame of this endpoint can
        // fill the queue even on a path that fails. This runs after the
        // `errdefer` below it, which stops that task.
        errdefer self.discardDeliveries();

        try self.link.register(.{
            .session = session,
            .queue = &self.storage.queue,
            .role = .receiver,
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
            .role = .receiver,
            .snd_settle_mode = options.snd_settle_mode,
            // Section 2.8.3: first is 0. Section 2.7.5: with first "the
            // receiver MUST settle the delivery once it has arrived without
            // waiting for the sender to settle first".
            .rcv_settle_mode = .first,
            .source = .{
                .address = .{ .string = options.link.address },
                .filter = options.filter,
            },
            .target = .{ .address = .{ .string = self.link.name } },
            // Section 2.7.3: the initial delivery count "MUST NOT be null if
            // role is sender, and it is ignored if the role is receiver".
            .max_message_size = options.max_message_size,
            .offered_capabilities = options.link.offered_capabilities,
            .desired_capabilities = options.link.desired_capabilities,
            .properties = options.link.properties,
        });
        try self.link.awaitAttached(options.link.attach_timeout);

        // Section 2.6.3: a peer that will not create a terminus answers with a
        // null local terminus and then detaches. The source is the local
        // terminus of the remote sender, so a missing source refuses this link.
        if (self.link.remote.source_absent) {
            self.link.fail(
                error.LinkRefused,
                null,
                "the attach frame of the remote peer carried no source",
            );
            self.link.detach(.{ .closed = true }, options.link.detach_timeout) catch {};
            return error.LinkRefused;
        }

        try self.grantInitialCredit();
        return self;
    }

    /// Sends the first flow frame of the link, which grants the prefetch.
    ///
    /// Section 2.6.7: "The receiver initializes its delivery-count upon
    /// receiving the sender's attach", so the count comes from the frame that
    /// `awaitAttached` waited for.
    fn grantInitialCredit(self: *Receiver) link_mod.SendError!void {
        self.credit_mutex.lockUncancelable(self.io);
        self.delivery_count = self.link.remote.initial_delivery_count orelse 0;
        self.link_credit = self.prefetch;
        const flow = self.linkFlowLocked();
        self.credit_mutex.unlock(self.io);

        if (self.prefetch == 0) return;
        return self.link.sendFlow(flow);
    }

    /// Frees the receiver.
    ///
    /// The call ends the link, stops the router task, waits for it, gives the
    /// output handle back to the session, frees every delivery that no caller
    /// took, and frees every allocation, including `self`. It sends no detach
    /// frame, so call `detach` first for an orderly close.
    ///
    /// Call it before `Session.deinit`.
    ///
    /// No other call on this receiver may run at the same time. A `receive`, an
    /// `attach`, or a `detach` that is still in flight reads memory that this
    /// call frees. Let every such call return first.
    pub fn deinit(self: *Receiver) void {
        const gpa = self.gpa;
        self.link.deinit();
        self.discardDeliveries();
        gpa.free(self.delivery_slots);
        self.storage.deinit(gpa);
        gpa.destroy(self);
    }

    /// Frees every delivery that no caller took, and the octets of the delivery
    /// that the router task was joining.
    ///
    /// The call closes the queue, so a router task that waits for room in it
    /// wakes. Stop that task first, or it can put one more delivery in.
    fn discardDeliveries(self: *Receiver) void {
        self.deliveries.close(self.io);
        while (true) {
            const delivery = self.deliveries.getOneUncancelable(self.io) catch break;
            delivery.deinit();
        }
        self.assembly.bytes.deinit(self.gpa);
    }

    // ---------------------------------------------------------------------
    // The state that a caller reads
    // ---------------------------------------------------------------------

    /// Returns the reason that the link ended, or null while it runs.
    ///
    /// The two slices of the result live until `deinit` returns.
    pub fn failure(self: *Receiver) ?link_mod.Failure {
        return self.link.failure();
    }

    /// Returns the largest message that the remote peer accepts, in octets, or
    /// null when the remote peer set no limit.
    ///
    /// A receiver sends no message, so the value matters only to a caller that
    /// reads it for a report.
    pub fn maxMessageSize(self: *Receiver) ?u64 {
        return self.link.maxMessageSize();
    }

    /// Returns the settlement policy that the remote sender really uses.
    ///
    /// Section 2.7.3: "When set at the sender this indicates the actual
    /// settlement mode in use." The specification gives the field the default
    /// `mixed`, so an attach frame that leaves it out means mixed and not the
    /// mode that this endpoint asked for. Under `mixed` the sender settles the
    /// deliveries that it chooses, so read `Delivery.settled` for each one.
    pub fn senderSettleMode(self: *Receiver) performatives.SenderSettleMode {
        return self.link.remote.snd_settle_mode orelse .mixed;
    }

    /// Returns the credit that this endpoint granted and the remote sender has
    /// not used. Section 2.6.7.
    pub fn credit(self: *Receiver) u32 {
        self.credit_mutex.lockUncancelable(self.io);
        defer self.credit_mutex.unlock(self.io);
        return self.link_credit;
    }

    /// Returns the delivery count of section 2.6.7.
    pub fn deliveryCount(self: *Receiver) u32 {
        self.credit_mutex.lockUncancelable(self.io);
        defer self.credit_mutex.unlock(self.io);
        return self.delivery_count;
    }

    /// Runs the detach handshake of sections 2.6.4 and 2.6.6.
    ///
    /// The caller owns every slice of `options.error_condition`, and the slices
    /// must live until this call returns.
    pub fn detach(self: *Receiver, options: link_mod.DetachOptions) DetachError!void {
        return self.link.detach(options, self.detach_timeout);
    }

    // ---------------------------------------------------------------------
    // The receive path
    // ---------------------------------------------------------------------

    /// Takes one delivery, and waits up to `timeout` for one to arrive.
    ///
    /// The call returns null when no delivery arrives within the timeout. Give
    /// `.none` to wait until a delivery arrives or the link ends. It reports a
    /// link that ended under the wait with the reason of the link.
    ///
    /// The result owns its memory. Free it with `Delivery.deinit`, after the
    /// disposition call that reports its outcome.
    pub fn receive(self: *Receiver, timeout: Io.Timeout) ReceiveError!?Delivery {
        const deadline: Deadline = .start(self.io, timeout);

        // The timer runs until this call returns. The cancel must follow the
        // unlock, because the timer task takes the same lock, so this defer
        // comes first and therefore runs last.
        var timer: ?Io.Future(void) = null;
        defer if (timer) |*future| future.cancel(self.io);

        try self.receive_mutex.lock(self.io);
        defer self.receive_mutex.unlock(self.io);

        while (true) {
            if (self.take()) |delivery| return delivery;
            if (self.out_of_memory.swap(false, .acq_rel)) return error.OutOfMemory;
            if (self.link.failure()) |f| return f.err;
            if (deadline.passed(self.io)) return null;
            // `std.Io.Condition` has no wait with a timeout, so one task wakes
            // every waiter when the deadline of this call passes. Each waiter
            // then reads its own deadline.
            if (timer == null and deadline.at != null) {
                timer = try self.io.concurrent(wakeAtDeadline, .{ self, deadline });
            }
            try self.arrived.wait(self.io, &self.receive_mutex);
        }
    }

    /// Takes one delivery from the queue without waiting for one.
    ///
    /// The caller holds `receive_mutex`, so the router task cannot signal
    /// `arrived` between this look and the wait that follows it.
    fn take(self: *Receiver) ?Delivery {
        var buf: [1]Delivery = undefined;
        // A minimum of zero never blocks, so the call reaches no cancellation
        // point and it returns `error.Closed` only after `deinit`.
        const count = self.deliveries.get(self.io, &buf, 0) catch return null;
        if (count == 0) return null;
        return buf[0];
    }

    /// Wakes every task that waits for a delivery, after the deadline passes.
    fn wakeAtDeadline(self: *Receiver, deadline: Deadline) void {
        deadline.timeout().sleep(self.io) catch return;
        self.receive_mutex.lock(self.io) catch return;
        defer self.receive_mutex.unlock(self.io);
        self.arrived.broadcast(self.io);
    }

    /// Wakes every task that waits for a delivery.
    fn wake(self: *Receiver) void {
        self.receive_mutex.lockUncancelable(self.io);
        defer self.receive_mutex.unlock(self.io);
        self.arrived.broadcast(self.io);
    }

    // ---------------------------------------------------------------------
    // The dispositions
    // ---------------------------------------------------------------------

    /// Reports the accepted outcome of section 3.4.2 for one delivery.
    ///
    /// The call sends nothing for a delivery that arrived settled, because
    /// section 2.6.12 leaves such a delivery in no unsettled map. It sends
    /// nothing either when `Options.auto_accept` already accepted the delivery.
    ///
    /// The call does not free `delivery`. Free it with `Delivery.deinit`.
    pub fn accept(self: *Receiver, delivery: Delivery) DispositionError!void {
        return self.dispose(delivery, .{ .accepted = .{} });
    }

    /// Reports the released outcome of section 3.4.4 for one delivery. The
    /// receiver made no judgement about the message.
    ///
    /// The call does not free `delivery`. Free it with `Delivery.deinit`.
    pub fn release(self: *Receiver, delivery: Delivery) DispositionError!void {
        return self.dispose(delivery, .{ .released = .{} });
    }

    /// Reports the rejected outcome of section 3.4.3 for one delivery.
    ///
    /// The caller owns every slice of `error_condition`, and the slices must
    /// live until this call returns.
    ///
    /// The call does not free `delivery`. Free it with `Delivery.deinit`.
    pub fn reject(
        self: *Receiver,
        delivery: Delivery,
        error_condition: ?performatives.ErrorCondition,
    ) DispositionError!void {
        return self.dispose(delivery, .{
            .rejected = .{ .error_condition = error_condition },
        });
    }

    /// Reports the modified outcome of section 3.4.5 for one delivery. The
    /// receiver asks the sender to change the message before another attempt.
    ///
    /// The caller owns every slice of `options`, and the slices must live until
    /// this call returns.
    ///
    /// The call does not free `delivery`. Free it with `Delivery.deinit`.
    pub fn modify(
        self: *Receiver,
        delivery: Delivery,
        options: ModifyOptions,
    ) DispositionError!void {
        return self.dispose(delivery, .{ .modified = .{
            .delivery_failed = options.delivery_failed,
            .undeliverable_here = options.undeliverable_here,
            .message_annotations = options.message_annotations,
        } });
    }

    /// Sends one disposition frame for one delivery.
    fn dispose(
        self: *Receiver,
        delivery: Delivery,
        state: performatives.DeliveryState,
    ) DispositionError!void {
        if (delivery.settled) return;
        return self.sendDisposition(delivery.delivery_id, state);
    }

    /// Writes the disposition frame of section 2.7.6.
    ///
    /// The frame carries the receiver role, because section 2.7.6 makes the
    /// role name the endpoints that the frame reports on. It carries no `last`,
    /// which the same section reads as the value of `first`, so the frame
    /// covers this one delivery. It settles the delivery, which section 2.7.5
    /// makes the duty of a receiver with rcv-settle-mode first.
    fn sendDisposition(
        self: *Receiver,
        delivery_id: u32,
        state: performatives.DeliveryState,
    ) DispositionError!void {
        return self.link.sendFrame(.{ .disposition = .{
            .role = .receiver,
            .first = delivery_id,
            .settled = true,
            .state = state,
        } });
    }

    // ---------------------------------------------------------------------
    // The router task
    // ---------------------------------------------------------------------

    /// Reads one frame that the shared link plumbing does not read.
    fn onFrame(context: *anyopaque, frame: Frame) void {
        const self: *Receiver = @ptrCast(@alignCast(context));
        defer frame.deinit();

        const body = frame.body orelse return;
        switch (body) {
            .transfer => |performative| self.receiveTransfer(performative, frame.payload),
            // A flow answer is best effort. The session reports its own failure
            // to every caller, so a lost answer here needs no second report.
            //
            // A cancel is the one error that this call must not drop. The
            // signal fires one time, so an `error.Canceled` that stops here
            // leaves the router with no pending signal, and the router then
            // parks in `getOne` forever. `Link.detach` cancels the group and
            // waits for the task, so it would never return. `recancel` arms the
            // request again, and the next wait then ends the router.
            .flow => |performative| if (self.receiveFlow(performative)) |answer| {
                self.link.sendFlow(answer) catch |err| switch (err) {
                    error.Canceled => self.io.recancel(),
                    else => {},
                };
            },
            // Section 2.7.6 gives a disposition no handle, so the session hands
            // one copy to every link. A disposition of the sender reports the
            // state of the sender for a delivery of this link. This endpoint
            // settles on receipt under rcv-settle-mode first, so it holds no
            // unsettled delivery that such a frame could change.
            else => {},
        }
    }

    /// Wakes every task that waits inside the receiver, because the link ended.
    fn onEnd(context: *anyopaque) void {
        const self: *Receiver = @ptrCast(@alignCast(context));
        self.wake();
    }

    /// Joins one transfer frame into the delivery that it belongs to.
    ///
    /// Section 2.6.14: "messages transferred along a single link MUST NOT be
    /// interleaved", so the frames of one delivery arrive in one run and the
    /// receiver needs one buffer.
    fn receiveTransfer(
        self: *Receiver,
        performative: performatives.Transfer,
        payload: []const u8,
    ) void {
        if (self.link.failure() != null) return;

        // Section 2.7.5: "if both the more and aborted fields are set to true,
        // the aborted flag takes precedence", and an aborted message "SHOULD be
        // discarded by the recipient (any payload within the frame carrying the
        // performative MUST be ignored). An aborted message is implicitly
        // settled", so it needs no disposition.
        if (performative.aborted orelse false) {
            self.assembly.active = false;
            self.assembly.bytes.clearRetainingCapacity();
            self.consume();
            return;
        }

        if (!self.assembly.active) {
            // Section 2.7.5: "The delivery-id MUST be supplied on the first
            // transfer of a multi-transfer delivery."
            const id = performative.delivery_id orelse {
                self.link.fail(
                    error.LinkRemoteError,
                    session_mod.condition.not_allowed,
                    "a transfer frame started a delivery without a delivery id",
                );
                return;
            };
            self.assembly = .{
                .active = true,
                .delivery_id = id,
                // Section 2.7.5: "If not set on the first (or only) transfer
                // for a (multi-transfer) delivery, then the settled flag MUST
                // be interpreted as being false."
                .settled = performative.settled orelse false,
                // Section 2.7.5: "The receiver MUST ignore resumed deliveries
                // that are not in its local unsettled map." This module sends
                // no unsettled map, so its map is empty and every resumed
                // delivery goes no further.
                .dropped = performative.resumed orelse false,
                .bytes = self.assembly.bytes,
            };
            self.assembly.bytes.clearRetainingCapacity();
        } else {
            // Section 2.7.5: "It is an error if the delivery-id on a
            // continuation transfer differs from the delivery-id on the first
            // transfer of a delivery."
            if (performative.delivery_id) |id| {
                if (id != self.assembly.delivery_id) {
                    self.link.fail(
                        error.LinkRemoteError,
                        session_mod.condition.not_allowed,
                        "a continuation transfer frame named another delivery id",
                    );
                    return;
                }
            }
            // Section 2.7.5: for a later transfer an unset flag "MUST be
            // interpreted as true if and only if the value of the settled flag
            // on any of the preceding transfers was true", so the flag only
            // ever moves to true inside one delivery.
            if (performative.settled orelse false) self.assembly.settled = true;
        }

        if (!self.assembly.dropped) self.append(payload);

        // Section 2.7.5 gives `more` the default false, so a frame that leaves
        // it out ends the delivery.
        if (performative.more orelse false) return;

        self.assembly.active = false;
        const dropped = self.assembly.dropped;
        const id = self.assembly.delivery_id;
        const settled = self.assembly.settled;
        if (!dropped) self.finish(id, settled);
        self.assembly.bytes.clearRetainingCapacity();
        self.consume();
    }

    /// Adds the payload of one transfer frame to the delivery.
    fn append(self: *Receiver, payload: []const u8) void {
        if (self.max_message_size) |limit| {
            if (self.assembly.bytes.items.len + payload.len > limit) {
                // Section 2.7.3: a delivery larger than the max-message-size of
                // the endpoint "results in a message-size-exceeded link-error".
                self.assembly.dropped = true;
                self.link.fail(
                    error.LinkRemoteError,
                    condition.message_size_exceeded,
                    "a delivery passed the maximum message size of this endpoint",
                );
                return;
            }
        }
        self.assembly.bytes.appendSlice(self.gpa, payload) catch {
            self.assembly.dropped = true;
            self.out_of_memory.store(true, .release);
            self.wake();
        };
    }

    /// Builds one delivery from the joined octets and hands it to a caller.
    fn finish(self: *Receiver, delivery_id: u32, settled: bool) void {
        const bytes = self.assembly.bytes.items;

        const decoded = message_mod.Message.decode(self.gpa, bytes) catch |err| {
            switch (err) {
                error.OutOfMemory => {
                    self.out_of_memory.store(true, .release);
                    self.wake();
                },
                // The octets are not a message of section 3.2. The delivery
                // reaches no caller, and the remote peer reads why.
                else => if (!settled) self.sendDisposition(delivery_id, .{
                    .rejected = .{ .error_condition = .{
                        .condition = .of(condition.decode_error),
                        .description = "the message sections of the delivery are malformed",
                    } },
                }) catch |send_err| switch (send_err) {
                    error.Canceled => self.io.recancel(),
                    else => {},
                },
            }
            return;
        };

        const owned = self.gpa.dupe(u8, bytes) catch {
            decoded.deinit();
            self.out_of_memory.store(true, .release);
            self.wake();
            return;
        };

        const delivery: Delivery = .{
            .delivery_id = delivery_id,
            .settled = settled,
            .bytes = owned,
            .message = decoded.value,
            .arena_state = decoded.arena_state,
            .gpa = self.gpa,
        };

        // Section 2.7.5: a receiver with rcv-settle-mode first settles the
        // delivery as soon as it has arrived. The accept therefore goes out
        // before the queue, which a caller can hold.
        if (!settled and self.auto_accept) {
            self.sendDisposition(delivery_id, .{ .accepted = .{} }) catch |err| switch (err) {
                error.Canceled => self.io.recancel(),
                else => {},
            };
        }

        // The queue holds one window of messages, so this wait means that no
        // caller took a delivery for a whole window. The credit top-up below it
        // therefore stops until a caller does.
        self.deliveries.putOne(self.io, delivery) catch |err| {
            delivery.deinit();
            if (err == error.Canceled) self.io.recancel();
            return;
        };
        self.wake();
    }

    /// Accounts for one delivery that arrived, and tops the credit up.
    fn consume(self: *Receiver) void {
        const answer = self.consumeLocked() orelse return;
        if (self.link.failure() != null) return;
        self.link.sendFlow(answer) catch |err| switch (err) {
            error.Canceled => self.io.recancel(),
            else => {},
        };
    }

    /// Moves the flow control state for one delivery that arrived, and returns
    /// the flow state to send when the credit needs a top-up.
    ///
    /// Section 2.6.9: "As transfers arrive on the link, the sender's
    /// link-credit decreases as the delivery-count increases. When the sender's
    /// link-credit falls below a threshold, the flow state MAY be sent to
    /// increase the sender's link-credit back to the desired target amount."
    /// The threshold of this module is half of the prefetch: the top-up goes
    /// out when the deliveries that arrived take half of the window or more.
    fn consumeLocked(self: *Receiver) ?session_mod.LinkFlow {
        self.credit_mutex.lockUncancelable(self.io);
        defer self.credit_mutex.unlock(self.io);

        self.delivery_count +%= 1;
        if (self.link_credit > 0) self.link_credit -= 1;

        if (self.prefetch == 0) return null;
        // A sender that reports a delivery count behind the one of this
        // endpoint raises the credit above the window, so the subtraction
        // saturates and the top-up waits for the count to catch up.
        const used = self.prefetch -| self.link_credit;
        // The multiplication runs in 64 bits, so a prefetch of one tops up
        // after one delivery and not after none.
        if (@as(u64, used) * 2 < self.prefetch) return null;

        self.link_credit = self.prefetch;
        return self.linkFlowLocked();
    }

    /// Takes the flow control state of section 2.6.7 from a flow frame of the
    /// remote sender.
    ///
    /// Only the receiver chooses the credit, so a flow of the sender moves the
    /// delivery count alone. The call holds the delivery limit of section 2.6.7
    /// still, which means that the credit falls by the number of deliveries
    /// that the sender counted and this endpoint has not seen.
    ///
    /// The call returns the flow state to send when the sender asks for it, and
    /// null in every other case.
    fn receiveFlow(self: *Receiver, performative: performatives.Flow) ?session_mod.LinkFlow {
        self.credit_mutex.lockUncancelable(self.io);
        defer self.credit_mutex.unlock(self.io);

        if (performative.delivery_count) |count| {
            const limit = self.delivery_count +% self.link_credit;
            const room = limit -% count;
            // The delivery count uses RFC 1982 serial arithmetic, so a count
            // past the limit gives no credit and not a very large one.
            self.link_credit = if (room >= serial_negative) 0 else room;
            self.delivery_count = count;
        }
        if (performative.available) |value| self.available = value;

        // Section 2.7.4: "If set to true then the receiver SHOULD send its
        // state at the earliest convenient opportunity." The field has the
        // default false, so a flow that leaves it out asks for nothing.
        if (performative.echo orelse false) return self.linkFlowLocked();
        return null;
    }

    /// Returns the flow state of this link. The caller holds `credit_mutex`.
    fn linkFlowLocked(self: *Receiver) session_mod.LinkFlow {
        return .{
            .handle = self.link.handle,
            .delivery_count = self.delivery_count,
            .link_credit = self.link_credit,
            .available = self.available,
            // Section 2.6.7 initializes the drain flag to false, and this
            // module never sets it. Section 2.7.4 gives the field the same
            // default, so the value states the state of the link.
            .drain = false,
        };
    }
};

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
const Writer = std.Io.Writer;
const appendFrame = test_peer.appendFrame;
const remoteBegin = test_peer.remoteBegin;
const scriptOpen = test_peer.scriptOpen;
const startWatchdog = test_peer.startWatchdog;
const stopWatchdog = test_peer.stopWatchdog;

/// The arguments of the attach frame that the remote sender answers with.
const RemoteAttach = struct {
    name: []const u8 = "link-one",
    handle: u32 = 7,
    snd_settle_mode: ?performatives.SenderSettleMode = .unsettled,
    initial_delivery_count: ?u32 = 0,
    /// True when the peer refuses the link. Section 2.6.3.
    no_source: bool = false,
};

/// The attach frame that the remote sender answers with.
fn remoteAttach(args: RemoteAttach) framing.Body {
    return .{ .attach = .{
        .name = args.name,
        .handle = args.handle,
        .role = .sender,
        .snd_settle_mode = args.snd_settle_mode,
        .rcv_settle_mode = .first,
        .source = if (args.no_source) null else .{ .address = .{ .string = "the-node" } },
        .target = .{ .address = .{ .string = args.name } },
        .initial_delivery_count = args.initial_delivery_count,
    } };
}

/// The arguments of one transfer frame that the remote sender sends.
const RemoteTransfer = struct {
    handle: u32 = 7,
    delivery_id: ?u32 = null,
    delivery_tag: ?[]const u8 = null,
    settled: ?bool = null,
    more: ?bool = null,
    aborted: ?bool = null,
    resumed: ?bool = null,
};

/// One transfer frame that the remote sender sends.
fn remoteTransfer(args: RemoteTransfer) framing.Body {
    return .{ .transfer = .{
        .handle = args.handle,
        .delivery_id = args.delivery_id,
        .delivery_tag = if (args.delivery_tag) |tag| .of(tag) else null,
        .message_format = if (args.delivery_id == null) null else 0,
        .settled = args.settled,
        .more = args.more,
        .resumed = args.resumed,
        .aborted = args.aborted,
    } };
}

/// Encodes one message of section 3.2 with `body` in one data section.
///
/// The caller frees the result.
fn encodeMessage(gpa: Allocator, body: []const u8) ![]u8 {
    const items = [_][]const u8{body};
    const message: message_mod.Message = .{ .body = .{ .data = &items } };
    const bytes = try gpa.alloc(u8, try message.encodedSize());
    errdefer gpa.free(bytes);
    var writer: Writer = .fixed(bytes);
    try message.encode(&writer);
    return bytes;
}

/// Waits until the code under test wrote `count` frames.
///
/// The call reports `error.Timeout` rather than blocking, so a test that waits
/// for a frame that never comes fails with a name and not with a watchdog kill.
fn waitForFrames(io: Io, peer: *Peer, count: u32) !void {
    var waited: usize = 0;
    while (peer.frames() < count) : (waited += 1) {
        if (waited == 2000) return error.Timeout;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
}

/// The parts of the test double stack that every test builds.
const Fixture = struct {
    gpa: Allocator,
    io: Io,
    storage: session_mod.Storage,
    peer: Peer,
    connection: *Connection,
    session: *Session,

    /// Builds the stack around `script`. The value must not move after this
    /// call, so a test holds it in a `var` and takes its address at once.
    fn start(self: *Fixture, gpa: Allocator, io: Io, script: []const u8, gates: []Gate) !void {
        self.gpa = gpa;
        self.io = io;
        self.storage = try .init(gpa, 8);
        errdefer self.storage.deinit(gpa);

        self.peer = .init(gpa, io, script, gates);
        errdefer self.peer.deinit();
        const stream = self.peer.ready();

        self.connection = try Connection.open(gpa, io, stream, .{});
        errdefer self.connection.deinit();

        self.session = try Session.begin(self.connection, &self.storage, .{ .incoming_window = 8 });
    }

    fn deinit(self: *Fixture) void {
        self.session.deinit();
        self.connection.deinit();
        self.peer.deinit();
        self.storage.deinit(self.gpa);
    }
};

// -------------------------------------------------------------------------
// The tests of the attach handshake
// -------------------------------------------------------------------------

test "attach writes the golden bytes of the attach frame with a filter" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    var filter: StringFilter = .init("a.org:a-filter:string", "a-value");
    const filters = [_]MapEntry{filter.entry()};

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .filter = &filters,
        .prefetch = 4,
    });
    defer rcv.deinit();

    // The attach frame is the third frame that this endpoint wrote: the open
    // frame, the begin frame, and then the attach frame.
    const bytes = test_peer.frameBytes(fixture.peer.sent(), 2);

    // The octets, field by field of section 2.7.3:
    //
    //   00 00 00 7f         the frame size, 127 octets
    //   02 00 00 00         doff 2, an AMQP frame, channel 0
    //   00 53 12            the descriptor of `attach`
    //   c0 72 07            a list of 7 fields in 114 octets
    //   a1 08 "link-one"    name
    //   43                  handle 0
    //   41                  role receiver
    //   50 00               snd-settle-mode unsettled
    //   50 00               rcv-settle-mode first
    //   00 53 28 c0 4c 08   the source, a list of 8 fields
    //     a1 08 "the-node"  source.address
    //     40 40 40 40 40 40 durable, expiry-policy, timeout, dynamic,
    //                       dynamic-node-properties, distribution-mode
    //     c1 39 02          the filter map, one entry
    //       a3 15 "a.org:a-filter:string"    the filter name
    //       00 a3 15 "a.org:a-filter:string" the descriptor of the value
    //       a1 07 "a-value"                  the value
    //   00 53 29 c0 0b 01   the target, a list of 1 field
    //     a1 08 "link-one"  target.address, which is the link name
    const golden =
        "\x00\x00\x00\x7f\x02\x00\x00\x00" ++
        "\x00\x53\x12" ++
        "\xc0\x72\x07" ++
        "\xa1\x08link-one" ++
        "\x43" ++
        "\x41" ++
        "\x50\x00" ++
        "\x50\x00" ++
        "\x00\x53\x28\xc0\x4c\x08" ++
        "\xa1\x08the-node" ++
        "\x40\x40\x40\x40\x40\x40" ++
        "\xc1\x39\x02" ++
        "\xa3\x15a.org:a-filter:string" ++
        "\x00\xa3\x15a.org:a-filter:string" ++
        "\xa1\x07a-value" ++
        "\x00\x53\x29\xc0\x0b\x01" ++
        "\xa1\x08link-one";
    try testing.expectEqualSlices(u8, golden, bytes);
}

test "attach reports a link that the remote peer refused with no source" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    // Section 2.6.3: a peer that will not create a terminus answers with a null
    // local terminus and then detaches.
    try appendFrame(&sink, 0, remoteAttach(.{ .no_source = true }), "");
    try appendFrame(&sink, 0, .{ .detach = .{ .handle = 7, .closed = true } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    try testing.expectError(error.LinkRefused, Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
    }));

    // The refused link must draw no credit, because a flow frame for a link
    // that the peer refused reaches a link endpoint that is going away.
    var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer frames.deinit();
    try testing.expectEqual(@as(usize, 0), frames.count(.flow));
    try testing.expectEqual(@as(usize, 1), frames.count(.detach));
}

// -------------------------------------------------------------------------
// The tests of the receive path
// -------------------------------------------------------------------------

test "receive returns null after the timeout" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
    });
    defer rcv.deinit();

    const started: Io.Clock.Timestamp = .now(io, .awake);
    const delivery = try rcv.receive(.{
        .duration = .{ .raw = .fromMilliseconds(40), .clock = .awake },
    });
    try testing.expect(delivery == null);

    // The call must wait for the deadline, and not return null at once.
    const elapsed = started.untilNow(io).raw.nanoseconds;
    try testing.expect(elapsed >= std.time.ns_per_ms * 30);
}

test "a remote detach wakes a receive that waits for a delivery" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");
    const detach_at = sink.written().len;
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
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
    });
    defer rcv.deinit();

    // The timeout is far longer than the detach takes, so a call that waited
    // for the clock instead of the link would fail this test.
    const started: Io.Clock.Timestamp = .now(io, .awake);
    try testing.expectError(error.LinkRemoteError, rcv.receive(.{
        .duration = .{ .raw = .fromSeconds(5), .clock = .awake },
    }));
    const elapsed = started.untilNow(io).raw.nanoseconds;
    try testing.expect(elapsed < std.time.ns_per_s * 2);
    try testing.expectEqualStrings("amqp:link:detach-forced", rcv.failure().?.condition.?);
}

test "a delivery that takes three transfer frames arrives as one message" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const encoded = try encodeMessage(gpa, "the body of the message");
    defer gpa.free(encoded);
    const cut = encoded.len / 3;

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");
    const transfer_at = sink.written().len;
    // Section 2.6.14 sets `more` on every frame of a delivery but the last one.
    try appendFrame(&sink, 0, remoteTransfer(.{
        .delivery_id = 9,
        .delivery_tag = "a-tag",
        .settled = true,
        .more = true,
    }), encoded[0..cut]);
    try appendFrame(&sink, 0, remoteTransfer(.{ .more = true }), encoded[cut .. cut * 2]);
    try appendFrame(&sink, 0, remoteTransfer(.{}), encoded[cut * 2 ..]);

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = transfer_at, .after_frames = 4 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
    });
    defer rcv.deinit();

    const delivery = (try rcv.receive(.{
        .duration = .{ .raw = .fromSeconds(5), .clock = .awake },
    })).?;
    defer delivery.deinit();

    // The three payloads join in order, and the join is the whole message.
    try testing.expectEqualSlices(u8, encoded, delivery.bytes);
    try testing.expectEqual(@as(u32, 9), delivery.delivery_id);
    try testing.expect(delivery.settled);
    try testing.expectEqual(@as(usize, 1), delivery.message.body.data.len);
    try testing.expectEqualStrings("the body of the message", delivery.message.body.data[0]);

    // Section 2.6.7 counts messages and not frames, so three frames of one
    // delivery take one unit of credit.
    try testing.expectEqual(@as(u32, 3), rcv.credit());
    try testing.expectEqual(@as(u32, 1), rcv.deliveryCount());
}

test "an aborted delivery reaches no caller and still takes its credit" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const encoded = try encodeMessage(gpa, "the first half");
    defer gpa.free(encoded);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");
    const transfer_at = sink.written().len;
    try appendFrame(&sink, 0, remoteTransfer(.{
        .delivery_id = 3,
        .delivery_tag = "a-tag",
        .more = true,
    }), encoded);
    // Section 2.7.5: "Aborted messages SHOULD be discarded by the recipient",
    // and "an aborted message is implicitly settled", so it draws no
    // disposition.
    try appendFrame(&sink, 0, remoteTransfer(.{ .aborted = true }), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = transfer_at, .after_frames = 4 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
    });
    defer rcv.deinit();

    const delivery = try rcv.receive(.{
        .duration = .{ .raw = .fromMilliseconds(60), .clock = .awake },
    });
    try testing.expect(delivery == null);

    // The sender counted the delivery, so this endpoint counts it too. A
    // receiver that skipped it would hold a credit view above the one of the
    // sender for the rest of the link.
    try testing.expectEqual(@as(u32, 1), rcv.deliveryCount());
    try testing.expectEqual(@as(u32, 3), rcv.credit());

    var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer frames.deinit();
    try testing.expectEqual(@as(usize, 0), frames.count(.disposition));
}

// -------------------------------------------------------------------------
// The tests of the credit
// -------------------------------------------------------------------------

test "the credit tops up when the deliveries take half of the window" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const encoded = try encodeMessage(gpa, "a-body");
    defer gpa.free(encoded);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");
    const first_at = sink.written().len;
    try appendFrame(&sink, 0, remoteTransfer(.{
        .delivery_id = 0,
        .delivery_tag = "one",
        .settled = true,
    }), encoded);
    const second_at = sink.written().len;
    try appendFrame(&sink, 0, remoteTransfer(.{
        .delivery_id = 1,
        .delivery_tag = "two",
        .settled = true,
    }), encoded);

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        // The initial flow frame is the fourth frame of this endpoint.
        .{ .at = first_at, .after_frames = 4 },
        .{ .at = second_at, .after_frames = Gate.manual },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
    });
    defer rcv.deinit();

    // The attach frame grants the whole window in one flow frame.
    try waitForFrames(io, &fixture.peer, 4);
    try testing.expectEqual(@as(u32, 4), rcv.credit());

    const first = (try rcv.receive(.{
        .duration = .{ .raw = .fromSeconds(5), .clock = .awake },
    })).?;
    defer first.deinit();

    // One delivery of a window of four is below the half, so no flow frame
    // follows it. The wait is long enough that a top-up which fired here would
    // reach the wire.
    try io.sleep(.fromMilliseconds(50), .awake);
    try testing.expectEqual(@as(u32, 4), fixture.peer.frames());
    try testing.expectEqual(@as(u32, 3), rcv.credit());
    try testing.expectEqual(@as(u32, 1), rcv.deliveryCount());

    gates[3].event.set(io);
    const second = (try rcv.receive(.{
        .duration = .{ .raw = .fromSeconds(5), .clock = .awake },
    })).?;
    defer second.deinit();

    // The second delivery reaches the half of the window, so the credit goes
    // back to four in one flow frame.
    try waitForFrames(io, &fixture.peer, 5);
    try testing.expectEqual(@as(u32, 4), rcv.credit());
    try testing.expectEqual(@as(u32, 2), rcv.deliveryCount());

    var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer frames.deinit();

    var flows: std.ArrayListUnmanaged(SentFrame) = .empty;
    defer flows.deinit(gpa);
    try frames.all(.flow, &flows);
    try testing.expectEqual(@as(usize, 2), flows.items.len);

    // Section 2.7.4 puts the link state of section 2.6.7 on the flow frame.
    const initial = flows.items[0].frame.body.?.flow;
    try testing.expectEqual(@as(?u32, 0), initial.handle);
    try testing.expectEqual(@as(?u32, 0), initial.delivery_count);
    try testing.expectEqual(@as(?u32, 4), initial.link_credit);

    const top_up = flows.items[1].frame.body.?.flow;
    try testing.expectEqual(@as(?u32, 0), top_up.handle);
    try testing.expectEqual(@as(?u32, 2), top_up.delivery_count);
    try testing.expectEqual(@as(?u32, 4), top_up.link_credit);
    try testing.expectEqual(@as(?bool, false), top_up.drain);
}

test "the first flow frame counts from the attach frame of the sender" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    // Section 2.6.7: "The sender MAY choose an arbitrary point to initialize
    // the delivery-count. This value is communicated in the initial attach
    // frame. The receiver initializes its delivery-count upon receiving the
    // sender's attach."
    try appendFrame(&sink, 0, remoteAttach(.{ .initial_delivery_count = 40 }), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 6,
    });
    defer rcv.deinit();

    try testing.expectEqual(@as(u32, 40), rcv.deliveryCount());

    var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer frames.deinit();
    const flow = frames.find(.flow).?.body.?.flow;
    // A flow that counted from zero would grant a delivery limit of 6, which
    // sits far behind the count of the sender, and the link would then carry
    // no message at all.
    try testing.expectEqual(@as(?u32, 40), flow.delivery_count);
    try testing.expectEqual(@as(?u32, 6), flow.link_credit);
}

test "a prefetch of zero grants no credit and writes no flow frame" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 0,
    });
    defer rcv.deinit();

    try testing.expectEqual(@as(u32, 0), rcv.credit());
    try testing.expectEqual(@as(u32, 3), fixture.peer.frames());
}

test "the credit top-up fires at the half of every window size" {
    const gpa = testing.allocator;
    const io = testing.io;

    // The unit under test is the rule of section 2.6.9 alone, so this test
    // builds the flow control state and no link.
    var rcv: Receiver = undefined;
    rcv.io = io;
    rcv.gpa = gpa;
    rcv.link = .empty;
    rcv.credit_mutex = .init;

    // A window of one tops up after one delivery. A rule that read the half as
    // zero would send a flow frame after every delivery of every window, and a
    // rule that waited for a whole window would starve a window of one.
    rcv.prefetch = 1;
    rcv.delivery_count = 0;
    rcv.link_credit = 1;
    rcv.available = 0;
    try testing.expect(rcv.consumeLocked() != null);
    try testing.expectEqual(@as(u32, 1), rcv.link_credit);
    try testing.expectEqual(@as(u32, 1), rcv.delivery_count);

    // A window of four tops up on the second delivery and not on the first.
    rcv.prefetch = 4;
    rcv.delivery_count = 0;
    rcv.link_credit = 4;
    try testing.expect(rcv.consumeLocked() == null);
    try testing.expectEqual(@as(u32, 3), rcv.link_credit);
    const answer = rcv.consumeLocked();
    try testing.expect(answer != null);
    try testing.expectEqual(@as(?u32, 4), answer.?.link_credit);
    try testing.expectEqual(@as(?u32, 2), answer.?.delivery_count);
    try testing.expectEqual(@as(u32, 4), rcv.link_credit);

    // A window of five tops up on the third delivery, because two of five is
    // below the half.
    rcv.prefetch = 5;
    rcv.delivery_count = 0;
    rcv.link_credit = 5;
    try testing.expect(rcv.consumeLocked() == null);
    try testing.expect(rcv.consumeLocked() == null);
    try testing.expect(rcv.consumeLocked() != null);
    try testing.expectEqual(@as(u32, 5), rcv.link_credit);
    try testing.expectEqual(@as(u32, 3), rcv.delivery_count);
}

test "a flow frame of the sender moves the delivery count and holds the limit" {
    const gpa = testing.allocator;
    const io = testing.io;

    var rcv: Receiver = undefined;
    rcv.io = io;
    rcv.gpa = gpa;
    rcv.link = .empty;
    rcv.credit_mutex = .init;
    rcv.prefetch = 10;
    rcv.delivery_count = 4;
    rcv.link_credit = 10;
    rcv.available = 0;

    // The sender counted two deliveries that this endpoint has not seen, so the
    // credit falls by two and the delivery limit stays at 14.
    try testing.expect(rcv.receiveFlow(.{ .delivery_count = 6 }) == null);
    try testing.expectEqual(@as(u32, 6), rcv.delivery_count);
    try testing.expectEqual(@as(u32, 8), rcv.link_credit);

    // A count past the limit gives no credit, because section 2.6.7 uses RFC
    // 1982 serial arithmetic and the difference is negative.
    try testing.expect(rcv.receiveFlow(.{ .delivery_count = 20 }) == null);
    try testing.expectEqual(@as(u32, 0), rcv.link_credit);

    // Section 2.7.4 gives `echo` the default false, so only a frame that sets
    // it draws an answer.
    rcv.delivery_count = 6;
    rcv.link_credit = 8;
    const answer = rcv.receiveFlow(.{ .echo = true, .available = 3 });
    try testing.expect(answer != null);
    try testing.expectEqual(@as(?u32, 6), answer.?.delivery_count);
    try testing.expectEqual(@as(?u32, 8), answer.?.link_credit);
    try testing.expectEqual(@as(?u32, 3), answer.?.available);
}

// -------------------------------------------------------------------------
// The tests of the dispositions
// -------------------------------------------------------------------------

/// Scripts two deliveries, the first unsettled and the second settled.
fn scriptTwoDeliveries(
    sink: *Writer.Allocating,
    encoded: []const u8,
) !struct { begin_at: usize, attach_at: usize, transfer_at: usize } {
    try scriptOpen(sink, null);
    const begin_at = sink.written().len;
    try appendFrame(sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(sink, 0, remoteAttach(.{}), "");
    const transfer_at = sink.written().len;
    try appendFrame(sink, 0, remoteTransfer(.{
        .delivery_id = 0,
        .delivery_tag = "one",
        .settled = false,
    }), encoded);
    try appendFrame(sink, 0, remoteTransfer(.{
        .delivery_id = 1,
        .delivery_tag = "two",
        .settled = true,
    }), encoded);
    return .{ .begin_at = begin_at, .attach_at = attach_at, .transfer_at = transfer_at };
}

test "auto-accept settles an unsettled delivery and steps over a settled one" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const encoded = try encodeMessage(gpa, "a-body");
    defer gpa.free(encoded);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    const marks = try scriptTwoDeliveries(&sink, encoded);

    var gates = [_]Gate{
        .{ .at = marks.begin_at, .after_frames = 2 },
        .{ .at = marks.attach_at, .after_frames = 3 },
        .{ .at = marks.transfer_at, .after_frames = 4 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
    });
    defer rcv.deinit();

    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    const first = (try rcv.receive(timeout)).?;
    defer first.deinit();
    try testing.expect(!first.settled);

    const second = (try rcv.receive(timeout)).?;
    defer second.deinit();
    try testing.expect(second.settled);

    // The router task writes the disposition before it queues the delivery, so
    // both answers are on the wire by now.
    var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer frames.deinit();

    // Section 2.6.12 leaves a settled delivery in no unsettled map, so it needs
    // no answer. One disposition covers the unsettled delivery alone.
    try testing.expectEqual(@as(usize, 1), frames.count(.disposition));
    const answer = frames.find(.disposition).?.body.?.disposition;
    try testing.expectEqual(performatives.Role.receiver, answer.role.?);
    try testing.expectEqual(@as(?u32, 0), answer.first);
    // Section 2.7.6 reads an absent `last` as the value of `first`.
    try testing.expectEqual(@as(?u32, null), answer.last);
    try testing.expectEqual(@as(?bool, true), answer.settled);
    try testing.expect(answer.state.? == .accepted);

    // An accept of a delivery that arrived settled writes no second frame.
    try rcv.accept(second);
    var again: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer again.deinit();
    try testing.expectEqual(@as(usize, 1), again.count(.disposition));
}

test "an explicit reject carries the condition and the description" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const encoded = try encodeMessage(gpa, "a-body");
    defer gpa.free(encoded);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    const marks = try scriptTwoDeliveries(&sink, encoded);

    var gates = [_]Gate{
        .{ .at = marks.begin_at, .after_frames = 2 },
        .{ .at = marks.attach_at, .after_frames = 3 },
        .{ .at = marks.transfer_at, .after_frames = 4 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
        .auto_accept = false,
    });
    defer rcv.deinit();

    const first = (try rcv.receive(.{
        .duration = .{ .raw = .fromSeconds(5), .clock = .awake },
    })).?;
    defer first.deinit();

    // The caller owns the outcome when auto-accept is off, so no frame went out
    // before this point.
    {
        var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
        defer frames.deinit();
        try testing.expectEqual(@as(usize, 0), frames.count(.disposition));
    }

    try rcv.reject(first, .{
        .condition = .of("amqp:not-allowed"),
        .description = "this receiver refuses the message",
    });

    var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer frames.deinit();
    try testing.expectEqual(@as(usize, 1), frames.count(.disposition));

    const answer = frames.find(.disposition).?.body.?.disposition;
    try testing.expectEqual(performatives.Role.receiver, answer.role.?);
    try testing.expectEqual(@as(?u32, 0), answer.first);
    try testing.expectEqual(@as(?bool, true), answer.settled);
    try testing.expect(answer.state.? == .rejected);

    const condition_error = answer.state.?.rejected.error_condition.?;
    try testing.expectEqualStrings("amqp:not-allowed", condition_error.condition.?.text);
    try testing.expectEqualStrings(
        "this receiver refuses the message",
        condition_error.description.?,
    );
}

test "release and modify report the outcomes of sections 3.4.4 and 3.4.5" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const encoded = try encodeMessage(gpa, "a-body");
    defer gpa.free(encoded);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");
    const transfer_at = sink.written().len;
    try appendFrame(&sink, 0, remoteTransfer(.{
        .delivery_id = 0,
        .delivery_tag = "one",
    }), encoded);
    try appendFrame(&sink, 0, remoteTransfer(.{
        .delivery_id = 1,
        .delivery_tag = "two",
    }), encoded);

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = transfer_at, .after_frames = 4 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
        .auto_accept = false,
    });
    defer rcv.deinit();

    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    const first = (try rcv.receive(timeout)).?;
    defer first.deinit();
    const second = (try rcv.receive(timeout)).?;
    defer second.deinit();

    try rcv.release(first);
    const annotations = [_]MapEntry{.{
        .key = .{ .symbol = "an-annotation" },
        .value = .{ .string = "a-value" },
    }};
    try rcv.modify(second, .{
        .delivery_failed = true,
        .undeliverable_here = false,
        .message_annotations = &annotations,
    });

    var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer frames.deinit();

    var answers: std.ArrayListUnmanaged(SentFrame) = .empty;
    defer answers.deinit(gpa);
    try frames.all(.disposition, &answers);
    try testing.expectEqual(@as(usize, 2), answers.items.len);

    const released = answers.items[0].frame.body.?.disposition;
    try testing.expectEqual(@as(?u32, 0), released.first);
    try testing.expect(released.state.? == .released);

    const modified = answers.items[1].frame.body.?.disposition;
    try testing.expectEqual(@as(?u32, 1), modified.first);
    try testing.expect(modified.state.? == .modified);
    try testing.expectEqual(@as(?bool, true), modified.state.?.modified.delivery_failed);
    try testing.expectEqual(@as(?bool, false), modified.state.?.modified.undeliverable_here);
    try testing.expectEqualStrings(
        "an-annotation",
        modified.state.?.modified.message_annotations.?[0].key.symbol,
    );
}

test "a later transfer frame that carries settled settles the whole delivery" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const encoded = try encodeMessage(gpa, "a-body");
    defer gpa.free(encoded);
    const cut = encoded.len / 2;

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");
    const transfer_at = sink.written().len;
    // Section 2.7.5: for a later transfer of a delivery an unset settled flag
    // "MUST be interpreted as true if and only if the value of the settled flag
    // on any of the preceding transfers was true", so the flag only ever moves
    // to true inside one delivery.
    try appendFrame(&sink, 0, remoteTransfer(.{
        .delivery_id = 2,
        .delivery_tag = "a-tag",
        .more = true,
    }), encoded[0..cut]);
    try appendFrame(&sink, 0, remoteTransfer(.{ .settled = true }), encoded[cut..]);

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = transfer_at, .after_frames = 4 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
    });
    defer rcv.deinit();

    const delivery = (try rcv.receive(.{
        .duration = .{ .raw = .fromSeconds(5), .clock = .awake },
    })).?;
    defer delivery.deinit();

    try testing.expect(delivery.settled);
    try testing.expectEqualSlices(u8, encoded, delivery.bytes);

    // A settled delivery needs no disposition, so auto-accept must write none.
    var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer frames.deinit();
    try testing.expectEqual(@as(usize, 0), frames.count(.disposition));
}

test "a delivery that holds no message draws a rejected disposition" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");
    const transfer_at = sink.written().len;
    // The payload is not a sequence of message sections of section 3.2.
    try appendFrame(&sink, 0, remoteTransfer(.{
        .delivery_id = 5,
        .delivery_tag = "a-tag",
    }), "not a message");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = transfer_at, .after_frames = 4 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
    });
    defer rcv.deinit();

    const delivery = try rcv.receive(.{
        .duration = .{ .raw = .fromMilliseconds(60), .clock = .awake },
    });
    try testing.expect(delivery == null);

    var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer frames.deinit();
    try testing.expectEqual(@as(usize, 1), frames.count(.disposition));

    const answer = frames.find(.disposition).?.body.?.disposition;
    try testing.expect(answer.state.? == .rejected);
    try testing.expectEqualStrings(
        condition.decode_error,
        answer.state.?.rejected.error_condition.?.condition.?.text,
    );
}

test "a delivery past the maximum message size ends the link" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    const encoded = try encodeMessage(gpa, "a body that is longer than the limit below");
    defer gpa.free(encoded);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink, null);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, remoteAttach(.{}), "");
    const transfer_at = sink.written().len;
    try appendFrame(&sink, 0, remoteTransfer(.{
        .delivery_id = 0,
        .delivery_tag = "a-tag",
    }), encoded);

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = 3 },
        .{ .at = transfer_at, .after_frames = 4 },
    };
    var fixture: Fixture = undefined;
    try fixture.start(gpa, io, sink.written(), &gates);
    defer fixture.deinit();

    const rcv = try Receiver.attach(fixture.session, .{
        .link = .{ .name = "link-one", .address = "the-node" },
        .prefetch = 4,
        .max_message_size = 8,
    });
    defer rcv.deinit();

    // Section 2.7.3 makes a delivery past the limit a `message-size-exceeded`
    // link error, so the wait ends with the reason of the link and not with a
    // message.
    try testing.expectError(error.LinkRemoteError, rcv.receive(.{
        .duration = .{ .raw = .fromSeconds(5), .clock = .awake },
    }));
    try testing.expectEqualStrings(
        condition.message_size_exceeded,
        rcv.failure().?.condition.?,
    );

    var frames: SentFrames = try .parse(gpa, fixture.peer.sent());
    defer frames.deinit();
    const attach = frames.find(.attach).?.body.?.attach;
    try testing.expectEqual(@as(?u64, 8), attach.max_message_size);
}
