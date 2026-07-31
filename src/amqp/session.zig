//! The AMQP 1.0 session.
//!
//! A session is one conversation on one channel of a connection. It carries the
//! begin handshake, the window arithmetic of section 2.5.6, the routing of the
//! link frames by handle, and the end handshake.
//!
//! Specification:
//! OASIS AMQP Version 1.0 Part 2: Transport, sections 2.5.1, 2.5.2, 2.5.4,
//! 2.5.5, 2.5.6, 2.6.2, 2.7.2, 2.7.4, 2.7.7, 2.7.8, and 2.8.17.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html
//!
//! # The tasks
//!
//! `begin` reads the answer of the remote peer on the task of the caller, and
//! then it starts one router task in an `std.Io.Group`. The router task is the
//! only code that takes a frame from the channel queue after `begin` returns.
//! It updates the window, it answers a `flow`, and it pushes each link frame to
//! the queue of the link.
//!
//! # The order of the calls
//!
//! **Call `Session.deinit` before `Connection.deinit`.** The connection frees
//! itself in `deinit`, and it holds no lock against a task that a closed queue
//! just woke. The router task reads `Connection.failure` when its queue wakes
//! it, so the connection must still be alive at that moment. `Session.deinit`
//! ends the router task and waits for it, which gives that guarantee.
//!
//! The memory of the channel queue must stay valid until `Connection.deinit`
//! returns, because the demultiplexer can hold a pointer to it while it pushes
//! a frame. The caller therefore owns that memory: it builds a `Storage`, it
//! gives it to `begin`, and it frees it after `Connection.deinit` returns. A
//! `Storage` that a caller declares before it opens the connection gets that
//! order from the defer statements alone.
//!
//! The same rule holds one level down. A link gives the memory of its queue to
//! `attachLink`, and that memory must stay valid until `Session.deinit`
//! returns.
//!
//! # The channel queue
//!
//! `Storage.init` sizes the queue to the incoming window, because the
//! demultiplexer of the connection blocks on a full queue and that starves
//! every other channel. The window bounds the transfer frames alone: section
//! 2.5.6 puts no bound on a `flow`, a `disposition`, or a `detach`, so a
//! hostile peer can still fill the queue. The connection needs its own bound
//! for that case, and this module cannot give it one.
//!
//! One queue slot holds one `framing.Frame`, which is a little over 512 octets,
//! so the default window of 5000 frames costs about 2.7 megabytes for each
//! session. A caller that wants less memory lowers `Options.incoming_window`,
//! and `Storage.init` then allocates less.
//!
//! # The channel numbers
//!
//! Section 2.5.1 gives a session two channel numbers. `channel` is the outgoing
//! channel of this peer, and `remote_channel` is the outgoing channel of the
//! remote peer. The two are independent, and each peer picks its own.
//!
//! `begin` registers `channel` with the connection and sends its begin frame.
//! The remote peer answers on any free channel of its own, and it names
//! `channel` in the `remote-channel` field of that frame. The demultiplexer of
//! the connection reads that field, binds the channel that the answer arrived
//! on to the queue of this session, and routes every later frame of the session
//! by the binding alone.
//!
//! The answer is therefore the first frame that a session ever receives. A
//! frame that arrives on a channel that no answer bound ends the connection
//! with a protocol error, because the connection cannot tell which session
//! owns it.
//!
//! # The locks
//!
//! `state_mutex` guards the window and the end flag. A holder can write one
//! frame while it holds the lock, because the transfer id of section 2.5.6 is
//! implicit: the peer gives the id of a transfer from the order of the frames
//! on the wire. A task that took an id and then lost the lock before it wrote
//! would let a second task write first, and the two frames would then carry the
//! ids of each other.
//!
//! `links_mutex` guards the two handle tables. Every holder does a short table
//! operation and then unlocks, and no holder writes a frame.

const std = @import("std");

const connection_mod = @import("connection.zig");
const framing = @import("framing.zig");
const performatives = @import("performatives.zig");
const transport = @import("transport.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Connection = connection_mod.Connection;
const Frame = framing.Frame;
const FrameQueue = connection_mod.FrameQueue;
const Io = std.Io;
const MapEntry = types.MapEntry;
const Symbol = performatives.Symbol;

// -------------------------------------------------------------------------
// The constants
// -------------------------------------------------------------------------

/// The distance at which an RFC 1982 serial difference counts as negative.
///
/// Section 2.5.6 applies RFC 1982 serial number arithmetic to the transfer
/// ids and the windows, so a difference of this size or more means that the
/// value is behind, and not far ahead.
const serial_negative: u32 = 1 << 31;

/// The incoming window that `Options` asks for, in transfer frames.
pub const default_incoming_window: u32 = 5000;

/// The outgoing window that `Options` advertises, in transfer frames.
pub const default_outgoing_window: u32 = 5000;

/// The time that `end` waits for the end frame of the remote peer.
pub const default_end_timeout: Io.Timeout =
    .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } };

/// The error condition symbols that this module sends. Sections 2.8.15 and
/// 2.8.17 define them.
pub const condition = struct {
    /// The peer sent more transfer frames than the incoming window allows.
    pub const window_violation = "amqp:session:window-violation";
    /// The peer named a handle that no attached link uses.
    pub const unattached_handle = "amqp:session:unattached-handle";
    /// The peer attached a link with a handle that another link already uses.
    pub const handle_in_use = "amqp:session:handle-in-use";
    /// The peer sent a frame that the current state does not permit.
    pub const illegal_state = "amqp:illegal-state";
    /// The peer used a frame in a way that the specification does not define.
    pub const not_allowed = "amqp:not-allowed";
    /// This endpoint failed, and the failure is not the fault of the peer.
    pub const internal_error = "amqp:internal-error";
};

// -------------------------------------------------------------------------
// The errors
// -------------------------------------------------------------------------

/// The reason that a session ended. The value is sticky: the first reason wins,
/// and every later caller reads the same one.
///
/// Zig holds one namespace for every error name, so these names carry the word
/// `Session`. `error.RemoteError` of the connection would otherwise be the same
/// value as a remote error of the session, and a caller could not tell which
/// layer failed.
pub const Error = error{
    /// The two peers ended the session, and neither one reported an error.
    /// `deinit` also gives this reason.
    SessionEnded,
    /// The remote peer ended the session with an error. Read `failure` for the
    /// condition symbol and the description text.
    SessionRemoteError,
    /// The remote peer broke the protocol, and this endpoint ended the session.
    /// Read `failure` for the condition symbol that this peer sent.
    SessionProtocolError,
    /// This endpoint could not process a frame, and it ended the session. The
    /// fault is local.
    SessionInternalError,
    /// The connection under the session ended. Read `Connection.failure` for
    /// the reason.
    ConnectionFailed,
};

/// The errors of `Session.begin`.
pub const BeginError = Error || connection_mod.RegisterError || connection_mod.SendError ||
    Allocator.Error || Io.ConcurrentError || Io.Cancelable || error{
    /// Every channel up to the limit of the remote peer is in use.
    NoFreeChannel,
    /// The queue of `Storage` holds fewer frames than the incoming window.
    QueueTooSmall,
    /// The begin frame of the remote peer left out a mandatory field.
    MissingField,
};

/// The errors of the send paths of a session.
pub const SendError = Error || connection_mod.SendError;

/// The errors of `Session.sendTransfer`.
pub const TransferError = SendError || error{
    /// The remote peer has no room for another transfer frame. Wait for a
    /// `flow` that raises the window of the remote peer.
    RemoteWindowClosed,
};

/// The errors of `Session.end`.
pub const EndError = SendError || Io.Timeout.Error;

/// The errors of `Session.attachLink`.
pub const AttachError = Error || Allocator.Error || Io.Cancelable || error{
    /// Another attached link of this session already uses the name.
    LinkNameInUse,
};

/// The reason that a session ended, with the text that came with it.
///
/// The two slices live until `Session.deinit` returns.
pub const Failure = struct {
    /// The reason.
    err: Error,
    /// The condition symbol, or null when the reason carries none.
    condition: ?[]const u8,
    /// The description text, or null.
    description: ?[]const u8,
};

// -------------------------------------------------------------------------
// The window
// -------------------------------------------------------------------------

/// The flow control state of one session endpoint. Section 2.5.6.
///
/// Every field is a serial number or a count of **transfer frames**. Section
/// 2.5.6 moves the ids and the windows for a transfer alone, so an `attach`, a
/// `flow`, a `disposition`, or a `detach` costs nothing.
///
/// # The two policy choices
///
/// Section 2.5.6 lets an endpoint decrement its own `incoming-window` when it
/// receives a transfer, and its own `outgoing-window` when it sends one, and it
/// says MAY for both.
///
/// This module decrements the incoming window, because the window then counts
/// the transfer frames that the peer may still send before this peer sends a
/// `flow`. That is what makes the replenishment rule of `needsReplenishment`
/// mean something.
///
/// This module holds the outgoing window at its initial value. Section 2.7.4
/// reads the field as the number of transfers that this peer "could potentially
/// currently send, if it was not constrained by restrictions imposed by its
/// peer's incoming-window", so a constant value states a constant capacity.
/// `remote_incoming_window` is the value that really bounds a send.
pub const Window = struct {
    /// The transfer id that this peer expects on the next incoming transfer.
    next_incoming_id: u32,
    /// The number of transfer frames that this peer can still receive.
    incoming_window: u32,
    /// The value that `replenish` writes back into `incoming_window`.
    initial_incoming_window: u32,
    /// The transfer id that this peer gives to the next outgoing transfer.
    next_outgoing_id: u32,
    /// The first transfer id that this peer ever sends. Section 2.5.6 uses it
    /// in the flow formula when the flow frame carries no next-incoming-id.
    initial_outgoing_id: u32,
    /// The number of transfer frames that this peer could send.
    outgoing_window: u32,
    /// The number of transfer frames that this peer may still send without
    /// passing the incoming window of the remote peer.
    remote_incoming_window: u32,
    /// The number of transfer frames that may still arrive without passing the
    /// outgoing window of the remote peer.
    remote_outgoing_window: u32,

    /// The error of `receiveTransfer`.
    pub const ReceiveError = error{
        /// The peer sent a transfer past the incoming window that this peer
        /// advertised. Section 2.8.17 names the condition
        /// `amqp:session:window-violation`.
        WindowViolation,
        /// The peer omitted a field that the specification makes mandatory.
        /// Section 2.7.4 marks `incoming-window` mandatory on a flow frame.
        MissingField,
    };

    /// Returns the state of a session that has sent no frame yet.
    pub fn init(options: Options) Window {
        return .{
            .next_incoming_id = 0,
            .incoming_window = options.incoming_window,
            .initial_incoming_window = options.incoming_window,
            .next_outgoing_id = options.initial_outgoing_id,
            .initial_outgoing_id = options.initial_outgoing_id,
            .outgoing_window = options.outgoing_window,
            .remote_incoming_window = 0,
            .remote_outgoing_window = 0,
        };
    }

    /// Takes the state of the remote peer from its begin frame.
    ///
    /// Section 2.7.2 makes the three fields mandatory. The begin frame carries
    /// no next-incoming-id, so the remote incoming window follows the second
    /// formula of section 2.5.6: the initial outgoing id of this endpoint, plus
    /// the incoming window of the frame, minus the next outgoing id of this
    /// endpoint.
    pub fn receiveBegin(self: *Window, begin: performatives.Begin) error{MissingField}!void {
        const next_outgoing_id = begin.next_outgoing_id orelse return error.MissingField;
        const incoming_window = begin.incoming_window orelse return error.MissingField;
        const outgoing_window = begin.outgoing_window orelse return error.MissingField;

        self.next_incoming_id = next_outgoing_id;
        self.remote_incoming_window =
            self.initial_outgoing_id +% incoming_window -% self.next_outgoing_id;
        self.remote_outgoing_window = outgoing_window;
    }

    /// Accounts for one incoming transfer frame.
    ///
    /// Section 2.5.6: "Upon receiving a transfer, the receiving endpoint will
    /// increment the next-incoming-id to match the implicit transfer-id of the
    /// incoming transfer plus one, as well as decrementing the
    /// remote-outgoing-window, and MAY (depending on policy) decrement its
    /// incoming-window."
    ///
    /// A window of zero means that the frame passed the maximum incoming
    /// transfer id that section 2.5.6 computes from the last advertisement, so
    /// the caller must end the session with a window violation.
    ///
    /// The remote outgoing window stops at zero. A peer that sends past its own
    /// outgoing window breaks no rule that section 2.8.17 gives a condition
    /// for, so this endpoint does not end the session for it.
    pub fn receiveTransfer(self: *Window) ReceiveError!void {
        if (self.incoming_window == 0) return error.WindowViolation;
        self.next_incoming_id +%= 1;
        self.incoming_window -= 1;
        if (self.remote_outgoing_window > 0) self.remote_outgoing_window -= 1;
    }

    /// Takes the state of the remote peer from a flow frame.
    ///
    /// Section 2.5.6: "When the endpoint receives a flow frame from its peer,
    /// it MUST update the next-incoming-id directly from the next-outgoing-id
    /// of the frame, and it MUST update the remote-outgoing-window directly
    /// from the outgoing-window of the frame."
    ///
    /// The same section computes the remote incoming window from the
    /// next-incoming-id and the incoming-window of the frame, and it falls back
    /// to the initial outgoing id of this endpoint when the frame carries no
    /// next-incoming-id.
    pub fn receiveFlow(self: *Window, flow: performatives.Flow) ReceiveError!void {
        if (flow.next_outgoing_id) |id| self.next_incoming_id = id;
        if (flow.outgoing_window) |window| self.remote_outgoing_window = window;

        const incoming_window = flow.incoming_window orelse return error.MissingField;
        const base = flow.next_incoming_id orelse self.initial_outgoing_id;

        // The formula of section 2.5.6 is the base plus the window of the frame
        // minus the next outgoing id of this endpoint. The two ids are serial
        // numbers, so their difference is the number of transfers in flight,
        // and the window less that number is the room that is left.
        //
        // The subtraction runs on the two ids alone, and not on the sum. A
        // window of 2^31 or more is a legal value that an endpoint uses to mean
        // no limit, and a sum that carried that window would read as a negative
        // distance under RFC 1982 and give a room of zero.
        //
        // A peer may hold fewer transfers than this endpoint already sent. It
        // shrinks its window, or its flow carries a view that is older than the
        // transfers in flight. The saturating subtraction gives zero for that
        // case, and this endpoint then waits for the next flow. A difference at
        // or above 2^31 is negative under RFC 1982, which means that the peer
        // reports an id that this endpoint has not reached, so nothing is in
        // flight.
        const in_flight = self.next_outgoing_id -% base;
        const flight = if (in_flight >= serial_negative) 0 else in_flight;
        self.remote_incoming_window = incoming_window -| flight;
    }

    /// Accounts for one outgoing transfer frame.
    ///
    /// Section 2.5.6: "Upon sending a transfer, the sending endpoint will
    /// increment its next-outgoing-id, decrement its remote-incoming-window,
    /// and MAY (depending on policy) decrement its outgoing-window."
    ///
    /// Section 2.5.6 increments the next outgoing id "according to RFC-1982
    /// serial number arithmetic", so the addition wraps.
    pub fn sendTransfer(self: *Window) void {
        self.next_outgoing_id +%= 1;
        if (self.remote_incoming_window > 0) self.remote_incoming_window -= 1;
    }

    /// Returns true while the remote peer has room for one more transfer frame.
    pub fn canSendTransfer(self: Window) bool {
        return self.remote_incoming_window > 0;
    }

    /// Returns true when the incoming window dropped below half of its initial
    /// value, which is when this endpoint sends a flow to replenish it.
    ///
    /// The multiplication runs in 64 bits, so an initial window of one still
    /// replenishes at zero.
    pub fn needsReplenishment(self: Window) bool {
        return @as(u64, self.incoming_window) * 2 < self.initial_incoming_window;
    }

    /// Puts the incoming window back to its initial value.
    pub fn replenish(self: *Window) void {
        self.incoming_window = self.initial_incoming_window;
    }

    /// Returns the four session fields of a flow frame. Section 2.7.4 makes
    /// three of them mandatory, and it makes the next-incoming-id mandatory for
    /// a peer that has received the begin frame of the remote peer.
    pub fn sessionFlow(self: Window) performatives.Flow {
        return .{
            .next_incoming_id = self.next_incoming_id,
            .incoming_window = self.incoming_window,
            .next_outgoing_id = self.next_outgoing_id,
            .outgoing_window = self.outgoing_window,
        };
    }

    /// Returns the flow frame that replenishes the incoming window. The caller
    /// commits the new window with `replenish` after the frame reaches the
    /// wire.
    pub fn replenishedFlow(self: Window) performatives.Flow {
        var replenished = self;
        replenished.replenish();
        return replenished.sessionFlow();
    }
};

// -------------------------------------------------------------------------
// The options
// -------------------------------------------------------------------------

/// The arguments of `Session.begin`.
pub const Options = struct {
    /// The outgoing channel number. `begin` takes the lowest free channel when
    /// this field is null, which section 2.5.1 recommends.
    channel: ?u16 = null,
    /// The number of transfer frames that this peer accepts before it sends a
    /// flow. `Storage.init` sizes the queue from the same number.
    incoming_window: u32 = default_incoming_window,
    /// The number of transfer frames that this peer advertises as its own
    /// capacity.
    outgoing_window: u32 = default_outgoing_window,
    /// The transfer id of the first transfer that this peer sends. Section
    /// 2.5.6 lets an endpoint pick any value.
    initial_outgoing_id: u32 = 0,
    /// The extension capabilities that this peer supports.
    ///
    /// This module names no capability. The caller supplies every entry, and
    /// the entries live until `begin` returns.
    offered_capabilities: ?[]const Symbol = null,
    /// The extension capabilities that this peer wants.
    desired_capabilities: ?[]const Symbol = null,
    /// The properties of the session. The caller supplies every entry.
    properties: ?[]const MapEntry = null,
    /// The time that `end` waits for the end frame of the remote peer.
    end_timeout: Io.Timeout = default_end_timeout,
};

/// The link fields of a flow frame. Section 2.7.4 forbids every one of them
/// when the frame carries no handle.
pub const LinkFlow = struct {
    /// The output handle of the link that the frame reports.
    handle: u32,
    /// The delivery count of the link endpoint.
    delivery_count: ?u32 = null,
    /// The credit that the receiver grants.
    link_credit: ?u32 = null,
    /// The number of messages that wait at the sender.
    available: ?u32 = null,
    /// True when the receiver drains the credit that it does not use.
    drain: ?bool = null,
    /// True when this peer asks for a flow frame in answer.
    echo: ?bool = null,
    /// The link state properties. The caller supplies every entry.
    properties: ?[]const MapEntry = null,
};

// -------------------------------------------------------------------------
// The queue storage
// -------------------------------------------------------------------------

/// The channel queue of one session, and the memory that holds it.
///
/// The caller owns this value, because its memory must stay valid until
/// `Connection.deinit` returns. Read the order of the calls at the top of this
/// file.
///
/// The value must not move after `Session.begin`, because the connection and
/// the session both hold a pointer to the queue.
pub const Storage = struct {
    /// The slots of the queue.
    frames: []Frame,
    /// The queue that the demultiplexer of the connection pushes to.
    queue: FrameQueue,

    /// Allocates a queue that holds `capacity` frames.
    ///
    /// Give it `Options.incoming_window`, so that the transfer frames of one
    /// window never fill the queue.
    pub fn init(gpa: Allocator, capacity: usize) Allocator.Error!Storage {
        const frames = try gpa.alloc(Frame, capacity);
        return .{ .frames = frames, .queue = .init(frames) };
    }

    /// Frees the slots. Call it after `Connection.deinit` returns.
    pub fn deinit(self: *Storage, gpa: Allocator) void {
        gpa.free(self.frames);
        self.* = undefined;
    }
};

// -------------------------------------------------------------------------
// The link table
// -------------------------------------------------------------------------

/// One attached link of a session.
const LinkEntry = struct {
    /// The name of the link. The session owns the memory. Section 2.6.1 makes
    /// the name the identity of a link, and the two handles are shorthands.
    name: []const u8,
    /// The handle that the remote peer chose, or null before its attach frame
    /// arrives. Section 2.6.2 calls it the input handle.
    input_handle: ?u32,
    /// The queue that receives the frames of the link. The link owns the
    /// memory, and it must stay valid until `Session.deinit` returns.
    queue: *FrameQueue,
};

// -------------------------------------------------------------------------
// The session
// -------------------------------------------------------------------------

/// One AMQP 1.0 session.
///
/// Build it with `begin` and free it with `deinit`. The value must not move,
/// because the router task holds a pointer to it, so `begin` puts it on the
/// heap.
pub const Session = struct {
    gpa: Allocator,
    io: Io,
    connection: *Connection,
    /// The queue of the channel. The caller owns the memory.
    queue: *FrameQueue,

    /// The outgoing channel number of this endpoint.
    channel: u16,
    /// The channel that the remote peer sends on, or null until its begin
    /// arrives. Section 2.5.1 makes it independent of `channel`.
    remote_channel: ?u16,

    /// The lock that guards `window` and `end_sent`.
    state_mutex: Io.Mutex,
    /// The flow control state of section 2.5.6.
    window: Window,
    /// True after this peer sent its end frame.
    end_sent: bool,

    /// The lock that guards the two tables below.
    links_mutex: Io.Mutex,
    /// The links of the session, by output handle.
    links: std.AutoHashMapUnmanaged(u32, LinkEntry),
    /// The output handle of each bound input handle.
    input_handles: std.AutoHashMapUnmanaged(u32, u32),

    /// The step of the terminal state. Read the note on `fail`.
    state: std.atomic.Value(u8),
    failure_err: Error,
    failure_condition: ?[]const u8,
    failure_description: ?[]const u8,
    /// The event that the session sets when it reaches its terminal state.
    ended: Io.Event,
    /// True after the router task read the end frame of the remote peer.
    remote_end_seen: std.atomic.Value(bool),
    /// True after `end` or `deinit` removed the channel from the connection.
    unregistered: bool,

    /// True while the router task discards frames. Section 2.5.4. The router
    /// task is the only reader and the only writer.
    discarding: bool,

    /// The group that holds the router task.
    group: Io.Group,

    /// The time that `end` waits for the end frame of the remote peer.
    end_timeout: Io.Timeout,

    /// The session runs.
    const state_running: u8 = 0;
    /// One task won the race to write the terminal state, and it writes now.
    const state_claimed: u8 = 1;
    /// The terminal state is readable.
    const state_ended: u8 = 2;

    // ---------------------------------------------------------------------
    // The begin handshake
    // ---------------------------------------------------------------------

    /// Begins the session.
    ///
    /// The call takes a channel, registers the queue of `storage` with the
    /// connection, sends the begin frame of section 2.7.2, reads the begin
    /// frame of the remote peer, and starts the router task.
    ///
    /// It registers the queue before it sends, because the answer of the remote
    /// peer can arrive before the send returns, and a frame on a channel that
    /// no session registered ends the connection.
    ///
    /// The call takes the allocator and the `Io` of the connection, so that the
    /// session and the connection always share both.
    ///
    /// The result points to heap memory. Free it with `deinit`, and then free
    /// `storage` after `Connection.deinit` returns.
    pub fn begin(
        connection: *Connection,
        storage: *Storage,
        options: Options,
    ) BeginError!*Session {
        if (storage.queue.capacity() < options.incoming_window) return error.QueueTooSmall;

        const gpa = connection.gpa;
        const io = connection.io;

        const self = try gpa.create(Session);
        errdefer gpa.destroy(self);

        self.* = .{
            .gpa = gpa,
            .io = io,
            .connection = connection,
            .queue = &storage.queue,
            .channel = 0,
            .remote_channel = null,
            .state_mutex = .init,
            .window = .init(options),
            .end_sent = false,
            .links_mutex = .init,
            .links = .empty,
            .input_handles = .empty,
            .state = .init(state_running),
            .failure_err = error.SessionEnded,
            .failure_condition = null,
            .failure_description = null,
            .ended = .unset,
            .remote_end_seen = .init(false),
            .unregistered = false,
            .discarding = false,
            .group = .init,
            .end_timeout = options.end_timeout,
        };

        self.channel = try takeChannel(connection, &storage.queue, options.channel);
        // Every path below this point owns a registered channel. A begin that
        // fails before the answer of the remote peer arrives closes the queue
        // and leaves the channel registered, so the demultiplexer drops the
        // later frames of the channel instead of ending the connection with a
        // protocol error. A begin that fails after the answer arrives gives
        // both channels back, because `readRemoteBegin` sends an end frame on
        // every one of those paths.
        errdefer closeAndDrain(io, &storage.queue);

        try connection.send(self.channel, .{ .begin = .{
            .remote_channel = null,
            .next_outgoing_id = self.window.next_outgoing_id,
            .incoming_window = options.incoming_window,
            .outgoing_window = options.outgoing_window,
            .offered_capabilities = options.offered_capabilities,
            .desired_capabilities = options.desired_capabilities,
            .properties = options.properties,
        } }, "");

        try self.readRemoteBegin();
        try self.group.concurrent(io, route, .{self});
        return self;
    }

    /// Registers the queue on a free channel and returns the number.
    ///
    /// Section 2.5.1: "it is RECOMMENDED that implementations always assign the
    /// lowest available unused channel number."
    fn takeChannel(
        connection: *Connection,
        queue: *FrameQueue,
        wanted: ?u16,
    ) BeginError!u16 {
        if (wanted) |channel| {
            try connection.registerChannel(channel, queue);
            return channel;
        }

        var channel: u16 = 0;
        while (true) {
            connection.registerChannel(channel, queue) catch |err| switch (err) {
                error.ChannelInUse => {
                    if (channel == connection.remote_channel_max) return error.NoFreeChannel;
                    channel += 1;
                    continue;
                },
                else => return err,
            };
            return channel;
        }
    }

    /// Gives back every channel that this session holds.
    ///
    /// Section 2.5.1 gives a session two channel numbers, and the connection
    /// holds a table entry for each one once the begin of the remote peer
    /// arrives. Both must go, or the demultiplexer keeps routing frames to a
    /// queue that this session already closed.
    fn releaseChannels(self: *Session) void {
        self.connection.unregisterChannel(self.channel);
    }

    /// Gives the channels back one time, after the end frame of the remote peer
    /// arrived.
    ///
    /// Section 2.5.2 note (2) disassociates the session from its outgoing
    /// channel at that moment, and section 2.5.1 recommends the lowest free
    /// number, so a later session of this peer can take the number at once.
    ///
    /// The incoming channel is the number that the remote peer sends on, and
    /// the demultiplexer already removed that binding when it routed the end
    /// frame. Note (1) of section 2.5.2 puts the release at that point, and
    /// only the demultiplexer can do it in frame order: this task runs after
    /// the demultiplexer routed the frames that follow the end frame, and the
    /// remote peer reuses its number for the very next session. This call
    /// therefore does nothing on the incoming side after an end frame. A begin
    /// handshake that fails sees no end frame, and `unregisterChannel` then
    /// removes the binding itself.
    ///
    /// `unregistered` is a plain bool, because the router task writes it and
    /// `end` and `deinit` read it only after `stopRouter` joined that task.
    fn releaseChannelsOnce(self: *Session) void {
        if (self.unregistered) return;
        self.unregistered = true;
        self.releaseChannels();
    }

    /// Reads frames until the begin frame of the remote peer arrives.
    ///
    /// Section 2.5.5 puts this endpoint in the BEGIN_SENT state, where it can
    /// send but cannot receive, so any other performative before the begin
    /// breaks the protocol. Section 2.5.3 still lets the remote peer answer
    /// with an end, and this call reports that as a failure.
    ///
    /// Every path that fails here gives the channels back, because the
    /// answering begin frame already bound the channel of the remote peer to
    /// the queue of this session. A session that left the binding behind would
    /// hold both numbers for the life of the connection.
    fn readRemoteBegin(self: *Session) BeginError!void {
        while (true) {
            var frame = self.queue.getOne(self.io) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                // The connection ended under the handshake. Read
                // `Connection.failure` for the reason of that layer.
                error.Closed => return error.ConnectionFailed,
            };
            defer frame.deinit();

            // Section 2.4.5: the demultiplexer answers an empty frame itself,
            // so this loop never sees one. It steps over one anyway.
            const body = frame.body orelse continue;
            switch (body) {
                .begin => |remote| {
                    // Section 2.5.1: the remote channel of the answer names the
                    // channel that this peer sent its begin on. The
                    // demultiplexer routes by that field, so a mismatch here
                    // means that the connection gave this frame to the wrong
                    // session.
                    if (remote.remote_channel) |answered| {
                        if (answered != self.channel) {
                            self.sendEndUncancelable(.{
                                .condition = .of(condition.illegal_state),
                                .description = "the begin frame answered another channel",
                            }) catch {};
                            self.releaseChannelsOnce();
                            return error.SessionProtocolError;
                        }
                    }
                    self.window.receiveBegin(remote) catch {
                        // Section 2.5.4: a session that cannot process input
                        // "MUST indicate this by issuing an END with an
                        // appropriate error". The session is mapped here,
                        // because the answer of the remote peer arrived.
                        self.sendEndUncancelable(.{
                            .condition = .of(condition.not_allowed),
                            .description = "the begin frame left out a mandatory field",
                        }) catch {};
                        self.releaseChannelsOnce();
                        return error.MissingField;
                    };
                    self.remote_channel = frame.channel;
                    return;
                },
                .end => |remote| {
                    // Section 2.5.2: a peer answers an end frame with an end
                    // frame. The end of the remote peer arrived, so the channel
                    // is free again.
                    self.sendEndUncancelable(null) catch {};
                    self.releaseChannelsOnce();
                    self.remote_end_seen.store(true, .release);
                    if (remote.error_condition == null) return error.SessionEnded;
                    return error.SessionRemoteError;
                },
                else => {
                    self.sendEndUncancelable(.{
                        .condition = .of(condition.illegal_state),
                        .description = "a frame arrived before the begin frame",
                    }) catch {};
                    self.releaseChannelsOnce();
                    return error.SessionProtocolError;
                },
            }
        }
    }

    /// Frees the session.
    ///
    /// The call ends the router task, waits for it, drains the channel queue,
    /// and frees every allocation, including `self`. It does not free the
    /// memory of `Storage`, and it does not free the queue of any link.
    ///
    /// Call it before `Connection.deinit`.
    pub fn deinit(self: *Session) void {
        const gpa = self.gpa;

        // Wake every link before the router task stops, so that no reader waits
        // for a frame that no task can send.
        self.fail(error.SessionEnded, null, null);

        // Close the queue before the cancel, so that a router task inside a
        // `getOne` wakes. The demultiplexer then drops the later frames of the
        // channel, and it reports no protocol error for them.
        self.queue.close(self.io);
        self.group.cancel(self.io);

        // Section 2.5.2 frees the channel when the end frame of the remote peer
        // arrives, and not before. A peer keeps frames in flight until then,
        // and the connection ends with a protocol error for a frame on a
        // channel that no session registered. The router task frees the
        // channels at that moment, so this call normally finds them gone.
        if (self.remote_end_seen.load(.acquire)) self.releaseChannelsOnce();

        drainQueue(self.io, self.queue);

        var it = self.links.valueIterator();
        while (it.next()) |entry| gpa.free(entry.name);
        self.links.deinit(gpa);
        self.input_handles.deinit(gpa);

        if (self.failure_condition) |text| gpa.free(text);
        if (self.failure_description) |text| gpa.free(text);
        gpa.destroy(self);
    }

    // ---------------------------------------------------------------------
    // The terminal state
    // ---------------------------------------------------------------------

    /// Returns the reason that the session ended, or null while it runs.
    ///
    /// The reason is sticky. The first task to end the session writes it, and
    /// every later caller reads the same value.
    pub fn failure(self: *Session) ?Failure {
        if (self.state.load(.acquire) != state_ended) return null;
        return .{
            .err = self.failure_err,
            .condition = self.failure_condition,
            .description = self.failure_description,
        };
    }

    /// Ends the session with a reason, and wakes every link.
    ///
    /// The function takes the first reason and drops every later one. It moves
    /// the state in two steps: the winner claims the state, fills the fields,
    /// and only then publishes them, so a reader that sees `state_ended` sees
    /// the fields too.
    fn fail(
        self: *Session,
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
        // caller, and the session is already ending.
        if (failure_condition) |text| self.failure_condition = self.gpa.dupe(u8, text) catch null;
        if (description) |text| self.failure_description = self.gpa.dupe(u8, text) catch null;
        self.state.store(state_ended, .release);

        self.ended.set(self.io);
        self.closeLinks();
    }

    /// Closes the queue of every link, so that a blocked reader wakes.
    fn closeLinks(self: *Session) void {
        // The lock is uncancelable, because a canceled task must still wake the
        // readers of the queues.
        self.links_mutex.lockUncancelable(self.io);
        defer self.links_mutex.unlock(self.io);

        var it = self.links.valueIterator();
        while (it.next()) |entry| entry.queue.close(self.io);
    }

    // ---------------------------------------------------------------------
    // The link table
    // ---------------------------------------------------------------------

    /// Takes an output handle for a link and returns it.
    ///
    /// Section 2.6.2 lets each peer choose its own handle for a link, so this
    /// call gives the link the lowest free handle of this endpoint. The link
    /// puts that handle in the attach frame that it sends.
    ///
    /// The session routes an incoming attach frame by the link name of section
    /// 2.6.1, because the answering attach of the remote peer carries the
    /// handle of the remote peer and nothing that names the handle of this one.
    /// The attach binds the input handle, and every later frame of the link
    /// routes by that handle.
    ///
    /// The memory of `queue` must stay valid until `Session.deinit` returns.
    /// The call does not send a frame.
    pub fn attachLink(
        self: *Session,
        name: []const u8,
        queue: *FrameQueue,
    ) AttachError!u32 {
        if (self.failure()) |f| return f.err;

        try self.links_mutex.lock(self.io);
        defer self.links_mutex.unlock(self.io);

        var it = self.links.valueIterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.name, name)) return error.LinkNameInUse;
        }

        var handle: u32 = 0;
        while (self.links.contains(handle)) handle += 1;

        const owned_name = try self.gpa.dupe(u8, name);
        errdefer self.gpa.free(owned_name);

        try self.links.ensureUnusedCapacity(self.gpa, 1);
        // The router task binds the input handle without an allocator, so the
        // room for that entry comes from here.
        try self.input_handles.ensureUnusedCapacity(self.gpa, 1);
        self.links.putAssumeCapacity(handle, .{
            .name = owned_name,
            .input_handle = null,
            .queue = queue,
        });

        // The session can end while this call waits for the lock. Close the
        // queue of the new link here, or its reader waits for a frame that no
        // task can send.
        if (self.failure()) |f| {
            queue.close(self.io);
            return f.err;
        }
        return handle;
    }

    /// Removes the link of `handle` and frees its output handle.
    ///
    /// Section 2.7.7: a detach "unmaps the handle and makes it available for
    /// use by other links". The call does not send a frame, and it does not
    /// close the queue of the link.
    pub fn detachLink(self: *Session, handle: u32) void {
        self.links_mutex.lockUncancelable(self.io);
        defer self.links_mutex.unlock(self.io);

        const removed = self.links.fetchRemove(handle) orelse return;
        if (removed.value.input_handle) |input| _ = self.input_handles.remove(input);
        self.gpa.free(removed.value.name);
    }

    /// Returns the queue of the link that uses `input_handle`.
    fn linkQueue(self: *Session, input_handle: u32) ?*FrameQueue {
        self.links_mutex.lockUncancelable(self.io);
        defer self.links_mutex.unlock(self.io);

        const output = self.input_handles.get(input_handle) orelse return null;
        const entry = self.links.getPtr(output) orelse return null;
        return entry.queue;
    }

    // ---------------------------------------------------------------------
    // The send paths
    // ---------------------------------------------------------------------

    /// Sends one frame of the session on its channel.
    ///
    /// Use it for an `attach`, a `detach`, or a `disposition`. Section 2.5.6
    /// moves no window for those frames. Use `sendTransfer` for a transfer and
    /// `sendFlow` for a flow.
    pub fn send(self: *Session, body: framing.Body, payload: []const u8) SendError!void {
        if (self.failure()) |f| return f.err;
        return self.connection.send(self.channel, body, payload);
    }

    /// Sends one transfer frame and accounts for it.
    ///
    /// The call holds `state_mutex` across the write, because section 2.5.6
    /// gives a transfer an implicit id that follows the order of the frames on
    /// the wire. It returns `error.RemoteWindowClosed` when the remote incoming
    /// window is zero, because a frame past that window lets the remote peer
    /// end the session with `amqp:session:window-violation`.
    pub fn sendTransfer(
        self: *Session,
        performative: performatives.Transfer,
        payload: []const u8,
    ) TransferError!void {
        if (self.failure()) |f| return f.err;

        try self.state_mutex.lock(self.io);
        defer self.state_mutex.unlock(self.io);

        if (self.failure()) |f| return f.err;
        // Section 2.5.5: after this endpoint sends its end frame the session
        // is in END-SENT, and no further frame of the session may follow it.
        if (self.end_sent) return error.SessionEnded;
        if (!self.window.canSendTransfer()) return error.RemoteWindowClosed;

        try self.connection.send(self.channel, .{ .transfer = performative }, payload);
        self.window.sendTransfer();
    }

    /// Sends one flow frame that carries the session state, and the link state
    /// of `link` when the caller gives one.
    ///
    /// Section 2.7.4 makes the four session fields mandatory, and it forbids
    /// every link field when the frame carries no handle.
    pub fn sendFlow(self: *Session, link: ?LinkFlow) SendError!void {
        if (self.failure()) |f| return f.err;

        try self.state_mutex.lock(self.io);
        defer self.state_mutex.unlock(self.io);

        if (self.failure()) |f| return f.err;
        // Section 2.5.5: after this endpoint sends its end frame the session
        // is in END-SENT, and no further frame of the session may follow it.
        if (self.end_sent) return error.SessionEnded;
        return self.connection.send(self.channel, .{ .flow = self.flowBody(link) }, "");
    }

    /// Returns a flow frame that holds the session state. The caller holds
    /// `state_mutex`.
    fn flowBody(self: *Session, link: ?LinkFlow) performatives.Flow {
        var body = self.window.sessionFlow();
        const fields = link orelse return body;
        body.handle = fields.handle;
        body.delivery_count = fields.delivery_count;
        body.link_credit = fields.link_credit;
        body.available = fields.available;
        body.drain = fields.drain;
        body.echo = fields.echo;
        body.properties = fields.properties;
        return body;
    }

    /// Sends the end frame of section 2.7.8 once, from the task of a caller.
    ///
    /// The lock is uncancelable, because the end handshake of the caller must
    /// still run after a cancel request reached the task.
    ///
    /// The router task must not use this function. Read `sendEndBestEffort` for
    /// the reason.
    fn sendEndUncancelable(
        self: *Session,
        error_condition: ?performatives.ErrorCondition,
    ) SendError!void {
        self.state_mutex.lockUncancelable(self.io);
        defer self.state_mutex.unlock(self.io);
        return self.sendEndLocked(error_condition);
    }

    /// Sends the end frame from the router task, and gives up when the lock
    /// does not come free.
    ///
    /// The router task must never take the lock uncancelably. A caller that
    /// holds the lock inside a blocked write holds it until the connection
    /// fails, and a task inside an uncancelable wait never reaches a
    /// cancellation point, so `deinit` would then hang: `Io.Group.cancel` waits
    /// for its tasks.
    fn sendEndBestEffort(self: *Session, error_condition: ?performatives.ErrorCondition) void {
        self.state_mutex.lock(self.io) catch return;
        defer self.state_mutex.unlock(self.io);
        self.sendEndLocked(error_condition) catch {};
    }

    /// Writes the end frame. The caller holds `state_mutex`.
    fn sendEndLocked(
        self: *Session,
        error_condition: ?performatives.ErrorCondition,
    ) SendError!void {
        if (self.end_sent) return;
        self.end_sent = true;
        return self.connection.send(
            self.channel,
            .{ .end = .{ .error_condition = error_condition } },
            "",
        );
    }

    // ---------------------------------------------------------------------
    // The end handshake
    // ---------------------------------------------------------------------

    /// Runs the end handshake of section 2.5.2.
    ///
    /// The call sends the end frame, and then it waits for the router task to
    /// read the end frame of the remote peer. Give `error_condition` when this
    /// peer ends the session because of an error, and null for a clean end.
    ///
    /// The call returns `error.Timeout` when the remote peer sends no end
    /// within `Options.end_timeout`. It ends the session either way, so every
    /// link wakes.
    ///
    /// One task calls this. Two tasks that call it together race the guard of
    /// `releaseChannelsOnce`, and the loser then calls `unregisterChannel` a
    /// second time, which returns at once on the key that is already gone.
    ///
    /// After the call the session touches the connection no more, except for
    /// the `unregisterChannel` of `deinit`.
    pub fn end(
        self: *Session,
        error_condition: ?performatives.ErrorCondition,
    ) EndError!void {
        self.sendEndUncancelable(error_condition) catch |err| {
            self.fail(error.ConnectionFailed, null, @errorName(err));
            self.stopRouter();
            return err;
        };

        var timed_out = false;
        self.ended.waitTimeout(self.io, self.end_timeout) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Timeout => {
                // The remote peer never answered. End the session anyway, so
                // that no link waits for a frame that cannot arrive.
                self.fail(error.SessionEnded, null, "the remote peer sent no end frame");
                timed_out = true;
            },
        };

        self.stopRouter();

        // Section 2.5.2 frees the channel only when the end frame of the remote
        // peer arrived. The router task normally did that already.
        if (self.remote_end_seen.load(.acquire)) self.releaseChannelsOnce();

        if (timed_out) return error.Timeout;

        const f = self.failure().?;
        if (f.err == error.SessionEnded) return;
        return f.err;
    }

    /// Ends the router task and waits for it.
    fn stopRouter(self: *Session) void {
        self.group.cancel(self.io);
    }

    // ---------------------------------------------------------------------
    // The router task
    // ---------------------------------------------------------------------

    /// What the router task does after one frame.
    const Routing = enum { keep_reading, stop };

    /// Takes frames from the channel queue and routes them until the session
    /// ends.
    fn route(self: *Session) void {
        while (true) {
            const frame = self.queue.getOne(self.io) catch |err| {
                switch (err) {
                    // `deinit` closed the queue, or the connection ended. The
                    // connection is alive here, because the caller frees it
                    // only after `Session.deinit` returns.
                    error.Closed => {
                        if (self.failure() == null) {
                            const reason = self.connection.failure();
                            self.fail(
                                error.ConnectionFailed,
                                if (reason) |f| f.condition else null,
                                if (reason) |f| f.description else null,
                            );
                        }
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
    ///
    /// The function frees the frame, or it gives the frame to the queue of a
    /// link. The caller never touches the frame again.
    fn dispatch(self: *Session, frame: Frame) Routing {
        var owned = frame;

        // Section 2.5.4: an endpoint that ended the session with an error
        // "MUST then proceed to discard all incoming frames from the remote
        // endpoint until receiving the remote endpoint's corresponding end
        // frame".
        if (self.discarding) {
            const is_end = if (owned.body) |body| body == .end else false;
            owned.deinit();
            if (!is_end) return .keep_reading;
            // Section 2.5.4 ends the discarding state here, and section 2.5.2
            // note (2) frees the channel with it. The flag publishes after the
            // release, so a task that waits on the flag finds the number free.
            self.releaseChannelsOnce();
            self.remote_end_seen.store(true, .release);
            return .stop;
        }

        const body = owned.body orelse {
            owned.deinit();
            return .keep_reading;
        };

        switch (body) {
            .end => |performative| {
                const routing = self.handleRemoteEnd(performative);
                owned.deinit();
                return routing;
            },
            .attach => |performative| return self.routeAttach(owned, performative),
            .flow => |performative| return self.routeFlow(owned, performative),
            .transfer => |performative| return self.routeTransfer(owned, performative),
            .disposition => return self.routeDisposition(owned),
            .detach => |performative| return self.routeDetach(owned, performative),
            .begin => {
                owned.deinit();
                return self.endWithError(
                    condition.illegal_state,
                    "a second begin frame arrived on the channel",
                );
            },
            else => {
                owned.deinit();
                return self.endWithError(
                    condition.illegal_state,
                    "a connection frame arrived on a session channel",
                );
            },
        }
    }

    /// Ends the session with an error, and moves to the discarding state of
    /// section 2.5.5.
    fn endWithError(
        self: *Session,
        error_condition: []const u8,
        description: []const u8,
    ) Routing {
        self.sendEndBestEffort(.{
            .condition = .of(error_condition),
            .description = description,
        });
        self.fail(error.SessionProtocolError, error_condition, description);
        self.discarding = true;
        return .keep_reading;
    }

    /// Ends the session because this endpoint failed.
    fn endWithLocalError(self: *Session, description: []const u8) Routing {
        self.sendEndBestEffort(.{
            .condition = .of(condition.internal_error),
            .description = description,
        });
        self.fail(error.SessionInternalError, condition.internal_error, description);
        self.discarding = true;
        return .keep_reading;
    }

    /// Answers the end frame of the remote peer and ends the session.
    fn handleRemoteEnd(self: *Session, performative: performatives.End) Routing {
        // Section 2.5.2: a peer answers an end frame with an end frame.
        self.sendEndBestEffort(null);
        // The handshake is over, so the outgoing channel number goes back now.
        // The caller can hold this session for as long as it wants, and a later
        // session of this peer can take the number at once. The flag publishes
        // after the release, so a task that waits on the flag finds the number
        // free.
        self.releaseChannelsOnce();
        self.remote_end_seen.store(true, .release);

        const condition_error = performative.error_condition orelse {
            self.fail(error.SessionEnded, null, null);
            return .stop;
        };
        const symbol = if (condition_error.condition) |text| text.text else null;
        self.fail(error.SessionRemoteError, symbol, condition_error.description);
        return .stop;
    }

    /// Binds the input handle of a link and gives it the attach frame.
    fn routeAttach(self: *Session, frame: Frame, performative: performatives.Attach) Routing {
        var owned = frame;

        const name = performative.name orelse {
            owned.deinit();
            return self.endWithError(
                condition.not_allowed,
                "an attach frame arrived without a link name",
            );
        };
        const handle = performative.handle orelse {
            owned.deinit();
            return self.endWithError(
                condition.not_allowed,
                "an attach frame arrived without a handle",
            );
        };

        const queue = self.bindInputHandle(name, handle) catch |err| {
            owned.deinit();
            return switch (err) {
                error.HandleInUse => self.endWithError(
                    condition.handle_in_use,
                    "an attach frame arrived with a handle that another link uses",
                ),
                error.UnknownLink => self.endWithError(
                    condition.not_allowed,
                    "an attach frame arrived for a link that this endpoint did not create",
                ),
            };
        };
        return self.deliver(owned, queue);
    }

    /// Records the handle that the remote peer chose for a link, and returns
    /// the queue of that link.
    fn bindInputHandle(
        self: *Session,
        name: []const u8,
        handle: u32,
    ) error{ HandleInUse, UnknownLink }!*FrameQueue {
        self.links_mutex.lockUncancelable(self.io);
        defer self.links_mutex.unlock(self.io);

        if (self.input_handles.contains(handle)) return error.HandleInUse;

        var it = self.links.iterator();
        while (it.next()) |pair| {
            if (!std.mem.eql(u8, pair.value_ptr.name, name)) continue;
            pair.value_ptr.input_handle = handle;
            // `attachLink` reserved the room for this entry, so the put cannot
            // allocate and cannot fail.
            self.input_handles.putAssumeCapacity(handle, pair.key_ptr.*);
            return pair.value_ptr.queue;
        }
        return error.UnknownLink;
    }

    /// Takes the session state of a flow frame, answers an echo request, and
    /// gives a link flow to its link.
    fn routeFlow(self: *Session, frame: Frame, performative: performatives.Flow) Routing {
        var owned = frame;

        const echo = performative.echo orelse false;
        const link_handle = performative.handle;
        // The block below records a bad frame and leaves the lock, because
        // `endWithError` takes the same lock. A second unlock on an unlocked
        // mutex is `unreachable`, so the defer must run one time only.
        var missing_window = false;
        {
            self.state_mutex.lock(self.io) catch {
                owned.deinit();
                return .stop;
            };
            defer self.state_mutex.unlock(self.io);

            // Section 2.7.4 makes `incoming-window` mandatory on a flow, so a
            // frame without it is a protocol error and not a zero window.
            if (self.window.receiveFlow(performative)) |_| {
                // Section 2.7.4: "If set to true then the receiver SHOULD send
                // its state at the earliest convenient opportunity." A flow
                // that names no handle asks for the session state alone, which
                // this endpoint can answer here. A flow that names a handle
                // asks for link state, and the link answers that itself.
                //
                // An endpoint that sent its end frame is in END_SENT of
                // section 2.5.5, where it "MAY receive frames, but cannot
                // send them". The echo of a flow that crossed the end frame
                // on the wire must therefore stay unanswered.
                if (echo and link_handle == null and !self.end_sent) {
                    self.connection.send(
                        self.channel,
                        .{ .flow = self.window.sessionFlow() },
                        "",
                    ) catch {};
                }
            } else |_| {
                missing_window = true;
            }
        }

        if (missing_window) {
            owned.deinit();
            return self.endWithError(
                condition.not_allowed,
                "a flow frame carried no incoming window",
            );
        }

        const handle = link_handle orelse {
            owned.deinit();
            return .keep_reading;
        };
        return self.routeByHandle(owned, handle);
    }

    /// Accounts for one incoming transfer frame and gives it to its link.
    fn routeTransfer(self: *Session, frame: Frame, performative: performatives.Transfer) Routing {
        var owned = frame;

        switch (self.accountTransfer()) {
            .ok => {},
            .violation => {
                owned.deinit();
                return self.endWithError(
                    condition.window_violation,
                    "a transfer frame arrived past the incoming window",
                );
            },
            .canceled => {
                owned.deinit();
                return .stop;
            },
            .send_failed => |err| {
                owned.deinit();
                self.fail(error.ConnectionFailed, null, @errorName(err));
                return .stop;
            },
        }

        const handle = performative.handle orelse {
            owned.deinit();
            return self.endWithError(
                condition.not_allowed,
                "a transfer frame arrived without a handle",
            );
        };
        return self.routeByHandle(owned, handle);
    }

    /// What `accountTransfer` found.
    const Accounting = union(enum) {
        /// The transfer fits the window, and the flow frame reached the wire
        /// when the window needed one.
        ok,
        /// The transfer passed the window that this peer advertised.
        violation,
        /// A cancel request reached the router task.
        canceled,
        /// The flow frame did not reach the wire.
        send_failed: anyerror,
    };

    /// Moves the window for one incoming transfer, and replenishes the incoming
    /// window when it drops below half.
    ///
    /// The call sends the flow frame while it holds `state_mutex`, so that no
    /// transfer of this peer can slip between the read of the next outgoing id
    /// and the write of the frame. It commits the new window only after the
    /// frame reached the wire, because the remote peer computes its own limit
    /// from the frame and not from the state of this peer.
    fn accountTransfer(self: *Session) Accounting {
        self.state_mutex.lock(self.io) catch return .canceled;
        defer self.state_mutex.unlock(self.io);

        self.window.receiveTransfer() catch return .violation;
        if (!self.window.needsReplenishment()) return .ok;

        // Section 2.5.5 puts an endpoint that sent its end frame in END_SENT,
        // where it "MAY receive frames, but cannot send them". The accounting
        // above stays, because the window must stay correct for the frames
        // that still arrive. Only the advertisement stops, and the remote peer
        // needs none of it after the end frame.
        if (self.end_sent) return .ok;

        const body = self.window.replenishedFlow();
        self.connection.send(self.channel, .{ .flow = body }, "") catch |err| {
            return .{ .send_failed = err };
        };
        self.window.replenish();
        return .ok;
    }

    /// Gives a disposition frame to every attached link.
    ///
    /// Section 2.7.6 gives the disposition no handle, because the delivery ids
    /// of section 2.6.12 run over the whole session. Each link picks the
    /// delivery ids that it owns out of the range.
    ///
    /// One frame owns one arena, so a copy for each link costs one encode and
    /// one decode. The last link takes the frame that arrived, so a session
    /// with one link copies nothing.
    fn routeDisposition(self: *Session, frame: Frame) Routing {
        var owned = frame;

        var queues: [max_broadcast_links]*FrameQueue = undefined;
        const count = self.collectLinkQueues(&queues);
        if (count == 0) {
            owned.deinit();
            return .keep_reading;
        }

        const performative = owned.body.?.disposition;
        for (queues[0 .. count - 1]) |queue| {
            const copy = cloneDisposition(self.gpa, owned.channel, performative) catch {
                owned.deinit();
                return self.endWithLocalError("this endpoint ran out of memory for a disposition");
            };
            switch (self.deliver(copy, queue)) {
                .keep_reading => {},
                .stop => {
                    owned.deinit();
                    return .stop;
                },
            }
        }
        return self.deliver(owned, queues[count - 1]);
    }

    /// The number of links that one disposition frame reaches. A session with
    /// more links drops the frame for the links past the limit, which no test
    /// and no caller of this library reaches today.
    const max_broadcast_links: usize = 64;

    /// Writes the queue of each attached link into `out` and returns the count.
    fn collectLinkQueues(self: *Session, out: *[max_broadcast_links]*FrameQueue) usize {
        self.links_mutex.lockUncancelable(self.io);
        defer self.links_mutex.unlock(self.io);

        var count: usize = 0;
        var it = self.links.valueIterator();
        while (it.next()) |entry| {
            if (entry.input_handle == null) continue;
            if (count == out.len) break;
            out[count] = entry.queue;
            count += 1;
        }
        return count;
    }

    /// Gives a detach frame to its link and frees the input handle.
    ///
    /// Section 2.7.7: a detach "unmaps the handle and makes it available for use
    /// by other links". The output handle stays until the link calls
    /// `detachLink`.
    fn routeDetach(self: *Session, frame: Frame, performative: performatives.Detach) Routing {
        var owned = frame;

        const handle = performative.handle orelse {
            owned.deinit();
            return self.endWithError(
                condition.not_allowed,
                "a detach frame arrived without a handle",
            );
        };

        const queue = self.unbindInputHandle(handle) orelse {
            owned.deinit();
            return self.endWithError(
                condition.unattached_handle,
                "a frame arrived for a handle that no attached link uses",
            );
        };
        return self.deliver(owned, queue);
    }

    /// Removes the binding of `handle` and returns the queue of its link.
    fn unbindInputHandle(self: *Session, handle: u32) ?*FrameQueue {
        self.links_mutex.lockUncancelable(self.io);
        defer self.links_mutex.unlock(self.io);

        const removed = self.input_handles.fetchRemove(handle) orelse return null;
        const entry = self.links.getPtr(removed.value) orelse return null;
        entry.input_handle = null;
        return entry.queue;
    }

    /// Gives a frame to the link that uses `handle`.
    fn routeByHandle(self: *Session, frame: Frame, handle: u32) Routing {
        var owned = frame;
        const queue = self.linkQueue(handle) orelse {
            owned.deinit();
            // Section 2.8.17: `unattached-handle` reports "a frame (other than
            // attach) was received referencing a handle which is not currently
            // in use of an attached link". Section 2.7.4 makes the end of the
            // session the answer that a flow with such a handle requires.
            return self.endWithError(
                condition.unattached_handle,
                "a frame arrived for a handle that no attached link uses",
            );
        };
        return self.deliver(owned, queue);
    }

    /// Pushes a frame to the queue of a link, and takes its memory.
    fn deliver(self: *Session, frame: Frame, queue: *FrameQueue) Routing {
        var owned = frame;
        queue.putOne(self.io, owned) catch |err| {
            owned.deinit();
            switch (err) {
                // The link freed its queue, or the session already ended.
                // Neither one is a protocol error.
                error.Closed => return if (self.failure() == null) .keep_reading else .stop,
                error.Canceled => return .stop,
            }
        };
        return .keep_reading;
    }
};

// -------------------------------------------------------------------------
// The helpers
// -------------------------------------------------------------------------

/// Returns a disposition frame that owns its own memory.
///
/// A frame holds its body in an arena, and the slices of the body point into
/// that arena, so a field by field copy would share the memory of the original.
/// The function therefore encodes the performative and reads it back into a new
/// arena.
fn cloneDisposition(
    gpa: Allocator,
    channel: u16,
    performative: performatives.Disposition,
) !Frame {
    const size = try performative.encodedSize();
    const bytes = try gpa.alloc(u8, size);
    defer gpa.free(bytes);

    var writer: std.Io.Writer = .fixed(bytes);
    try performative.encode(&writer);

    const decoded = try performatives.Disposition.decode(gpa, bytes);
    return .{
        .frame_type = .amqp,
        .channel = channel,
        .body = .{ .disposition = decoded.value },
        .payload = "",
        .arena_state = decoded.arena_state,
        .child = decoded.child,
    };
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

/// Closes a queue and frees every frame that it still holds.
fn closeAndDrain(io: Io, queue: *FrameQueue) void {
    queue.close(io);
    drainQueue(io, queue);
}

// -------------------------------------------------------------------------
// The test doubles
// -------------------------------------------------------------------------

const testing = std.testing;
const Reader = std.Io.Reader;
const Writer = std.Io.Writer;

/// The number of seconds that a test may take before the watchdog stops the
/// process. A test that blocks must fail, and it must never hold CI.
const watchdog_seconds: i64 = 20;

/// One hold point in the script of the remote peer.
///
/// The reader gives no octet from the offset `at` until the code under test
/// flushed `after_frames` frames. A test uses the gate to make the answer of
/// the remote peer follow the frame that asks for it.
const Gate = struct {
    /// The offset in the script where the reader stops.
    at: usize,
    /// The number of frames that opens the gate.
    after_frames: u32,
    /// The event that opens the gate.
    event: Io.Event = .unset,

    /// An `after_frames` value that no frame count reaches. A test opens such a
    /// gate itself, at the point where the code under test is ready for the
    /// next frame of the remote peer.
    const manual: u32 = std.math.maxInt(u32);
};

/// A reader that gives scripted octets to the code under test.
///
/// The gates must run in order of `at`. After the last octet the reader either
/// reports the end of the stream or it waits, so that a test can hold the
/// demultiplexer inside a read. A cancel request ends that wait.
const ScriptReader = struct {
    interface: Reader,
    io: Io,
    script: []const u8,
    pos: usize,
    tail: Tail,
    gates: []Gate,
    next_gate: usize,
    /// The event that ends the wait after the last octet.
    release: Io.Event,
    buf: [4096]u8,

    /// What the reader does after the last octet of the script.
    const Tail = enum { end_of_stream, wait };

    const vtable: Reader.VTable = .{ .stream = stream };

    fn init(io: Io, script: []const u8, gates: []Gate, tail: Tail) ScriptReader {
        return .{
            .interface = .{ .vtable = &vtable, .buffer = &.{}, .seek = 0, .end = 0 },
            .io = io,
            .script = script,
            .pos = 0,
            .tail = tail,
            .gates = gates,
            .next_gate = 0,
            .release = .unset,
            .buf = undefined,
        };
    }

    /// Points the reader at its own buffer. Call it after the value reached its
    /// final address.
    fn ready(self: *ScriptReader) *Reader {
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

        if (self.pos == end) switch (self.tail) {
            .end_of_stream => return error.EndOfStream,
            .wait => {
                self.release.wait(self.io) catch return error.ReadFailed;
                return error.EndOfStream;
            },
        };

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
const FrameCounter = struct {
    interface: Writer,
    io: Io,
    inner: *Writer,
    /// The number of frames that this writer saw.
    frames: std.atomic.Value(u32),
    gates: []Gate,

    const vtable: Writer.VTable = .{ .drain = drain, .flush = flush };

    fn init(io: Io, inner: *Writer, gates: []Gate, buf: []u8) FrameCounter {
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
const Peer = struct {
    io: Io,
    reader: ScriptReader,
    mock: transport.MockTransport,
    counter: FrameCounter,
    gates: []Gate,
    counter_buf: [4096]u8,

    fn init(
        gpa: Allocator,
        io: Io,
        script: []const u8,
        gates: []Gate,
        tail: ScriptReader.Tail,
    ) Peer {
        return .{
            .io = io,
            .reader = .init(io, script, gates, tail),
            .mock = .init(gpa, ""),
            .counter = undefined,
            .gates = gates,
            .counter_buf = undefined,
        };
    }

    /// Finishes the setup and returns the stream for `Connection.open`.
    fn ready(self: *Peer) connection_mod.Stream {
        self.counter = .init(self.io, self.mock.writer(), self.gates, &self.counter_buf);
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

/// Waits until the router task of a session reads the end frame of the remote
/// peer.
///
/// The router task gives the channels back at that moment, and it writes no
/// frame for a session that already sent its own end, so this flag is the only
/// signal that the moment passed. The watchdog bounds the wait.
fn waitRemoteEnd(io: Io, session: *Session) !void {
    while (!session.remote_end_seen.load(.acquire)) {
        try io.sleep(.fromMilliseconds(1), .awake);
    }
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
    try framing.writeFrame(
        &sink.writer,
        channel,
        body,
        payload,
        connection_mod.default_max_frame_size,
    );
}

/// The frames that the code under test wrote.
///
/// Every frame borrows one shared buffer for its payload, so a later frame
/// overwrites the payload of an earlier one. The tests read the performatives
/// alone.
const SentFrames = struct {
    gpa: Allocator,
    buf: []u8,
    frames: std.ArrayListUnmanaged(Frame),

    fn parse(gpa: Allocator, bytes: []const u8) !SentFrames {
        var self: SentFrames = .{
            .gpa = gpa,
            .buf = try gpa.alloc(u8, connection_mod.default_max_frame_size),
            .frames = .empty,
        };
        errdefer self.deinit();

        var reader: Reader = .fixed(bytes);
        while (true) {
            const frame = framing.readFrame(
                gpa,
                &reader,
                self.buf,
                connection_mod.default_max_frame_size,
            ) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            errdefer frame.deinit();
            try self.frames.append(gpa, frame);
        }
        return self;
    }

    fn deinit(self: *SentFrames) void {
        for (self.frames.items) |frame| frame.deinit();
        self.frames.deinit(self.gpa);
        self.gpa.free(self.buf);
    }

    /// Returns the first frame whose body holds `tag`, or null.
    fn find(self: *SentFrames, comptime tag: std.meta.Tag(framing.Body)) ?Frame {
        for (self.frames.items) |frame| {
            const body = frame.body orelse continue;
            if (body == tag) return frame;
        }
        return null;
    }

    /// Returns the number of frames whose body holds `tag`.
    fn count(self: *SentFrames, comptime tag: std.meta.Tag(framing.Body)) usize {
        var found: usize = 0;
        for (self.frames.items) |frame| {
            const body = frame.body orelse continue;
            if (body == tag) found += 1;
        }
        return found;
    }
};

/// A reader of one link queue. It counts the frames that it took, and it
/// records that the queue woke it.
const LinkReader = struct {
    queue: *FrameQueue,
    io: Io,
    /// The number of frames that the task took before it woke.
    received: u32 = 0,
    /// True after the queue woke the task.
    woke: bool = false,

    fn run(self: *LinkReader) void {
        while (true) {
            var frame = self.queue.getOne(self.io) catch {
                self.woke = true;
                return;
            };
            frame.deinit();
            self.received += 1;
        }
    }
};

/// Runs `Session.begin` in its own task, so that a test can cancel the call or
/// act while it waits for the answer of the remote peer.
const BeginTask = struct {
    connection: *Connection,
    storage: *Storage,
    options: Options = .{},
    /// The session, or null when the call failed.
    session: ?*Session = null,
    /// The error of the call, or null when the call built a session.
    result: ?anyerror = null,

    fn run(self: *BeginTask) void {
        if (Session.begin(self.connection, self.storage, self.options)) |session| {
            self.session = session;
        } else |err| {
            self.result = err;
        }
    }

    /// Frees the session that the call built, if it built one.
    fn deinit(self: *BeginTask) void {
        if (self.session) |session| session.deinit();
    }
};

/// Builds the octets of the open frame of the remote peer.
fn scriptOpen(sink: *Writer.Allocating) !void {
    try appendFrame(sink, 0, .{ .open = .{ .container_id = "the-peer" } }, "");
}

/// The begin frame that the remote peer answers with.
fn remoteBegin(channel: u16) framing.Body {
    return .{ .begin = .{
        .remote_channel = channel,
        .next_outgoing_id = 0,
        .incoming_window = 100,
        .outgoing_window = 200,
    } };
}

// -------------------------------------------------------------------------
// The tests of the window
// -------------------------------------------------------------------------

test "the window takes the state of the remote peer from its begin frame" {
    var window: Window = .init(.{ .incoming_window = 10, .outgoing_window = 20 });
    try window.receiveBegin(.{
        .next_outgoing_id = 7,
        .incoming_window = 30,
        .outgoing_window = 40,
    });

    // Section 2.5.6: the next incoming id is the next outgoing id of the peer.
    try testing.expectEqual(@as(u32, 7), window.next_incoming_id);
    // The second formula of section 2.5.6 with an initial outgoing id of zero.
    try testing.expectEqual(@as(u32, 30), window.remote_incoming_window);
    try testing.expectEqual(@as(u32, 40), window.remote_outgoing_window);
}

test "a begin frame without a mandatory window field is an error" {
    var window: Window = .init(.{});
    try testing.expectError(error.MissingField, window.receiveBegin(.{
        .next_outgoing_id = 0,
        .outgoing_window = 1,
    }));
}

test "receiving a transfer moves the incoming state of section 2.5.6" {
    var window: Window = .init(.{ .incoming_window = 4 });
    try window.receiveBegin(.{
        .next_outgoing_id = 5,
        .incoming_window = 1,
        .outgoing_window = 2,
    });

    try window.receiveTransfer();
    try testing.expectEqual(@as(u32, 6), window.next_incoming_id);
    try testing.expectEqual(@as(u32, 3), window.incoming_window);
    try testing.expectEqual(@as(u32, 1), window.remote_outgoing_window);

    // The remote outgoing window stops at zero, and the session survives it.
    try window.receiveTransfer();
    try window.receiveTransfer();
    try testing.expectEqual(@as(u32, 0), window.remote_outgoing_window);
}

test "a transfer that arrives with no window left is a violation" {
    var window: Window = .init(.{ .incoming_window = 2 });
    try window.receiveTransfer();
    try window.receiveTransfer();
    try testing.expectError(error.WindowViolation, window.receiveTransfer());
}

test "sending a transfer moves the outgoing state of section 2.5.6" {
    var window: Window = .init(.{ .outgoing_window = 9 });
    try window.receiveBegin(.{
        .next_outgoing_id = 0,
        .incoming_window = 2,
        .outgoing_window = 2,
    });

    window.sendTransfer();
    try testing.expectEqual(@as(u32, 1), window.next_outgoing_id);
    try testing.expectEqual(@as(u32, 1), window.remote_incoming_window);
    // The policy of this module holds the outgoing window still.
    try testing.expectEqual(@as(u32, 9), window.outgoing_window);

    window.sendTransfer();
    try testing.expect(!window.canSendTransfer());
}

test "the transfer ids wrap with serial arithmetic" {
    var window: Window = .init(.{ .initial_outgoing_id = std.math.maxInt(u32) });
    try window.receiveBegin(.{
        .next_outgoing_id = std.math.maxInt(u32),
        .incoming_window = 4,
        .outgoing_window = 4,
    });

    window.sendTransfer();
    try testing.expectEqual(@as(u32, 0), window.next_outgoing_id);
    try window.receiveTransfer();
    try testing.expectEqual(@as(u32, 0), window.next_incoming_id);
}

test "a flow that shrinks the window below the frames in flight gives no room" {
    var window: Window = .init(.{
        .incoming_window = 5000,
        .outgoing_window = 5000,
        .initial_outgoing_id = 100,
    });

    // This endpoint sent ten transfers, so its next outgoing id is 110. The
    // peer then shrinks its window and allows ids up to 104 only. Section
    // 2.5.6 applies RFC 1982 arithmetic, so the difference is negative and the
    // window is zero. A plain wrapping subtraction gives 4294967291 instead,
    // and this endpoint would flood the peer.
    for (0..10) |_| window.sendTransfer();
    try testing.expectEqual(@as(u32, 110), window.next_outgoing_id);

    try window.receiveFlow(.{
        .next_incoming_id = 100,
        .incoming_window = 5,
        .next_outgoing_id = 0,
        .outgoing_window = 5000,
    });

    try testing.expectEqual(@as(u32, 0), window.remote_incoming_window);
    try testing.expect(!window.canSendTransfer());
}

test "a flow that advertises the largest window keeps every transfer" {
    var window: Window = .init(.{ .incoming_window = 5000, .outgoing_window = 5000 });

    // A window of 2^31 or more is a legal value, and an endpoint that wants no
    // limit sends one. The sum of the base and such a window reads as a
    // negative distance under RFC 1982, so a formula that clamps the sum gives
    // a room of zero and stalls every send from then on.
    try window.receiveFlow(.{
        .next_incoming_id = 0,
        .incoming_window = serial_negative,
        .next_outgoing_id = 0,
        .outgoing_window = 5000,
    });
    try testing.expectEqual(serial_negative, window.remote_incoming_window);
    try testing.expect(window.canSendTransfer());

    // The transfers in flight still come off the window that the peer gives.
    window.next_outgoing_id = 10;
    try window.receiveFlow(.{
        .next_incoming_id = 0,
        .incoming_window = std.math.maxInt(u32),
        .next_outgoing_id = 0,
        .outgoing_window = 5000,
    });
    try testing.expectEqual(std.math.maxInt(u32) - 10, window.remote_incoming_window);
}

test "a flow without an incoming window is a protocol error" {
    var window: Window = .init(.{
        .incoming_window = 5000,
        .outgoing_window = 5000,
        .initial_outgoing_id = 0,
    });

    // Section 2.7.4 makes the field mandatory, so a frame without it is a
    // protocol error and not a window of zero.
    try testing.expectError(error.MissingField, window.receiveFlow(.{
        .next_incoming_id = 0,
        .next_outgoing_id = 0,
        .outgoing_window = 5000,
    }));
}

test "a flow recomputes the remote incoming window from its own fields" {
    var window: Window = .init(.{});
    window.next_outgoing_id = 10;

    // Section 2.5.6: next-incoming-id(flow) + incoming-window(flow)
    // - next-outgoing-id(endpoint).
    try window.receiveFlow(.{
        .next_incoming_id = 8,
        .incoming_window = 5,
        .next_outgoing_id = 3,
        .outgoing_window = 7,
    });
    try testing.expectEqual(@as(u32, 3), window.next_incoming_id);
    try testing.expectEqual(@as(u32, 7), window.remote_outgoing_window);
    try testing.expectEqual(@as(u32, 3), window.remote_incoming_window);
}

test "a flow without a next incoming id uses the initial outgoing id" {
    var window: Window = .init(.{ .initial_outgoing_id = 100 });
    window.next_outgoing_id = 110;

    // Section 2.5.6: initial-outgoing-id(endpoint) + incoming-window(flow)
    // - next-outgoing-id(endpoint).
    try window.receiveFlow(.{
        .incoming_window = 20,
        .next_outgoing_id = 4,
        .outgoing_window = 4,
    });
    try testing.expectEqual(@as(u32, 10), window.remote_incoming_window);
}

test "the incoming window replenishes below half and not above it" {
    var window: Window = .init(.{ .incoming_window = 4 });
    try testing.expect(!window.needsReplenishment());
    try window.receiveTransfer();
    try testing.expect(!window.needsReplenishment());
    try window.receiveTransfer();
    // Half of four is two, and two is not below two.
    try testing.expect(!window.needsReplenishment());
    try window.receiveTransfer();
    try testing.expect(window.needsReplenishment());

    const body = window.replenishedFlow();
    try testing.expectEqual(@as(?u32, 4), body.incoming_window);
    try testing.expectEqual(@as(?u32, 3), body.next_incoming_id);
    window.replenish();
    try testing.expectEqual(@as(u32, 4), window.incoming_window);
}

test "a window of one replenishes when it reaches zero" {
    var window: Window = .init(.{ .incoming_window = 1 });
    try testing.expect(!window.needsReplenishment());
    try window.receiveTransfer();
    try testing.expect(window.needsReplenishment());
}

test "the session flow carries the four mandatory fields and no link field" {
    var window: Window = .init(.{ .incoming_window = 3, .outgoing_window = 6 });
    window.next_incoming_id = 2;
    window.next_outgoing_id = 5;

    const body = window.sessionFlow();
    try testing.expectEqual(@as(?u32, 2), body.next_incoming_id);
    try testing.expectEqual(@as(?u32, 3), body.incoming_window);
    try testing.expectEqual(@as(?u32, 5), body.next_outgoing_id);
    try testing.expectEqual(@as(?u32, 6), body.outgoing_window);
    try testing.expectEqual(@as(?u32, null), body.handle);
    try testing.expectEqual(@as(?u32, null), body.link_credit);
}

// -------------------------------------------------------------------------
// The tests of the handshakes
// -------------------------------------------------------------------------

test "the peer answers the begin on a channel of its own choosing" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, default_incoming_window);
    defer storage.deinit(gpa);

    // Section 2.5.1: the peer allocates an unused outgoing channel of its own
    // and names this peer's channel in `remote-channel`. The two numbers are
    // independent, so the answer arrives on channel 7 while this peer sent its
    // begin on channel 0. A later frame also arrives on channel 7.
    //
    // The flow asks for an echo, and the router task answers it. The test waits
    // for that answer, because the router task owns the channel queue and a
    // second reader on it would take the frame out of its hands.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_at = sink.written().len;
    try appendFrame(&sink, 7, remoteBegin(0), "");
    try appendFrame(&sink, 7, .{ .flow = .{
        .next_incoming_id = 0,
        .incoming_window = 50,
        .next_outgoing_id = 0,
        .outgoing_window = 60,
        .echo = true,
    } }, "");

    var gates = [_]Gate{
        .{ .at = gate_at, .after_frames = 2 },
        .{ .at = sink.written().len, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{});
    defer session.deinit();

    try testing.expectEqual(@as(u16, 0), session.channel);
    try testing.expectEqual(@as(?u16, 7), session.remote_channel);

    // The answer to the echo is the third frame on the wire, so the router task
    // took the flow that arrived on the channel of the peer.
    try gates[1].event.wait(io);
    try testing.expect(connection.failure() == null);
    try testing.expectEqual(@as(u32, 60), session.window.remote_outgoing_window);
    try testing.expectEqual(@as(u32, 50), session.window.remote_incoming_window);

    // Section 2.5.1: this peer answers on its own outgoing channel, which is
    // the channel that it sent its begin on.
    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();
    const answer = written.find(.flow).?;
    try testing.expectEqual(@as(u16, 0), answer.channel);
    try testing.expectEqual(@as(?u32, default_incoming_window), answer.body.?.flow.incoming_window);
}

test "a flow frame without an incoming window ends the session" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    // Section 2.7.4 makes `incoming-window` mandatory, so this frame is a
    // protocol error. The router task must end the session and must not unlock
    // `state_mutex` twice on the way out.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    try appendFrame(&sink, 0, .{ .flow = .{
        .next_incoming_id = 0,
        .next_outgoing_id = 0,
        .outgoing_window = 5,
    } }, "");

    var gates = [_]Gate{.{ .at = gate_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    try session.ended.wait(io);
    const f = session.failure().?;
    try testing.expectEqual(Error.SessionProtocolError, f.err);
    try testing.expectEqualStrings(condition.not_allowed, f.condition.?);

    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();
    const frame = written.find(.end).?;
    try testing.expectEqualStrings(
        condition.not_allowed,
        frame.body.?.end.error_condition.?.condition.?.text,
    );
}

test "begin sends one begin frame and records the state of the remote peer" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, default_incoming_window);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");

    // The peer answers only after this peer wrote its open frame and its begin
    // frame.
    var gates = [_]Gate{.{ .at = gate_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{});
    defer session.deinit();

    try testing.expectEqual(@as(u16, 0), session.channel);
    try testing.expectEqual(@as(?u16, 0), session.remote_channel);
    try testing.expectEqual(@as(u32, 0), session.window.next_incoming_id);
    try testing.expectEqual(@as(u32, 100), session.window.remote_incoming_window);
    try testing.expectEqual(@as(u32, 200), session.window.remote_outgoing_window);

    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();

    const frame = written.find(.begin).?;
    const body = frame.body.?.begin;
    try testing.expectEqual(@as(u16, 0), frame.channel);
    // Section 2.7.2: a locally initiated session sets no remote channel.
    try testing.expectEqual(@as(?u16, null), body.remote_channel);
    try testing.expectEqual(@as(?u32, 0), body.next_outgoing_id);
    try testing.expectEqual(@as(?u32, default_incoming_window), body.incoming_window);
    try testing.expectEqual(@as(?u32, default_outgoing_window), body.outgoing_window);
}

test "the end handshake sends one end frame and frees the channel" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const end_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .end = .{} }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = end_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    try session.end(null);
    try testing.expectEqual(Error.SessionEnded, session.failure().?.err);

    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();
    try testing.expectEqual(@as(usize, 1), written.count(.end));

    // The channel is free again, which is what the unregister of the end
    // handshake gives.
    var buf: [1]Frame = undefined;
    var other: FrameQueue = .init(&buf);
    try connection.registerChannel(0, &other);
}

test "a peer that refuses the session ends it with an error" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    // Section 2.5.1 maps the channel with the begin frame, so a peer that
    // refuses the session answers the begin and then ends it. An end frame on
    // an unmapped channel reaches no session, and the connection reports a
    // protocol error for it.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    try appendFrame(&sink, 0, .{ .end = .{ .error_condition = .{
        .condition = .of("amqp:not-allowed"),
        .description = "this container serves no session",
    } } }, "");

    var gates = [_]Gate{.{ .at = gate_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    try session.ended.wait(io);
    const f = session.failure().?;
    try testing.expectEqual(Error.SessionRemoteError, f.err);
    try testing.expectEqualStrings("amqp:not-allowed", f.condition.?);

    // Section 2.5.2: this peer answered the end frame with its own end frame.
    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();
    try testing.expectEqual(@as(usize, 1), written.count(.end));
}

test "a cancel while the session waits for the remote begin ends the wait" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, default_incoming_window);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);

    // The peer sends its open frame and nothing else, so the session waits for
    // a begin frame that never arrives. The gate opens after the begin frame of
    // this peer, which tells the test that the wait started.
    var gates = [_]Gate{.{ .at = sink.written().len, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    var beginner: BeginTask = .{ .connection = connection, .storage = &storage };
    var task = io.concurrent(BeginTask.run, .{&beginner}) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return error.SkipZigTest,
    };
    defer beginner.deinit();

    try gates[0].event.wait(io);
    task.cancel(io);
    try testing.expectEqual(@as(?anyerror, error.Canceled), beginner.result);
}

// -------------------------------------------------------------------------
// The tests of the window on the wire
// -------------------------------------------------------------------------

test "the session replenishes the incoming window with a flow below half" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    var link_buf: [8]Frame = undefined;
    var link_queue: FrameQueue = .init(&link_buf);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .begin = .{
        .remote_channel = 0,
        .next_outgoing_id = 0,
        .incoming_window = 100,
        .outgoing_window = 100,
    } }, "");
    const attach_at = sink.written().len;
    // The attach binds the handle of the remote peer to the link.
    try appendFrame(&sink, 0, .{ .attach = .{
        .name = "the-link",
        .handle = 3,
        .role = .sender,
    } }, "");
    // Three transfers take the window of four down to one, which is below half.
    try appendFrame(&sink, 0, .{ .transfer = .{ .handle = 3 } }, "a");
    try appendFrame(&sink, 0, .{ .transfer = .{ .handle = 3 } }, "b");
    try appendFrame(&sink, 0, .{ .transfer = .{ .handle = 3 } }, "c");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = Gate.manual },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    const handle = try session.attachLink("the-link", &link_queue);
    try testing.expectEqual(@as(u32, 0), handle);
    gates[1].event.set(io);

    // The attach and the three transfers all reach the link. The session sends
    // the flow before it gives the third transfer to the link, so the frame is
    // on the wire once the fourth frame arrives here.
    var taken: usize = 0;
    while (taken < 4) : (taken += 1) {
        var frame = try link_queue.getOne(io);
        frame.deinit();
    }

    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();

    const frame = written.find(.flow).?;
    const body = frame.body.?.flow;
    try testing.expectEqual(@as(u16, 0), frame.channel);
    try testing.expectEqual(@as(?u32, 3), body.next_incoming_id);
    try testing.expectEqual(@as(?u32, 4), body.incoming_window);
    try testing.expectEqual(@as(?u32, 0), body.next_outgoing_id);
    try testing.expectEqual(@as(?u32, default_outgoing_window), body.outgoing_window);
    // Section 2.7.4 forbids the link fields when the frame carries no handle.
    try testing.expectEqual(@as(?u32, null), body.handle);
    try testing.expectEqual(@as(?u32, null), body.link_credit);
}

test "the session sends no flow while the incoming window stays above half" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    var link_buf: [8]Frame = undefined;
    var link_queue: FrameQueue = .init(&link_buf);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .attach = .{
        .name = "the-link",
        .handle = 3,
        .role = .sender,
    } }, "");
    // Two transfers leave the window at two, which is half and not below it.
    try appendFrame(&sink, 0, .{ .transfer = .{ .handle = 3 } }, "a");
    try appendFrame(&sink, 0, .{ .transfer = .{ .handle = 3 } }, "b");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = Gate.manual },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    _ = try session.attachLink("the-link", &link_queue);
    gates[1].event.set(io);

    var taken: usize = 0;
    while (taken < 3) : (taken += 1) {
        var frame = try link_queue.getOne(io);
        frame.deinit();
    }
    try testing.expectEqual(@as(u32, 2), session.window.incoming_window);

    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();
    try testing.expectEqual(@as(usize, 0), written.count(.flow));
}

// -------------------------------------------------------------------------
// The tests of the routing
// -------------------------------------------------------------------------

test "the session routes frames to two links by handle" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var buf_one: [4]Frame = undefined;
    var buf_two: [4]Frame = undefined;
    var queue_one: FrameQueue = .init(&buf_one);
    var queue_two: FrameQueue = .init(&buf_two);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .attach = .{ .name = "link-one", .handle = 11 } }, "");
    try appendFrame(&sink, 0, .{ .attach = .{ .name = "link-two", .handle = 22 } }, "");
    try appendFrame(&sink, 0, .{ .transfer = .{ .handle = 11, .delivery_id = 1 } }, "");
    try appendFrame(&sink, 0, .{ .flow = .{
        .next_incoming_id = 0,
        .incoming_window = 5,
        .next_outgoing_id = 1,
        .outgoing_window = 5,
        .handle = 22,
        .link_credit = 7,
    } }, "");
    try appendFrame(&sink, 0, .{ .detach = .{ .handle = 22, .closed = true } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = Gate.manual },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    const handle_one = try session.attachLink("link-one", &queue_one);
    const handle_two = try session.attachLink("link-two", &queue_two);
    try testing.expectEqual(@as(u32, 0), handle_one);
    try testing.expectEqual(@as(u32, 1), handle_two);
    gates[1].event.set(io);

    var first = try queue_one.getOne(io);
    defer first.deinit();
    try testing.expectEqualStrings("link-one", first.body.?.attach.name.?);

    var second = try queue_two.getOne(io);
    defer second.deinit();
    try testing.expectEqualStrings("link-two", second.body.?.attach.name.?);

    var transfer = try queue_one.getOne(io);
    defer transfer.deinit();
    try testing.expectEqual(@as(?u32, 11), transfer.body.?.transfer.handle);

    var flow = try queue_two.getOne(io);
    defer flow.deinit();
    try testing.expectEqual(@as(?u32, 7), flow.body.?.flow.link_credit);
    // Section 2.5.6: a link flow carries the session state too.
    try testing.expectEqual(@as(u32, 1), session.window.next_incoming_id);

    var detach = try queue_two.getOne(io);
    defer detach.deinit();
    try testing.expectEqual(@as(?bool, true), detach.body.?.detach.closed);
}

test "a disposition reaches every attached link" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var buf_one: [4]Frame = undefined;
    var buf_two: [4]Frame = undefined;
    var queue_one: FrameQueue = .init(&buf_one);
    var queue_two: FrameQueue = .init(&buf_two);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const attach_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .attach = .{ .name = "link-one", .handle = 11 } }, "");
    try appendFrame(&sink, 0, .{ .attach = .{ .name = "link-two", .handle = 22 } }, "");
    try appendFrame(&sink, 0, .{ .disposition = .{
        .role = .receiver,
        .first = 4,
        .last = 6,
        .settled = true,
    } }, "");

    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = attach_at, .after_frames = Gate.manual },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    _ = try session.attachLink("link-one", &queue_one);
    _ = try session.attachLink("link-two", &queue_two);
    gates[1].event.set(io);

    var attach_one = try queue_one.getOne(io);
    attach_one.deinit();
    var attach_two = try queue_two.getOne(io);
    attach_two.deinit();

    var first = try queue_one.getOne(io);
    defer first.deinit();
    var second = try queue_two.getOne(io);
    defer second.deinit();

    try testing.expectEqual(@as(?u32, 4), first.body.?.disposition.first);
    try testing.expectEqual(@as(?u32, 6), first.body.?.disposition.last);
    try testing.expectEqual(@as(?u32, 4), second.body.?.disposition.first);
    try testing.expectEqual(@as(?u32, 6), second.body.?.disposition.last);
}

test "a frame for a handle that no link uses ends the session" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    try appendFrame(&sink, 0, .{ .flow = .{
        .next_incoming_id = 0,
        .incoming_window = 5,
        .next_outgoing_id = 0,
        .outgoing_window = 5,
        .handle = 9,
    } }, "");

    var gates = [_]Gate{.{ .at = begin_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    try session.ended.wait(io);
    const f = session.failure().?;
    try testing.expectEqual(Error.SessionProtocolError, f.err);
    try testing.expectEqualStrings(condition.unattached_handle, f.condition.?);

    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();
    const frame = written.find(.end).?;
    try testing.expectEqualStrings(
        condition.unattached_handle,
        frame.body.?.end.error_condition.?.condition.?.text,
    );
}

test "a transfer past the incoming window ends the session with a violation" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 2);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    try appendFrame(&sink, 0, .{ .transfer = .{ .handle = 3 } }, "a");

    var gates = [_]Gate{.{ .at = begin_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    // The session advertises an incoming window of zero, so section 2.5.6
    // allows the peer no transfer frame at all. A window of zero also
    // replenishes nothing, because zero is not below half of zero.
    const session = try Session.begin(connection, &storage, .{ .incoming_window = 0 });
    defer session.deinit();

    try session.ended.wait(io);
    const f = session.failure().?;
    try testing.expectEqual(Error.SessionProtocolError, f.err);
    try testing.expectEqualStrings(condition.window_violation, f.condition.?);

    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();
    const frame = written.find(.end).?;
    try testing.expectEqualStrings(
        condition.window_violation,
        frame.body.?.end.error_condition.?.condition.?.text,
    );
}

test "an end with an error reaches both child links" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 8);
    defer storage.deinit(gpa);

    var buf_one: [4]Frame = undefined;
    var buf_two: [4]Frame = undefined;
    var queue_one: FrameQueue = .init(&buf_one);
    var queue_two: FrameQueue = .init(&buf_two);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    const end_at = sink.written().len;
    try appendFrame(&sink, 0, .{ .end = .{ .error_condition = .{
        .condition = .of("amqp:internal-error"),
        .description = "the broker lost the session",
    } } }, "");

    // The end frame follows the third frame that this peer writes, which is the
    // flow that the test sends below.
    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = end_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 8 });
    defer session.deinit();

    _ = try session.attachLink("link-one", &queue_one);
    _ = try session.attachLink("link-two", &queue_two);

    var reader_one: LinkReader = .{ .queue = &queue_one, .io = io };
    var reader_two: LinkReader = .{ .queue = &queue_two, .io = io };
    var task_one = io.concurrent(LinkReader.run, .{&reader_one}) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return error.SkipZigTest,
    };
    var task_two = io.concurrent(LinkReader.run, .{&reader_two}) catch |err| switch (err) {
        error.ConcurrencyUnavailable => {
            task_one.cancel(io);
            return error.SkipZigTest;
        },
    };

    // The flow opens the gate, and the end frame of the peer follows it.
    try session.sendFlow(null);

    task_one.await(io);
    task_two.await(io);
    try testing.expect(reader_one.woke);
    try testing.expect(reader_two.woke);

    const f = session.failure().?;
    try testing.expectEqual(Error.SessionRemoteError, f.err);
    try testing.expectEqualStrings("amqp:internal-error", f.condition.?);
    try testing.expectEqualStrings("the broker lost the session", f.description.?);
}

test "a second begin frame on the channel is a protocol error" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");
    try appendFrame(&sink, 0, remoteBegin(0), "");

    var gates = [_]Gate{.{ .at = begin_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    try session.ended.wait(io);
    const f = session.failure().?;
    try testing.expectEqual(Error.SessionProtocolError, f.err);
    try testing.expectEqualStrings(condition.illegal_state, f.condition.?);
}

test "the session takes the lowest free channel" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_at = sink.written().len;
    try appendFrame(&sink, 1, remoteBegin(1), "");

    var gates = [_]Gate{.{ .at = gate_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    // Another session already holds channel 0.
    var buf: [1]Frame = undefined;
    var taken: FrameQueue = .init(&buf);
    try connection.registerChannel(0, &taken);

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    try testing.expectEqual(@as(u16, 1), session.channel);
    try testing.expectEqual(@as(u16, 1), session.remote_channel);
}

test "two sessions keep their channel numbers apart" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage_one: Storage = try .init(gpa, 4);
    defer storage_one.deinit(gpa);
    var storage_two: Storage = try .init(gpa, 4);
    defer storage_two.deinit(gpa);

    // The first session sends its begin on channel 0, and the second session on
    // channel 1. The peer answers the first one on its own channel 1, and the
    // second one on its own channel 0. Section 2.5.1 makes the two namespaces
    // independent, so each answer arrives on the number that the other session
    // sends on. One table for both namespaces gives each answer to the wrong
    // session, and both sessions then end with a protocol error.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_at = sink.written().len;
    try appendFrame(&sink, 1, remoteBegin(0), "");
    try appendFrame(&sink, 0, remoteBegin(1), "");

    // The first gate opens when the begin frame of the first session reaches
    // the wire. The second gate holds the answers back until the begin frame of
    // the second session follows it, so both sessions hold a channel before any
    // answer arrives.
    var gates = [_]Gate{
        .{ .at = gate_at, .after_frames = 2 },
        .{ .at = gate_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    var first: BeginTask = .{
        .connection = connection,
        .storage = &storage_one,
        .options = .{ .incoming_window = 4 },
    };
    var task = io.concurrent(BeginTask.run, .{&first}) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return error.SkipZigTest,
    };
    defer first.deinit();

    // The begin frame of the first session is on the wire, so that session
    // holds channel 0 and the second session takes channel 1.
    try gates[0].event.wait(io);
    const second_result = Session.begin(connection, &storage_two, .{ .incoming_window = 4 });
    task.await(io);

    const second = try second_result;
    defer second.deinit();

    try testing.expectEqual(@as(?anyerror, null), first.result);
    const one = first.session.?;
    try testing.expectEqual(@as(u16, 0), one.channel);
    try testing.expectEqual(@as(?u16, 1), one.remote_channel);
    try testing.expectEqual(@as(u16, 1), second.channel);
    try testing.expectEqual(@as(?u16, 0), second.remote_channel);

    // Each session took the window of its own answer, and neither one ended.
    try testing.expectEqual(@as(u32, 100), one.window.remote_incoming_window);
    try testing.expectEqual(@as(u32, 100), second.window.remote_incoming_window);
    try testing.expect(one.failure() == null);
    try testing.expect(second.failure() == null);
    try testing.expect(connection.failure() == null);
}

test "a session begins after the peer reuses the channel of an ended session" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage_one: Storage = try .init(gpa, 4);
    defer storage_one.deinit(gpa);
    var storage_two: Storage = try .init(gpa, 4);
    defer storage_two.deinit(gpa);

    // The first session begins on channel 0, and the peer answers on its
    // channel 5 and then ends the session with an error. Section 2.5.2 note (2)
    // frees both numbers at that point, and section 2.5.1 recommends the lowest
    // free number, so the peer answers the second session on channel 5 again.
    // A binding that outlived the first session would take that answer, and the
    // second session would wait for it forever.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const first_at = sink.written().len;
    try appendFrame(&sink, 5, remoteBegin(0), "");
    try appendFrame(&sink, 5, .{ .end = .{ .error_condition = .{
        .condition = .of("amqp:internal-error"),
        .description = "the broker lost the session",
    } } }, "");
    const second_at = sink.written().len;
    try appendFrame(&sink, 5, remoteBegin(0), "");

    // The frames of this peer are the open, the begin of the first session, the
    // end that answers the peer, and the begin of the second session.
    var gates = [_]Gate{
        .{ .at = first_at, .after_frames = 2 },
        .{ .at = second_at, .after_frames = 4 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const first = try Session.begin(connection, &storage_one, .{ .incoming_window = 4 });
    defer first.deinit();

    // The router task of the first session read the end frame of the peer, so
    // the session ended and gave both numbers back. The caller still holds it.
    try first.ended.wait(io);
    try testing.expectEqual(Error.SessionRemoteError, first.failure().?.err);

    const second = try Session.begin(connection, &storage_two, .{ .incoming_window = 4 });
    defer second.deinit();

    // The lowest free channel is 0 again, and the answer of the peer on its
    // channel 5 reaches the new session.
    try testing.expectEqual(@as(u16, 0), second.channel);
    try testing.expectEqual(@as(?u16, 5), second.remote_channel);
    try testing.expectEqual(@as(u32, 100), second.window.remote_incoming_window);
    try testing.expect(second.failure() == null);
    try testing.expect(connection.failure() == null);
}

test "a discarding session frees its channels when the end of the peer arrives" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage_one: Storage = try .init(gpa, 4);
    defer storage_one.deinit(gpa);
    var storage_two: Storage = try .init(gpa, 4);
    defer storage_two.deinit(gpa);

    // The peer names a handle that no link uses, so the first session ends the
    // session with an error and discards frames until the end frame of the peer
    // arrives. Section 2.5.4 ends the discarding state there, and the numbers
    // must go back at that point too.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const first_at = sink.written().len;
    try appendFrame(&sink, 5, remoteBegin(0), "");
    try appendFrame(&sink, 5, .{ .flow = .{
        .next_incoming_id = 0,
        .incoming_window = 5,
        .next_outgoing_id = 0,
        .outgoing_window = 5,
        .handle = 9,
    } }, "");
    const end_at = sink.written().len;
    try appendFrame(&sink, 5, .{ .end = .{} }, "");
    const second_at = sink.written().len;
    try appendFrame(&sink, 5, remoteBegin(0), "");

    // The frames of this peer are the open, the begin of the first session, the
    // end that reports the bad handle, and the begin of the second session.
    var gates = [_]Gate{
        .{ .at = first_at, .after_frames = 2 },
        .{ .at = end_at, .after_frames = 3 },
        .{ .at = second_at, .after_frames = 4 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const first = try Session.begin(connection, &storage_one, .{ .incoming_window = 4 });
    defer first.deinit();

    try first.ended.wait(io);
    try testing.expectEqualStrings(condition.unattached_handle, first.failure().?.condition.?);
    try waitRemoteEnd(io, first);

    // The peer reuses its channel 5 for the next session, and the answer
    // reaches that session.
    const second = try Session.begin(connection, &storage_two, .{ .incoming_window = 4 });
    defer second.deinit();

    try testing.expectEqual(@as(u16, 0), second.channel);
    try testing.expectEqual(@as(?u16, 5), second.remote_channel);
    try testing.expect(connection.failure() == null);
}

test "a second answering begin binds no second channel" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    // A broken peer answers the same session two times, on two channels of its
    // own. Section 2.5.1 gives the session one channel of the peer, so the
    // second answer binds nothing. A second binding would outlive the session,
    // because the release path knows one number alone.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_at = sink.written().len;
    try appendFrame(&sink, 5, remoteBegin(0), "");
    try appendFrame(&sink, 7, remoteBegin(0), "");

    var gates = [_]Gate{.{ .at = gate_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    try testing.expectEqual(@as(?u16, 5), session.remote_channel);

    // The second answer reaches the session, which reports it as a second begin
    // frame on the channel.
    try session.ended.wait(io);
    const f = session.failure().?;
    try testing.expectEqual(Error.SessionProtocolError, f.err);
    try testing.expectEqualStrings(condition.illegal_state, f.condition.?);
    try testing.expectEqual(@as(u32, 1), connection.remote_channels.count());
}

test "a begin answer without a mandatory field gives the channels back" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    // Section 2.7.2 makes `incoming-window` mandatory on a begin frame. The
    // demultiplexer bound its channel 7 to this session before the session read
    // the frame, so the failed begin must give both numbers back.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_at = sink.written().len;
    try appendFrame(&sink, 7, .{ .begin = .{
        .remote_channel = 0,
        .next_outgoing_id = 0,
        .outgoing_window = 100,
    } }, "");

    var gates = [_]Gate{.{ .at = gate_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    try testing.expectError(
        error.MissingField,
        Session.begin(connection, &storage, .{ .incoming_window = 4 }),
    );

    // Section 2.5.4: the session was mapped, so it must report the fault with
    // an end frame before it goes.
    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();
    const frame = written.find(.end).?;
    try testing.expectEqual(@as(u16, 0), frame.channel);
    try testing.expectEqualStrings(
        condition.not_allowed,
        frame.body.?.end.error_condition.?.condition.?.text,
    );

    // The channel of this peer is free for the next session.
    var buf: [1]Frame = undefined;
    var other: FrameQueue = .init(&buf);
    try connection.registerChannel(0, &other);
    // The binding of the channel of the remote peer is gone with it.
    try testing.expectEqual(@as(u32, 0), connection.remote_channels.count());
}

test "a queue smaller than the incoming window is refused" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 2);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);

    var gates = [_]Gate{};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    try testing.expectError(
        error.QueueTooSmall,
        Session.begin(connection, &storage, .{ .incoming_window = 3 }),
    );
}

test "two links cannot share one name" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    var buf_one: [1]Frame = undefined;
    var buf_two: [1]Frame = undefined;
    var queue_one: FrameQueue = .init(&buf_one);
    var queue_two: FrameQueue = .init(&buf_two);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_at = sink.written().len;
    try appendFrame(&sink, 0, remoteBegin(0), "");

    var gates = [_]Gate{.{ .at = gate_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    _ = try session.attachLink("the-link", &queue_one);
    try testing.expectError(
        error.LinkNameInUse,
        session.attachLink("the-link", &queue_two),
    );

    // A detach frees the handle for the next link.
    session.detachLink(0);
    try testing.expectEqual(@as(u32, 0), try session.attachLink("the-link", &queue_two));
}

test "a transfer that passes the window of the remote peer is refused" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_at = sink.written().len;
    // The remote peer accepts one transfer frame.
    try appendFrame(&sink, 0, .{ .begin = .{
        .remote_channel = 0,
        .next_outgoing_id = 0,
        .incoming_window = 1,
        .outgoing_window = 1,
    } }, "");

    var gates = [_]Gate{.{ .at = gate_at, .after_frames = 2 }};
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    try session.sendTransfer(.{ .handle = 0, .delivery_id = 0 }, "one");
    try testing.expectEqual(@as(u32, 1), session.window.next_outgoing_id);
    try testing.expectError(
        error.RemoteWindowClosed,
        session.sendTransfer(.{ .handle = 0 }, "two"),
    );
}

test "a stalled router leaves the answer on the reused channel to the new session" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage_a: Storage = try .init(gpa, 4);
    defer storage_a.deinit(gpa);
    var storage_b: Storage = try .init(gpa, 4);
    defer storage_b.deinit(gpa);

    // Session A lives on local 0, and the peer sends on its channel 5. Session
    // B begins on local 1 while A still runs. The peer then ends A and answers
    // B on channel 5, in two frames that arrive one after the other. Note (1)
    // of section 2.5.2 frees the channel at the peer when it sends its end, and
    // section 2.5.1 recommends the lowest free number, so that wire order is
    // legal and common.
    //
    // The test holds `state_mutex` of A, so the router task of A blocks inside
    // `sendEndBestEffort` and cannot give the channels back. A slow router
    // gives the same window in production: `sendTransfer` holds that lock
    // across the socket write, and the write stalls under back pressure. The
    // demultiplexer must free the binding of channel 5 itself, or the answer
    // for B lands in the dead queue of A and B waits inside `begin` forever.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const gate_a = sink.written().len;
    try appendFrame(&sink, 5, remoteBegin(0), "");
    const gate_b = sink.written().len;
    try appendFrame(&sink, 5, .{ .end = .{ .error_condition = .{
        .condition = .of("amqp:internal-error"),
        .description = "the broker lost the session",
    } } }, "");
    try appendFrame(&sink, 5, remoteBegin(1), "");

    // The frames of this peer are the open, the begin of A, and the begin of B.
    var gates = [_]Gate{
        .{ .at = gate_a, .after_frames = 2 },
        .{ .at = gate_b, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const a = try Session.begin(connection, &storage_a, .{ .incoming_window = 4 });
    defer a.deinit();
    try testing.expectEqual(@as(?u16, 5), a.remote_channel);

    try a.state_mutex.lock(io);

    var b_task: BeginTask = .{
        .connection = connection,
        .storage = &storage_b,
        .options = .{ .incoming_window = 4 },
    };
    var task = io.concurrent(BeginTask.run, .{&b_task}) catch |err| switch (err) {
        error.ConcurrencyUnavailable => {
            a.state_mutex.unlock(io);
            return error.SkipZigTest;
        },
    };
    defer b_task.deinit();

    // The begin frame of B opens the second gate, and the demultiplexer then
    // routes the end of A and the answer for B while the router of A waits for
    // the lock.
    try io.sleep(.fromMilliseconds(100), .awake);
    a.state_mutex.unlock(io);

    try a.ended.wait(io);
    try testing.expectEqual(Error.SessionRemoteError, a.failure().?.err);

    // B must get the answer that the peer sent on the reused channel 5.
    var settle: usize = 0;
    while (b_task.session == null and b_task.result == null and settle < 200) : (settle += 1) {
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    if (b_task.session == null and b_task.result == null) {
        task.cancel(io);
        return error.StaleBindingSwallowedTheAnswer;
    }

    task.await(io);
    try testing.expectEqual(@as(?anyerror, null), b_task.result);
    const second = b_task.session.?;
    try testing.expectEqual(@as(u16, 1), second.channel);
    try testing.expectEqual(@as(?u16, 5), second.remote_channel);
    try testing.expectEqual(@as(u32, 100), second.window.remote_incoming_window);
    try testing.expect(second.failure() == null);
    try testing.expect(connection.failure() == null);
}

test "an echo flow that crosses the local end draws no frame after the end" {
    const gpa = testing.allocator;
    const io = testing.io;
    var watchdog = startWatchdog(io);
    defer stopWatchdog(io, &watchdog);

    var storage: Storage = try .init(gpa, 4);
    defer storage.deinit(gpa);

    // This peer calls end. A flow with an echo request from the remote peer
    // crosses that end frame on the wire. Section 2.5.5 puts this endpoint in
    // END_SENT, where it "MAY receive frames, but cannot send them", so the
    // answer to the echo must not go out after the end frame.
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try scriptOpen(&sink);
    const begin_at = sink.written().len;
    try appendFrame(&sink, 5, remoteBegin(0), "");
    const cross_at = sink.written().len;
    try appendFrame(&sink, 5, .{ .flow = .{
        .next_incoming_id = 0,
        .incoming_window = 50,
        .next_outgoing_id = 0,
        .outgoing_window = 60,
        .echo = true,
    } }, "");
    try appendFrame(&sink, 5, .{ .end = .{} }, "");

    // Frame 3 of this peer is its end frame. The flow of the remote peer
    // arrives after it, which models the crossing.
    var gates = [_]Gate{
        .{ .at = begin_at, .after_frames = 2 },
        .{ .at = cross_at, .after_frames = 3 },
    };
    var peer: Peer = .init(gpa, io, sink.written(), &gates, .wait);
    defer peer.deinit();
    const stream = peer.ready();

    const connection = try Connection.open(gpa, io, stream, .{});
    defer connection.deinit();

    const session = try Session.begin(connection, &storage, .{ .incoming_window = 4 });
    defer session.deinit();

    try session.end(null);

    var written: SentFrames = try .parse(gpa, peer.sent());
    defer written.deinit();

    // Walk the wire in order. No session frame comes after the end frame.
    var end_seen = false;
    var after_end: usize = 0;
    for (written.frames.items) |frame| {
        const body = frame.body orelse continue;
        if (body == .end) {
            end_seen = true;
            continue;
        }
        if (end_seen) after_end += 1;
    }
    try testing.expectEqual(@as(usize, 0), after_end);
}
