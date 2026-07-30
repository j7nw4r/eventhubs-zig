//! The AMQP 1.0 transport.
//!
//! This file opens the byte stream that the frame layer reads and writes. It
//! resolves the host, connects a TCP stream, adds TLS when the caller asks for
//! it, runs the SASL dialog, and exchanges the AMQP protocol header. After
//! `connect` returns, the caller writes performatives with `framing.writeFrame`
//! and reads them with `framing.readFrame`.
//!
//! Specifications:
//! OASIS AMQP Version 1.0 Part 2: Transport, sections 2.1 and 2.2.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html
//! OASIS AMQP Version 1.0 Part 5: Security, sections 5.2 and 5.3.
//! https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html
//! IETF RFC 4616, the PLAIN SASL mechanism.
//! https://www.rfc-editor.org/rfc/rfc4616
//! IETF RFC 4505, the ANONYMOUS SASL mechanism.
//! https://www.rfc-editor.org/rfc/rfc4505
//!
//! # The order of the handshake
//!
//! Section 5.3.2 gives the order, and `connect` follows it:
//!
//! 1. Open the TCP stream, and add TLS when `Options.tls` is `.required`.
//! 2. Send the SASL protocol header, and read the same header back.
//! 3. Read `sasl-mechanisms`, and make sure the peer offers the mechanism that
//!    the caller selected.
//! 4. Send `sasl-init`.
//! 5. Read `sasl-outcome`, and make sure the code is `ok`.
//! 6. Send the AMQP protocol header, and read the same header back.
//!
//! # The mechanisms
//!
//! This file speaks ANONYMOUS of RFC 4505 and PLAIN of RFC 4616. A peer that
//! authorizes with a token, and not with the SASL dialog, takes ANONYMOUS.
//!
//! EXTERNAL is a non-goal. The mechanism needs a client certificate, and
//! `std.crypto.tls.Client` sends none.
//!
//! # Credentials
//!
//! A password never reaches a log line, an error, or a diagnostic. `Sasl.Plain`
//! holds one, and its `format` method prints the word `redacted` in place of
//! every field. The SASL code builds the initial response in a stack buffer and
//! erases the buffer before it returns, on the error path as well. `close`
//! erases the write buffers, because the cleartext of the initial response sits
//! in one of them until another write covers it. The failure paths of `connect`
//! erase the same buffers, because a rejected password is the usual failure of
//! a PLAIN dialog and it must not survive in freed memory.
//!
//! No test covers that last rule, and the reason is worth stating. Under
//! runtime safety `std.mem.Allocator.free` fills the memory with `undefined`
//! before it calls `rawFree` (`mem/Allocator.zig:448`), so a test allocator
//! always sees an erased buffer and can never fail. A release build skips that
//! fill, so the credential would survive there. The rule therefore holds by
//! reading the code, and a test would give false assurance.
//!
//! # The flush rule that this file exists to hold
//!
//! `std.crypto.tls.Client.flush` encrypts the buffered cleartext into the
//! buffer of the network writer and advances it (`crypto/tls/Client.zig:999`).
//! It does not flush the network writer, so the octets stay in memory and the
//! peer waits. A read after such a flush blocks until the connection times out.
//!
//! `Transport.writer` therefore returns a writer of this file for the TLS path.
//! Its `flush` empties the TLS writer and then the network writer, so one call
//! to `writer().flush()` puts the octets on the wire whether or not TLS is
//! active. `framing.exchangeProtocolHeader` and every later caller can flush
//! the writer that `Transport.writer` returns and expect the peer to see the
//! octets.
//!
//! # Addresses
//!
//! `std.crypto.tls.Client` keeps pointers to the reader and the writer of the
//! network stream, and the writer of this file finds its state with
//! `@fieldParentPtr`. So a `Transport` must not move after `connect` builds it.
//! `connect` therefore returns a pointer to a `Transport` on the heap, and
//! `close` frees it.

const builtin = @import("builtin");
const std = @import("std");

const framing = @import("framing.zig");
const performatives = @import("performatives.zig");

const Allocator = std.mem.Allocator;
const Reader = std.Io.Reader;
const Writer = std.Io.Writer;
const tls = std.crypto.tls;

// -------------------------------------------------------------------------
// The configuration
// -------------------------------------------------------------------------

/// Whether the transport puts TLS below the AMQP protocol header.
pub const TlsMode = enum {
    /// A plain TCP stream. Use this for a broker on a trusted network, such as
    /// a local test broker on port 5672.
    disabled,
    /// A TLS session with the system certificate bundle and host name
    /// verification. Use this for every public endpoint.
    required,
};

/// The SASL mechanism and the data that it needs.
pub const Sasl = union(enum) {
    /// The ANONYMOUS mechanism of RFC 4505. The initial response is absent, so
    /// the dialog carries no identity. Use it with a peer that authorizes
    /// later, over a token.
    anonymous,
    /// The PLAIN mechanism of RFC 4616.
    plain: Plain,

    /// The identity and the secret of the PLAIN mechanism.
    pub const Plain = struct {
        /// The authentication identity. RFC 4616 calls it the authcid.
        authcid: []const u8,
        /// The secret of the authentication identity.
        password: []const u8,

        /// Prints the value with every field removed.
        ///
        /// The method exists so that `{f}` on a credential cannot put a secret
        /// in a log line or an error message.
        pub fn format(self: Plain, w: *Writer) Writer.Error!void {
            _ = self;
            try w.writeAll("plain(redacted)");
        }
    };

    /// Returns the mechanism name that `sasl-init` carries.
    pub fn mechanismName(self: Sasl) []const u8 {
        return switch (self) {
            .anonymous => "ANONYMOUS",
            .plain => "PLAIN",
        };
    }

    /// Prints the value with every credential removed.
    pub fn format(self: Sasl, w: *Writer) Writer.Error!void {
        try w.writeAll(self.mechanismName());
    }
};

/// The arguments of `Transport.connect` that are neither the host nor the port.
pub const Options = struct {
    /// Whether to put TLS below the AMQP protocol header.
    tls: TlsMode,
    /// The SASL mechanism and its data.
    sasl: Sasl,
    /// How long the TCP connection may take. The SASL dialog and the protocol
    /// header exchange have no timeout of their own, because
    /// `std.Io.net.Stream` holds no read deadline. A caller that needs one runs
    /// `connect` in a task and cancels the task.
    connect_timeout: std.Io.Timeout = .none,
    /// Where to write the detail of a failed handshake. The call fills it
    /// before it returns the error.
    diagnostics: ?*Diagnostics = null,
};

// -------------------------------------------------------------------------
// The diagnostics
// -------------------------------------------------------------------------

/// The number of octets that `Diagnostics` keeps for the offered mechanisms.
///
/// Section 5.3.1 holds a SASL frame at 512 octets, so a peer can name more
/// mechanisms than this buffer holds. `offered` therefore truncates, and it
/// says so. The text is for a person reading a failure, not for a decision.
pub const max_offered_text: usize = 256;

/// The detail of a failed handshake.
///
/// No field of this type ever holds a credential. The mechanism names of a
/// peer are public, and the outcome code is a number.
pub const Diagnostics = struct {
    /// The two protocol headers of a failed exchange. Read it after a call
    /// returned `error.ProtocolHeaderMismatch`, and give it to
    /// `framing.describeProtocolHeader` for readable text.
    header_mismatch: framing.ProtocolHeaderMismatch = .{
        .sent = @splat(0),
        .received = @splat(0),
    },
    /// The code of a `sasl-outcome` that was not `ok`.
    outcome_code: ?performatives.SaslCode = null,
    /// The storage behind `offered`.
    offered_buf: [max_offered_text]u8 = undefined,
    /// The number of octets of `offered_buf` that hold text.
    offered_len: usize = 0,

    /// Returns the mechanisms that the peer offered, separated by a comma and a
    /// space. The text is empty until a call fills it, and a peer that offers
    /// more than `max_offered_text` octets of names loses the last of them.
    pub fn offered(self: *const Diagnostics) []const u8 {
        return self.offered_buf[0..self.offered_len];
    }
};

// -------------------------------------------------------------------------
// The errors
// -------------------------------------------------------------------------

/// The errors of the SASL dialog and the protocol header exchange.
pub const HandshakeError = framing.ReadFrameError || framing.WriteFrameError ||
    framing.ExchangeError || error{
    /// The peer did not offer the mechanism that the caller selected. Read
    /// `Diagnostics.offered` to learn which mechanisms it offered.
    MechanismNotOffered,
    /// The peer answered `sasl-outcome` with a code other than `ok`. Read
    /// `Diagnostics.outcome_code` to learn the code.
    SaslRejected,
    /// The peer sent a frame that the step does not allow.
    UnexpectedPerformative,
    /// The `sasl-mechanisms` frame left the mandatory field out.
    MissingField,
    /// The identity and the secret do not fit in a SASL frame.
    CredentialTooLarge,
};

/// The errors of `Transport.connect`.
pub const ConnectError = HandshakeError || Allocator.Error ||
    std.Io.net.HostName.ValidateError || std.Io.net.HostName.ConnectError ||
    tls.Client.InitError || std.crypto.Certificate.Bundle.RescanError;

// -------------------------------------------------------------------------
// The transport
// -------------------------------------------------------------------------

/// One connected AMQP 1.0 byte stream.
///
/// Build it with `connect` and free it with `close`. Read the address note at
/// the top of this file: the value must not move.
pub const Transport = struct {
    gpa: Allocator,
    io: std.Io,
    stream: std.Io.net.Stream,

    /// The buffer of `net_reader`. TLS asserts a size, so the field holds the
    /// slice for `close` to free.
    net_read_buf: []u8,
    /// The buffer of `net_writer`.
    net_write_buf: []u8,
    net_reader: std.Io.net.Stream.Reader,
    net_writer: std.Io.net.Stream.Writer,

    /// The TLS session, or null for a plain stream.
    tls_state: ?TlsState,

    /// The TLS session and the buffers that it needs.
    pub const TlsState = struct {
        client: tls.Client,
        bundle: std.crypto.Certificate.Bundle,
        lock: std.Io.RwLock,
        read_buf: []u8,
        write_buf: []u8,
        /// The buffer of `writer`.
        stage_buf: []u8,
        /// The writer that `Transport.writer` returns. Its `flush` reaches the
        /// socket. Read the flush note at the top of this file.
        writer: Writer,
    };

    /// The number of octets that the staging writer buffers. The value is one
    /// TLS record, so a frame of any legal size takes at most two drains.
    const stage_buf_len: usize = 16 * 1024;

    /// Opens the stream, runs the SASL dialog, and exchanges the AMQP protocol
    /// header.
    ///
    /// `host` must be a host name, and `Options.tls` names the same host to the
    /// certificate check, so a certificate for another name fails the call.
    ///
    /// The result points to heap memory that the call took from `gpa`. Free it
    /// with `close`.
    ///
    /// The call writes to `Options.diagnostics` before it returns an error of
    /// the handshake. It never writes a credential there.
    pub fn connect(
        gpa: Allocator,
        io: std.Io,
        host: []const u8,
        port: u16,
        options: Options,
    ) ConnectError!*Transport {
        const host_name: std.Io.net.HostName = try .init(host);

        const self = try gpa.create(Transport);
        errdefer gpa.destroy(self);

        // Both TLS buffers must hold one whole ciphertext record, and a short
        // write buffer panics inside the standard library instead of returning
        // an error, so the plain path takes the same size for one rule.
        const buf_len = tls.Client.min_buffer_len;
        const net_read_buf = try gpa.alloc(u8, buf_len);
        errdefer gpa.free(net_read_buf);
        const net_write_buf = try gpa.alloc(u8, buf_len);
        // The buffer holds the cleartext of a PLAIN initial response on the
        // plain path, and this errdefer runs when the SASL dialog fails. A
        // rejected password is the usual failure, so erase before the free.
        errdefer {
            std.crypto.secureZero(u8, net_write_buf);
            gpa.free(net_write_buf);
        }

        const stream = try host_name.connect(io, port, .{
            .mode = .stream,
            .timeout = options.connect_timeout,
        });
        errdefer stream.close(io);

        self.* = .{
            .gpa = gpa,
            .io = io,
            .stream = stream,
            .net_read_buf = net_read_buf,
            .net_write_buf = net_write_buf,
            .net_reader = stream.reader(io, net_read_buf),
            .net_writer = stream.writer(io, net_write_buf),
            .tls_state = null,
        };

        if (options.tls == .required) try self.startTls(host);
        // The cleartext of a PLAIN initial response sits in `stage_buf` and in
        // `write_buf` until another write covers it, and this errdefer runs
        // when the SASL dialog fails. Erase both before the free, as `close`
        // does on the path that succeeds.
        errdefer if (self.tls_state) |*state| {
            state.bundle.deinit(gpa);
            gpa.free(state.read_buf);
            std.crypto.secureZero(u8, state.write_buf);
            gpa.free(state.write_buf);
            std.crypto.secureZero(u8, state.stage_buf);
            gpa.free(state.stage_buf);
        };

        try performSasl(gpa, self.reader(), self.writer(), options.sasl, options.diagnostics);
        try framing.exchangeProtocolHeader(
            self.reader(),
            self.writer(),
            framing.amqp_protocol_header,
            if (options.diagnostics) |d| &d.header_mismatch else null,
        );
        return self;
    }

    /// Builds the TLS session in place. The caller has already filled every
    /// other field of `self`.
    fn startTls(self: *Transport, host: []const u8) ConnectError!void {
        const gpa = self.gpa;
        const io = self.io;

        var bundle: std.crypto.Certificate.Bundle = .empty;
        errdefer bundle.deinit(gpa);
        const now: std.Io.Timestamp = .now(io, .real);
        try bundle.rescan(gpa, io, now);

        const read_buf = try gpa.alloc(u8, tls.Client.min_buffer_len);
        errdefer gpa.free(read_buf);
        const write_buf = try gpa.alloc(u8, tls.Client.min_buffer_len);
        errdefer gpa.free(write_buf);
        const stage_buf = try gpa.alloc(u8, stage_buf_len);
        errdefer gpa.free(stage_buf);

        self.tls_state = .{
            .client = undefined,
            .bundle = bundle,
            .lock = .init,
            .read_buf = read_buf,
            .write_buf = write_buf,
            .stage_buf = stage_buf,
            .writer = .{
                .vtable = &.{ .drain = tlsDrain, .flush = tlsFlush },
                .buffer = stage_buf,
            },
        };
        // The errdefers above free the buffers and the bundle when the
        // handshake fails, so the field must not keep pointers to them.
        errdefer self.tls_state = null;
        const state = &self.tls_state.?;

        var entropy: [tls.Client.Options.entropy_len]u8 = undefined;
        io.random(&entropy);
        defer std.crypto.secureZero(u8, &entropy);

        // The bundle and the lock must already sit at their final addresses,
        // because the client keeps pointers to them.
        state.client = try .init(&self.net_reader.interface, &self.net_writer.interface, .{
            .host = .{ .explicit = host },
            .ca = .{ .bundle = .{
                .gpa = gpa,
                .io = io,
                .lock = &state.lock,
                .bundle = &state.bundle,
            } },
            .read_buffer = read_buf,
            .write_buffer = write_buf,
            .entropy = &entropy,
            .realtime_now = now,
        });
    }

    /// Returns the reader of the stream. The reader gives cleartext whether or
    /// not TLS is active.
    pub fn reader(self: *Transport) *Reader {
        if (self.tls_state) |*state| return &state.client.reader;
        return &self.net_reader.interface;
    }

    /// Returns the writer of the stream. `flush` on the result puts the octets
    /// on the wire whether or not TLS is active.
    pub fn writer(self: *Transport) *Writer {
        if (self.tls_state) |*state| return &state.writer;
        return &self.net_writer.interface;
    }

    /// Closes the stream and frees every buffer, including `self`.
    ///
    /// The function erases each write buffer before it frees it, because the
    /// cleartext of a PLAIN initial response stays in one of them until another
    /// write covers it.
    pub fn close(self: *Transport) void {
        const gpa = self.gpa;
        if (self.tls_state) |*state| {
            // Section 5.3 of RFC 8446 asks for close_notify. A peer that is
            // already gone makes the write fail, and the connection ends
            // either way.
            state.client.end() catch {};
            self.net_writer.interface.flush() catch {};
            std.crypto.secureZero(u8, state.stage_buf);
            std.crypto.secureZero(u8, state.write_buf);
            state.bundle.deinit(gpa);
            gpa.free(state.read_buf);
            gpa.free(state.write_buf);
            gpa.free(state.stage_buf);
        }
        std.crypto.secureZero(u8, self.net_write_buf);
        self.stream.close(self.io);
        gpa.free(self.net_read_buf);
        gpa.free(self.net_write_buf);
        gpa.destroy(self);
    }
};

/// Moves the staged octets into the TLS writer. Read the flush note at the top
/// of this file.
fn tlsDrain(w: *Writer, data: []const []const u8, splat: usize) Writer.Error!usize {
    const state: *Transport.TlsState = @alignCast(@fieldParentPtr("writer", w));
    try state.client.writer.writeAll(w.buffered());
    _ = w.consumeAll();
    return state.client.writer.writeSplat(data, splat);
}

/// Empties the staging buffer into the TLS writer, encrypts, and then puts the
/// ciphertext on the wire. Read the flush note at the top of this file.
fn tlsFlush(w: *Writer) Writer.Error!void {
    const state: *Transport.TlsState = @alignCast(@fieldParentPtr("writer", w));
    try state.client.writer.writeAll(w.buffered());
    _ = w.consumeAll();
    try state.client.writer.flush();
    try state.client.output.flush();
}

// -------------------------------------------------------------------------
// The SASL dialog
// -------------------------------------------------------------------------

/// The largest initial response that this file builds, in octets.
///
/// The value is the size of a whole SASL frame, and the frame also carries a
/// header and a mechanism name, so a response near this size still fails. That
/// is deliberate: `writeFrame` holds the size rule of section 5.3.1, and this
/// constant only bounds the stack buffer. A caller sees `error.FrameTooLarge`
/// from the frame layer and `error.CredentialTooLarge` from this buffer.
const max_initial_response: usize = framing.min_max_frame_size;

/// Runs the SASL dialog of section 5.3.2 on `r` and `w`.
///
/// The function sends the SASL protocol header, reads the same header back,
/// reads `sasl-mechanisms`, makes sure the peer offers the selected mechanism,
/// sends `sasl-init`, and reads `sasl-outcome`. It returns when the code of the
/// outcome is `ok`.
///
/// `Transport.connect` calls this function, and a test calls it with the reader
/// and the writer of a `MockTransport`.
///
/// The function writes to `diag` before it returns an error, and it never puts
/// a credential there. It erases its own copy of the initial response before it
/// returns, on the error path as well.
pub fn performSasl(
    gpa: Allocator,
    r: *Reader,
    w: *Writer,
    sasl: Sasl,
    diag: ?*Diagnostics,
) HandshakeError!void {
    try framing.exchangeProtocolHeader(
        r,
        w,
        framing.sasl_protocol_header,
        if (diag) |d| &d.header_mismatch else null,
    );

    var buf: [framing.min_max_frame_size]u8 = undefined;

    // Step 1: the peer offers its mechanisms.
    {
        const frame = try framing.readFrame(gpa, r, &buf, framing.min_max_frame_size);
        defer frame.deinit();
        const body = frame.body orelse return error.UnexpectedPerformative;
        if (body != .sasl_mechanisms) return error.UnexpectedPerformative;
        const offered = body.sasl_mechanisms.sasl_server_mechanisms orelse return error.MissingField;
        if (diag) |d| recordOffered(d, offered);

        const wanted = sasl.mechanismName();
        var found = false;
        for (offered) |mechanism| {
            if (std.mem.eql(u8, mechanism.text, wanted)) {
                found = true;
                break;
            }
        }
        if (!found) return error.MechanismNotOffered;
    }

    // Step 2: this peer selects one mechanism and sends its initial response.
    {
        var response: [max_initial_response]u8 = undefined;
        defer std.crypto.secureZero(u8, &response);

        // Section 5.3.3.2 gives `sasl-init` an optional `hostname` field, for a
        // peer that serves more than one virtual host. This call omits it,
        // because the brokers in scope select the host from the connection.
        // A peer with virtual hosts would need it.
        const init: performatives.SaslInit = .{
            .mechanism = .of(sasl.mechanismName()),
            .initial_response = switch (sasl) {
                // RFC 4505 gives ANONYMOUS an optional trace field, and this
                // library sends none, so the response is absent.
                .anonymous => null,
                // RFC 4616 section 2: authzid NUL authcid NUL passwd. The
                // authzid is empty, so the response starts with the NUL.
                .plain => |plain| blk: {
                    const len = plain.authcid.len + plain.password.len + 2;
                    if (len > response.len) return error.CredentialTooLarge;
                    var pos: usize = 0;
                    response[pos] = 0;
                    pos += 1;
                    @memcpy(response[pos..][0..plain.authcid.len], plain.authcid);
                    pos += plain.authcid.len;
                    response[pos] = 0;
                    pos += 1;
                    @memcpy(response[pos..][0..plain.password.len], plain.password);
                    pos += plain.password.len;
                    break :blk .of(response[0..pos]);
                },
            },
        };
        try framing.writeFrame(w, 0, .{ .sasl_init = init }, "", framing.min_max_frame_size);
        try w.flush();
    }

    // Step 3: the peer reports the outcome.
    {
        const frame = try framing.readFrame(gpa, r, &buf, framing.min_max_frame_size);
        defer frame.deinit();
        const body = frame.body orelse return error.UnexpectedPerformative;
        if (body != .sasl_outcome) return error.UnexpectedPerformative;
        const code = body.sasl_outcome.code orelse return error.MissingField;
        if (diag) |d| d.outcome_code = code;
        if (code != .ok) return error.SaslRejected;
    }
}

/// Writes the offered mechanism names into `diag`, separated by a comma and a
/// space. The function stops at `max_offered_text` octets.
fn recordOffered(diag: *Diagnostics, offered: []const performatives.Symbol) void {
    var w: Writer = .fixed(&diag.offered_buf);
    for (offered, 0..) |mechanism, i| {
        if (i != 0) w.writeAll(", ") catch break;
        w.writeAll(mechanism.text) catch break;
    }
    diag.offered_len = w.end;
}

// -------------------------------------------------------------------------
// The test double
// -------------------------------------------------------------------------

/// A `Transport` that reads a scripted answer and keeps every octet that the
/// code under test wrote.
///
/// Build it with the octets that the peer would send, in order. `reader` and
/// `writer` have the signatures of the same two methods of `Transport`, so a
/// test can drive the handshake and the frame layer with no socket.
///
/// The type exists only in a test build.
pub const MockTransport = if (builtin.is_test) struct {
    /// The scripted answer of the peer.
    script: Reader,
    /// Every octet that the code under test wrote.
    sink: Writer.Allocating,

    /// Returns a mock that answers with `script`.
    ///
    /// The result must not move after the first call to `reader` or `writer`,
    /// for the reason at the top of this file.
    pub fn init(gpa: Allocator, script: []const u8) MockTransport {
        return .{ .script = .fixed(script), .sink = .init(gpa) };
    }

    /// Frees the octets that the code under test wrote.
    pub fn deinit(self: *MockTransport) void {
        self.sink.deinit();
    }

    /// Returns the reader of the scripted answer.
    pub fn reader(self: *MockTransport) *Reader {
        return &self.script;
    }

    /// Returns the writer that keeps every octet.
    pub fn writer(self: *MockTransport) *Writer {
        return &self.sink.writer;
    }

    /// Returns every octet that the code under test wrote.
    pub fn sent(self: *MockTransport) []const u8 {
        return self.sink.written();
    }
} else void;

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

const testing = std.testing;

/// Appends one SASL frame that the peer would send to `sink`.
fn scriptFrame(sink: *Writer.Allocating, body: framing.Body) !void {
    try framing.writeFrame(&sink.writer, 0, body, "", framing.min_max_frame_size);
}

/// Builds the octets of a peer that offers `mechanisms` and then answers with
/// `code`. The caller frees the result.
fn scriptSasl(
    gpa: Allocator,
    mechanisms: []const performatives.Symbol,
    code: ?performatives.SaslCode,
) ![]u8 {
    var sink: Writer.Allocating = .init(gpa);
    errdefer sink.deinit();
    try sink.writer.writeAll(&framing.sasl_protocol_header);
    try scriptFrame(&sink, .{ .sasl_mechanisms = .{ .sasl_server_mechanisms = mechanisms } });
    if (code) |c| try scriptFrame(&sink, .{ .sasl_outcome = .{ .code = c } });
    return sink.toOwnedSlice();
}

test "the anonymous dialog sends the header, the init, and reads the outcome" {
    const gpa = testing.allocator;
    const script = try scriptSasl(gpa, &.{ .of("ANONYMOUS"), .of("PLAIN") }, .ok);
    defer gpa.free(script);

    var mock: MockTransport = .init(gpa, script);
    defer mock.deinit();

    var diag: Diagnostics = .{};
    try performSasl(gpa, mock.reader(), mock.writer(), .anonymous, &diag);

    // The peer must have seen the SASL header and then one sasl-init.
    var want: Writer.Allocating = .init(gpa);
    defer want.deinit();
    try want.writer.writeAll(&framing.sasl_protocol_header);
    try framing.writeFrame(
        &want.writer,
        0,
        .{ .sasl_init = .{ .mechanism = .of("ANONYMOUS") } },
        "",
        framing.min_max_frame_size,
    );
    try testing.expectEqualSlices(u8, want.written(), mock.sent());
    try testing.expectEqualStrings("ANONYMOUS, PLAIN", diag.offered());
    try testing.expectEqual(performatives.SaslCode.ok, diag.outcome_code.?);
}

test "the plain dialog sends the rfc 4616 initial response" {
    const gpa = testing.allocator;
    const script = try scriptSasl(gpa, &.{.of("PLAIN")}, .ok);
    defer gpa.free(script);

    var mock: MockTransport = .init(gpa, script);
    defer mock.deinit();

    try performSasl(gpa, mock.reader(), mock.writer(), .{ .plain = .{
        .authcid = "user",
        .password = "secret",
    } }, null);

    // RFC 4616 section 2: authzid NUL authcid NUL passwd, and the authzid is
    // empty.
    var want: Writer.Allocating = .init(gpa);
    defer want.deinit();
    try want.writer.writeAll(&framing.sasl_protocol_header);
    try framing.writeFrame(&want.writer, 0, .{ .sasl_init = .{
        .mechanism = .of("PLAIN"),
        .initial_response = .of("\x00user\x00secret"),
    } }, "", framing.min_max_frame_size);
    try testing.expectEqualSlices(u8, want.written(), mock.sent());
}

test "the dialog rejects a mechanism that the peer does not offer" {
    const gpa = testing.allocator;
    const script = try scriptSasl(gpa, &.{ .of("EXTERNAL"), .of("ANONYMOUS") }, .ok);
    defer gpa.free(script);

    var mock: MockTransport = .init(gpa, script);
    defer mock.deinit();

    var diag: Diagnostics = .{};
    try testing.expectError(error.MechanismNotOffered, performSasl(
        gpa,
        mock.reader(),
        mock.writer(),
        .{ .plain = .{ .authcid = "user", .password = "secret" } },
        &diag,
    ));
    try testing.expectEqualStrings("EXTERNAL, ANONYMOUS", diag.offered());
    // The dialog stopped before it wrote a sasl-init, so the peer saw the
    // header alone. A rejected mechanism must never put a secret on the wire.
    try testing.expectEqualSlices(u8, &framing.sasl_protocol_header, mock.sent());
}

test "the dialog reports every outcome code that is not ok" {
    const gpa = testing.allocator;
    for ([_]performatives.SaslCode{ .auth, .sys, .sys_perm, .sys_temp }) |code| {
        const script = try scriptSasl(gpa, &.{.of("PLAIN")}, code);
        defer gpa.free(script);

        var mock: MockTransport = .init(gpa, script);
        defer mock.deinit();

        var diag: Diagnostics = .{};
        try testing.expectError(error.SaslRejected, performSasl(
            gpa,
            mock.reader(),
            mock.writer(),
            .{ .plain = .{ .authcid = "user", .password = "secret" } },
            &diag,
        ));
        try testing.expectEqual(code, diag.outcome_code.?);
    }
}

test "the dialog names the protocol header that the peer sent" {
    const gpa = testing.allocator;
    // The peer answers the SASL header with the AMQP header.
    var mock: MockTransport = .init(gpa, &framing.amqp_protocol_header);
    defer mock.deinit();

    var diag: Diagnostics = .{};
    try testing.expectError(error.ProtocolHeaderMismatch, performSasl(
        gpa,
        mock.reader(),
        mock.writer(),
        .anonymous,
        &diag,
    ));

    var text: [framing.header_text_size]u8 = undefined;
    const mismatch = diag.header_mismatch;
    try testing.expectEqualStrings("AMQP 3.1.0.0", framing.describeProtocolHeader(mismatch.sent, &text));
    try testing.expectEqualStrings("AMQP 0.1.0.0", framing.describeProtocolHeader(mismatch.received, &text));
}

test "the dialog reports a truncation at each step" {
    const gpa = testing.allocator;
    const full = try scriptSasl(gpa, &.{.of("ANONYMOUS")}, .ok);
    defer gpa.free(full);

    // Every prefix of the script must fail, and none may hang or leak. The
    // steps are the 8-octet header, the mechanisms frame, and the outcome
    // frame.
    var len: usize = 0;
    while (len < full.len) : (len += 1) {
        var mock: MockTransport = .init(gpa, full[0..len]);
        defer mock.deinit();
        try testing.expectError(error.EndOfStream, performSasl(
            gpa,
            mock.reader(),
            mock.writer(),
            .anonymous,
            null,
        ));
    }
    // The whole script succeeds, so the loop above tested truncation and not a
    // script that never worked.
    var mock: MockTransport = .init(gpa, full);
    defer mock.deinit();
    try performSasl(gpa, mock.reader(), mock.writer(), .anonymous, null);
}

test "the dialog rejects a frame that is not the one the step wants" {
    const gpa = testing.allocator;

    // An open frame where the mechanisms frame belongs.
    var wrong_first: Writer.Allocating = .init(gpa);
    defer wrong_first.deinit();
    try wrong_first.writer.writeAll(&framing.sasl_protocol_header);
    try framing.writeFrame(
        &wrong_first.writer,
        0,
        .{ .open = .{ .container_id = "c" } },
        "",
        framing.min_max_frame_size,
    );

    var mock: MockTransport = .init(gpa, wrong_first.written());
    defer mock.deinit();
    try testing.expectError(error.UnexpectedPerformative, performSasl(
        gpa,
        mock.reader(),
        mock.writer(),
        .anonymous,
        null,
    ));

    // A second mechanisms frame where the outcome belongs.
    var wrong_second: Writer.Allocating = .init(gpa);
    defer wrong_second.deinit();
    try wrong_second.writer.writeAll(&framing.sasl_protocol_header);
    for (0..2) |_| try scriptFrame(&wrong_second, .{
        .sasl_mechanisms = .{ .sasl_server_mechanisms = &.{.of("ANONYMOUS")} },
    });

    var mock2: MockTransport = .init(gpa, wrong_second.written());
    defer mock2.deinit();
    try testing.expectError(error.UnexpectedPerformative, performSasl(
        gpa,
        mock2.reader(),
        mock2.writer(),
        .anonymous,
        null,
    ));
}

test "the dialog reports a mechanisms frame with no mechanisms field" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try sink.writer.writeAll(&framing.sasl_protocol_header);
    // Section 5.3.3.1 makes the field mandatory, and this frame leaves it out.
    try scriptFrame(&sink, .{ .sasl_mechanisms = .{} });

    var mock: MockTransport = .init(gpa, sink.written());
    defer mock.deinit();
    try testing.expectError(error.MissingField, performSasl(
        gpa,
        mock.reader(),
        mock.writer(),
        .anonymous,
        null,
    ));
}

test "the dialog reports a peer that offers no mechanism at all" {
    const gpa = testing.allocator;
    const script = try scriptSasl(gpa, &.{}, .ok);
    defer gpa.free(script);

    // The field is present and empty, so no mechanism matches.
    var mock: MockTransport = .init(gpa, script);
    defer mock.deinit();

    var diag: Diagnostics = .{};
    try testing.expectError(error.MechanismNotOffered, performSasl(
        gpa,
        mock.reader(),
        mock.writer(),
        .anonymous,
        &diag,
    ));
    try testing.expectEqual(@as(usize, 0), diag.offered().len);
}

test "the dialog reports an outcome frame with no code" {
    const gpa = testing.allocator;
    var sink: Writer.Allocating = .init(gpa);
    defer sink.deinit();
    try sink.writer.writeAll(&framing.sasl_protocol_header);
    try scriptFrame(&sink, .{ .sasl_mechanisms = .{ .sasl_server_mechanisms = &.{.of("ANONYMOUS")} } });
    try scriptFrame(&sink, .{ .sasl_outcome = .{} });

    var mock: MockTransport = .init(gpa, sink.written());
    defer mock.deinit();
    try testing.expectError(error.MissingField, performSasl(
        gpa,
        mock.reader(),
        mock.writer(),
        .anonymous,
        null,
    ));
}

test "an identity that is too large never reaches the wire" {
    const gpa = testing.allocator;
    const script = try scriptSasl(gpa, &.{.of("PLAIN")}, .ok);
    defer gpa.free(script);

    var mock: MockTransport = .init(gpa, script);
    defer mock.deinit();

    const long: [max_initial_response]u8 = @splat('x');
    try testing.expectError(error.CredentialTooLarge, performSasl(
        gpa,
        mock.reader(),
        mock.writer(),
        .{ .plain = .{ .authcid = &long, .password = &long } },
        null,
    ));
    try testing.expectEqualSlices(u8, &framing.sasl_protocol_header, mock.sent());
}

test "an identity that fills a sasl frame reports the frame size rule" {
    const gpa = testing.allocator;
    const script = try scriptSasl(gpa, &.{.of("PLAIN")}, .ok);
    defer gpa.free(script);

    var mock: MockTransport = .init(gpa, script);
    defer mock.deinit();

    // The response fits the buffer of `performSasl` and not the 512-octet SASL
    // frame of section 5.3.1.
    const long: [500]u8 = @splat('x');
    try testing.expectError(error.FrameTooLarge, performSasl(
        gpa,
        mock.reader(),
        mock.writer(),
        .{ .plain = .{ .authcid = "u", .password = &long } },
        null,
    ));
}

test "a credential prints with no secret" {
    var buf: [64]u8 = undefined;
    const sasl: Sasl = .{ .plain = .{ .authcid = "user", .password = "secret" } };

    var w: Writer = .fixed(&buf);
    try w.print("{f}", .{sasl});
    try testing.expectEqualStrings("PLAIN", w.buffered());

    var w2: Writer = .fixed(&buf);
    try w2.print("{f}", .{sasl.plain});
    try testing.expectEqualStrings("plain(redacted)", w2.buffered());
}

test "the mechanism names are the ones that the specifications give" {
    try testing.expectEqualStrings("ANONYMOUS", (Sasl{ .anonymous = {} }).mechanismName());
    try testing.expectEqualStrings("PLAIN", (Sasl{ .plain = .{
        .authcid = "u",
        .password = "p",
    } }).mechanismName());
}

test "the offered text stops at the size of its buffer" {
    var diag: Diagnostics = .{};
    const name: [max_offered_text]u8 = @splat('M');
    recordOffered(&diag, &.{ .of(&name), .of("PLAIN") });
    try testing.expectEqual(max_offered_text, diag.offered().len);
}

// -------------------------------------------------------------------------
// The loopback test of the plain path
// -------------------------------------------------------------------------
//
// A mock exercises the SASL dialog with no socket, and the live test below
// exercises the TLS path. This test covers the rest: `Transport.connect` on a
// plain TCP stream, the reader and the writer that it returns for that path,
// and `close`. It needs no credential and no network beyond the loopback
// interface.

/// The first port that `listenOnLoopback` tries.
const loopback_first_port: u16 = 45671;
/// How many ports `listenOnLoopback` tries before it gives up.
const loopback_port_count: u16 = 32;

/// Listens on the loopback interface and returns the server and its port.
///
/// The function tries a range of ports, because `std.Io.net.Server` reports no
/// way to read the port that the kernel selected for port 0.
fn listenOnLoopback(io: std.Io) !struct { std.Io.net.Server, u16 } {
    var port = loopback_first_port;
    while (port < loopback_first_port + loopback_port_count) : (port += 1) {
        const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(port) };
        const server = address.listen(io, .{ .reuse_address = true }) catch |err| switch (err) {
            error.AddressInUse => continue,
            else => |e| return e,
        };
        return .{ server, port };
    }
    return error.SkipZigTest;
}

/// Answers one client with a scripted ANONYMOUS dialog and then the AMQP
/// protocol header.
fn servePlainPeer(io: std.Io, server: *std.Io.net.Server) anyerror!void {
    const stream = try server.accept(io);
    defer stream.close(io);

    var read_buf: [framing.min_max_frame_size]u8 = undefined;
    var write_buf: [framing.min_max_frame_size]u8 = undefined;
    var peer_reader = stream.reader(io, &read_buf);
    var peer_writer = stream.writer(io, &write_buf);
    const r = &peer_reader.interface;
    const w = &peer_writer.interface;

    var header: [framing.protocol_header_size]u8 = undefined;
    try r.readSliceAll(&header);
    if (!std.mem.eql(u8, &header, &framing.sasl_protocol_header)) return error.TestUnexpectedResult;
    try w.writeAll(&framing.sasl_protocol_header);
    try framing.writeFrame(w, 0, .{ .sasl_mechanisms = .{
        .sasl_server_mechanisms = &.{.of("ANONYMOUS")},
    } }, "", framing.min_max_frame_size);
    try w.flush();

    // Read the sasl-init frame without decoding it. The size counts the 8
    // header octets, and section 2.3.1 holds it at or above 8.
    var frame_header: [framing.frame_header_size]u8 = undefined;
    try r.readSliceAll(&frame_header);
    const size = std.mem.readInt(u32, frame_header[0..4], .big);
    if (size < framing.frame_header_size) return error.TestUnexpectedResult;
    try r.discardAll(size - framing.frame_header_size);

    try framing.writeFrame(w, 0, .{ .sasl_outcome = .{ .code = .ok } }, "", framing.min_max_frame_size);
    try w.flush();

    try r.readSliceAll(&header);
    if (!std.mem.eql(u8, &header, &framing.amqp_protocol_header)) return error.TestUnexpectedResult;
    try w.writeAll(&framing.amqp_protocol_header);
    try w.flush();
}

/// Runs `servePlainPeer` and discards its error, so it can join a group.
fn servePlainPeerQuietly(io: std.Io, server: *std.Io.net.Server) void {
    servePlainPeer(io, server) catch {};
}

/// Connects to the scripted peer over a plain stream and closes the result.
fn plainRoundTrip(gpa: Allocator, io: std.Io, server: *std.Io.net.Server, port: u16) anyerror!void {
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    try group.concurrent(io, servePlainPeerQuietly, .{ io, server });

    const t = try Transport.connect(gpa, io, "localhost", port, .{
        .tls = .disabled,
        .sasl = .anonymous,
    });
    t.close();
}

test "the plain path completes the handshake over a loopback socket" {
    const gpa = testing.allocator;
    const io = testing.io;

    var server, const port = try listenOnLoopback(io);
    defer server.deinit(io);

    try withTimeout(io, plainRoundTrip, .{ gpa, io, &server, port });
}

// -------------------------------------------------------------------------
// The live tests
// -------------------------------------------------------------------------
//
// These tests need a real broker, so each one returns `error.SkipZigTest` when
// its environment variables are absent. A developer with no broker still gets a
// green build.
//
// Every live test runs under `withTimeout`, so a peer that never answers fails
// the test instead of stopping the suite.

/// The host name of the live broker. The value is a bare host name, such as
/// `broker.example.com`.
const live_host_var = "AMQP_LIVE_HOST";
/// The port of the live broker. The default is 5671, the AMQP over TLS port.
const live_port_var = "AMQP_LIVE_PORT";
/// The identity of the live SASL PLAIN test.
const live_authcid_var = "AMQP_LIVE_AUTHCID";
/// The secret of the live SASL PLAIN test. Its value never reaches an error, a
/// log line, or a test name.
const live_password_var = "AMQP_LIVE_PASSWORD";

/// How long a live test may take before it fails.
const live_timeout_seconds: i64 = 30;

/// Returns the value of `name`, or null when the variable is absent. The caller
/// frees the result.
fn envOrNull(gpa: Allocator, name: []const u8) !?[]u8 {
    return testing.environ.getAlloc(gpa, name) catch |err| switch (err) {
        error.EnvironmentVariableMissing => null,
        else => |e| return e,
    };
}

/// The result of one live test task.
const LiveResult = union(enum) {
    work: anyerror!void,
    timer: void,
};

/// Sleeps for `live_timeout_seconds` and returns.
fn liveTimer(io: std.Io) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .fromSeconds(live_timeout_seconds),
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}

/// Runs `function` with `args` and fails with `error.Timeout` when it does not
/// return within `live_timeout_seconds`.
///
/// `function` must return `anyerror!void` and must own every resource that it
/// takes, because a timeout cancels it and discards its result.
fn withTimeout(io: std.Io, function: anytype, args: anytype) !void {
    var buffer: [2]LiveResult = undefined;
    var select: std.Io.Select(LiveResult) = .init(io, &buffer);
    try select.concurrent(.work, function, args);
    select.async(.timer, liveTimer, .{io});

    const first = try select.await();
    select.cancelDiscard();
    return switch (first) {
        .work => |result| result,
        .timer => error.Timeout,
    };
}

/// Connects to the live broker with `sasl` and closes the connection.
fn liveConnect(gpa: Allocator, io: std.Io, host: []const u8, port: u16, sasl: Sasl) anyerror!void {
    var diag: Diagnostics = .{};
    const transport = Transport.connect(gpa, io, host, port, .{
        .tls = .required,
        .sasl = sasl,
        .diagnostics = &diag,
    }) catch |err| {
        // The diagnostic holds no credential, so it is safe to print.
        if (diag.offered().len != 0) {
            std.debug.print("the peer offered: {s}\n", .{diag.offered()});
        }
        if (diag.outcome_code) |code| {
            std.debug.print("the outcome code was: {t}\n", .{code});
        }
        return err;
    };
    transport.close();
}

test "live: the anonymous handshake completes against the broker" {
    const gpa = testing.allocator;
    const host = try envOrNull(gpa, live_host_var) orelse return error.SkipZigTest;
    defer gpa.free(host);
    const port = try livePort(gpa);

    const sasl: Sasl = .anonymous;
    try withTimeout(testing.io, liveConnect, .{ gpa, testing.io, host, port, sasl });
}

test "live: the plain handshake completes against the broker" {
    const gpa = testing.allocator;
    const host = try envOrNull(gpa, live_host_var) orelse return error.SkipZigTest;
    defer gpa.free(host);
    const authcid = try envOrNull(gpa, live_authcid_var) orelse return error.SkipZigTest;
    defer gpa.free(authcid);
    const password = try envOrNull(gpa, live_password_var) orelse return error.SkipZigTest;
    defer {
        std.crypto.secureZero(u8, password);
        gpa.free(password);
    }
    const port = try livePort(gpa);

    const sasl: Sasl = .{ .plain = .{ .authcid = authcid, .password = password } };
    try withTimeout(testing.io, liveConnect, .{ gpa, testing.io, host, port, sasl });
}

/// Returns the port of the live broker.
fn livePort(gpa: Allocator) !u16 {
    const text = try envOrNull(gpa, live_port_var) orelse return 5671;
    defer gpa.free(text);
    return std.fmt.parseInt(u16, text, 10);
}
