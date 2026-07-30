const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The two published modules. A consumer imports "amqp" for the AMQP 1.0
    // client alone, or "eventhubs" for the Event Hubs SDK.
    const amqp_mod = b.addModule("amqp", .{
        .root_source_file = b.path("src/amqp/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const eventhubs_mod = b.addModule("eventhubs", .{
        .root_source_file = b.path("src/eventhubs/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "amqp", .module = amqp_mod }},
    });

    // zig build test: one test binary for each module.
    const amqp_tests = b.addTest(.{
        .name = "amqp-test",
        .root_module = amqp_mod,
    });
    const eventhubs_tests = b.addTest(.{
        .name = "eventhubs-test",
        .root_module = eventhubs_mod,
    });

    const test_step = b.step("test", "Run the unit tests of both modules");
    test_step.dependOn(&b.addRunArtifact(amqp_tests).step);
    test_step.dependOn(&b.addRunArtifact(eventhubs_tests).step);

    // zig build fmt: the format check that CI runs. Add each new top-level
    // source directory to this list, or its files escape the check.
    const fmt_check = b.addFmt(.{
        .paths = &.{ "build.zig", "build.zig.zon", "src" },
        .check = true,
    });
    const fmt_step = b.step("fmt", "Make sure the source is formatted");
    fmt_step.dependOn(&fmt_check.step);

    // zig build purity: the layer purity rule. The amqp module must name no
    // vendor concept, because it ships as a general-purpose AMQP 1.0 client.
    const purity = b.addSystemCommand(&.{"sh"});
    purity.addFileArg(b.path("scripts/check-amqp-purity.sh"));
    purity.addDirectoryArg(b.path("src/amqp"));
    purity.has_side_effects = true;

    const purity_step = b.step("purity", "Make sure the amqp module stays vendor free");
    purity_step.dependOn(&purity.step);
}
