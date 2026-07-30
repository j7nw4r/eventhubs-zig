#!/bin/sh
#
# The layer purity check.
#
# The amqp module ships as a general-purpose AMQP 1.0 client. It must stay free
# of Event Hubs and Azure knowledge. The eventhubs module supplies every vendor
# value as a caller-supplied argument: link properties, desired capabilities,
# source filters, message annotations, and node addresses.
#
# This script fails the build when a forbidden string appears in the directories
# that it is given.
#
# Usage: check-amqp-purity.sh [directory ...]
# Default: src/amqp
#
# Limits of this check. It matches bytes, so it cannot see a string that the
# compiler assembles at build time, such as "com.micro" ++ "soft", and it cannot
# see text in an encoding other than UTF-8. Code review remains the backstop.
# The check must never report a clean tree that it did not read, so every error
# below exits 2 instead of passing.

set -eu

if [ "$#" -eq 0 ]; then
    set -- src/amqp
fi

# Fixed strings, matched without regard to case. None of them contains a space.
patterns="com.microsoft x-opt- eventhub servicebus azure"

found=0

for dir in "$@"; do
    if [ ! -d "$dir" ]; then
        printf 'purity: %s is not a directory\n' "$dir" >&2
        exit 2
    fi

    # Reject symlinks. `grep -r` does not follow a symlink that it meets during
    # recursion, so a link such as src/amqp/impl.zig -> ../eventhubs/vendor.zig
    # compiles into the amqp module and still passes this check.
    links=$(find "$dir" -type l) || {
        printf 'purity: cannot list %s\n' "$dir" >&2
        exit 2
    }
    if [ -n "$links" ]; then
        printf 'purity: found a symlink in %s\n' "$dir" >&2
        printf '%s\n' "$links" >&2
        printf 'purity: a symlink hides its target from this check. Remove it.\n' >&2
        found=1
    fi

    for pattern in $patterns; do
        # grep exits 0 when it matches, 1 when it does not, and 2 on an error.
        # An error must fail the build. grep exits 2 even when it also found a
        # match, so treating 2 as "no match" would hide a real violation.
        status=0
        matches=$(grep -rniF -- "$pattern" "$dir") || status=$?
        if [ "$status" -eq 0 ]; then
            printf 'purity: found the forbidden string "%s" in %s\n' "$pattern" "$dir" >&2
            printf '%s\n' "$matches" >&2
            found=1
        elif [ "$status" -ne 1 ]; then
            printf 'purity: grep exited %s on %s. The check did not read the tree.\n' \
                "$status" "$dir" >&2
            exit 2
        fi
    done
done

if [ "$found" -ne 0 ]; then
    printf '\n' >&2
    printf 'purity: the amqp module must name no vendor concept.\n' >&2
    printf 'purity: move the value into the eventhubs layer and pass it in.\n' >&2
    exit 1
fi

printf 'purity: clean (%s)\n' "$*"
