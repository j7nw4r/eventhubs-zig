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
    for pattern in $patterns; do
        if matches=$(grep -rniF -- "$pattern" "$dir"); then
            printf 'purity: found the forbidden string "%s" in %s\n' "$pattern" "$dir" >&2
            printf '%s\n' "$matches" >&2
            found=1
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
