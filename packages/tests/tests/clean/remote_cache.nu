use ../../util/test.nu *

# Cleaning reports cleared remote responses while preserving local tags.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let blob = tg --url $remote.url put --no-tokens 'tg.blob("remote")' | referent node
tg --url $remote.url tag cached $blob

# Cache a remote response by resolving its tag and retain a local tag.
tg --url $local.url get 'cached?follow=true' | ignore
let retained = tg --url $local.url put --no-tokens 'tg.blob("local")' | referent node
tg --url $local.url tag retained $retained

# Report the cleared responses and confirm the cached lookup is unavailable.
let output = tg --url $local.url clean | complete
success $output
let summary = $output.stdout | from json
assert ($summary.cached_responses > 0)
failure (tg --url $local.url get --remote --cached --no-ttl 'cached?follow=true' | complete)
assert equal (tg --url $local.url read retained) 'local'

# Cleaning again reports no cached responses.
let output = tg --url $local.url clean | complete
success $output
let summary = $output.stdout | from json
assert equal $summary.cached_responses 0

# Resolve the remote tag again after its cached response was cleared.
assert equal (tg --url $local.url get --no-tokens 'cached?follow=true' | referent node) $blob
