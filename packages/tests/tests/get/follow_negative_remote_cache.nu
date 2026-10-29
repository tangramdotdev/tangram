use ../../lib/test.nu *

# A negative remote get with follow can be served from the principal-scoped remote cache.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let id = tg --url $remote.url put --no-tokens 'tg.file("test")' | referent node
tg --url $remote.url tag foo $id

# Cache the tag without following it.
let tag = tg --url $local.url tag get foo | from json
tg --url $local.url get $tag.id | ignore

# Follow the cached tag after deleting it on the remote to cache a negative response.
tg --url $remote.url tag delete foo | ignore
let first = tg --url $local.url get "foo?follow=true" | complete
failure $first

# The same negative response should be available after the remote stops.
let pid = open ($remote.directory | path join lock) | into int
kill --signal 2 $pid
wait_for_exit $pid "the remote should stop"

let second = tg --url $local.url get "foo?follow=true" | complete
failure $second
assert equal $second.stderr $first.stderr
