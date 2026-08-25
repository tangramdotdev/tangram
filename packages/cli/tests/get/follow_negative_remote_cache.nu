use ../../test.nu *

# A negative remote get with follow can be served from the principal-scoped remote cache.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let id = tg --url $remote.url put 'tg.file("test")' | str trim
tg --url $remote.url tag foo $id

# Cache the tag without following it.
let tag = tg --url $local.url tag get foo | from json
tg --url $local.url get $tag.id | ignore

# Follow the cached tag after deleting it on the remote to cache a negative response.
tg --url $remote.url tag delete foo | ignore
let first = tg --url $local.url get "foo?follow=true" | complete
failure $first

let response = (
	open ($local.directory | path join database)
	| query db `select response from remote_cache where request like '%"follow":true%'`
	| get response
	| first
	| from json
)
assert equal $response.kind get
assert equal $response.output null

# The same negative response should be available after the remote stops.
let pid = open ($remote.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

let second = tg --url $local.url get "foo?follow=true" | complete
failure $second
assert equal $second.stderr $first.stderr
