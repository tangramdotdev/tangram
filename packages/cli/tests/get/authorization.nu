use ../../test.nu *

# A reference token for an ancestor authorizes pattern selection, listing, and following.

let server = server spawn --tokens --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $server.url login --verbose --name alice | from json
let bob = tg --url $server.url login --verbose --name bob | from json
let path = artifact 'contents'
let artifact = tg --url $server.url --token $alice.token checkin $path
let parent = tg --url $server.url --token $alice.token group create private | from json
tg --url $server.url --token $alice.token group create private/1.0.0 | ignore
tg --url $server.url --token $alice.token tag private/1.0.0/latest $artifact
tg --url $server.url index

let token = $parent.tokens.local | url encode --all
let reference = $"private/^1?tokens[local]=($token)"
let version = tg --url $server.url --token $bob.token get $reference | from json
assert equal $version.specifier private/1.0.0

let children = tg --url $server.url --token $bob.token list $reference | from json
assert equal ($children | get specifier) [private/1.0.0/latest]

let reference = $"private/^1?follow=true&tokens[local]=($token)"
let output = tg --url $server.url --token $bob.token get $reference | complete
assert equal $output.exit_code 0
assert ($output.stdout | str starts-with 'tg.file(')
