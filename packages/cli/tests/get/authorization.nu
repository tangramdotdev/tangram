use ../lib/test.nu *

# A reference token for an ancestor authorizes pattern selection, listing, and following.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $local.url login --verbose --name alice | from json
let bob = tg --url $local.url login --verbose --name bob | from json
let path = artifact 'contents'
let artifact = tg --url $local.url --token $alice.token checkin $path
let parent = tg --url $local.url --token $alice.token group create --verbose private | from json
tg --url $local.url --token $alice.token group create private/1.0.0 | ignore
tg --url $local.url --token $alice.token tag private/1.0.0/latest $artifact
tg --url $local.url index

let token = $parent.tokens.local.0 | url encode --all
let reference = $"private/^1?tokens[local][0]=($token)"
let version = tg --url $local.url --token $bob.token get $reference | from json
assert equal $version.specifier private/1.0.0

let children = tg --url $local.url --token $bob.token list $reference | from json
assert equal ($children | get specifier) [private/1.0.0/latest]

let reference = $"private/^1?follow=true&tokens[local][0]=($token)"
let output = tg --url $local.url --token $bob.token get $reference | complete
assert equal $output.exit_code 0
assert ($output.stdout | str starts-with 'tg.file(')
