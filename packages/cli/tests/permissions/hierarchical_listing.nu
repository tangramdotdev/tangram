use ../../test.nu *
use ../lib/permissions.nu *

# Listing a tag hierarchy exposes only the path segments leading to a readable descendant and hides private siblings, and revoking the descendant's read grant hides the path again.

let server = spawn --config { authentication: true }
tg user login alice
let alice = current_token
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice tag apple/secretproject/0 $id
tg --token $alice tag apple/macos/code/26 $id
tg --token $alice tag apple/macos/code/27 $id
tg --token $alice tag apple/macos/builds/26 $id
tg --token $alice tag apple/macos/builds/27 $id
tg --token $alice grant public read apple/macos/builds/26

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg ls apple | complete }
success $output "An anonymous user should be able to list a parent with a readable descendant."
assert ($output.stdout | str contains "apple/macos") "The readable descendant should expose the macos path segment."
assert (not ($output.stdout | str contains "apple/secretproject")) "A private sibling path segment should not be visible."

let output = with-env { TANGRAM_CONFIG: $config } { tg ls apple/macos | complete }
success $output "An anonymous user should be able to list an intermediate path with a readable descendant."
assert ($output.stdout | str contains "apple/macos/builds") "The readable descendant should expose the builds path segment."
assert (not ($output.stdout | str contains "apple/macos/code")) "A private sibling path segment should not be visible."

let output = with-env { TANGRAM_CONFIG: $config } { tg ls apple/macos/builds | complete }
success $output "An anonymous user should be able to list the readable release parent."
assert ($output.stdout | str contains "apple/macos/builds/26") "The released build should be visible."
assert (not ($output.stdout | str contains "apple/macos/builds/27")) "The unreleased build should not be visible."

tg --token $alice grants delete public read apple/macos/builds/26
let output = with-env { TANGRAM_CONFIG: $config } { tg ls apple | complete }
success $output "An anonymous user should be able to list a private parent after the grant is revoked."
assert (not ($output.stdout | str contains "apple/macos")) "The group specifier should stop being visible without readable descendants."
