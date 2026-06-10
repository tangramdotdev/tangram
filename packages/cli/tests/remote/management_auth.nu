use ../../test.nu *

# Remote management requires authentication and each authenticated user manages their own isolated set of remotes.

let root_remote = spawn --name root-remote
let alice_server = spawn --name alice-remote
let bob_server = spawn --name bob-remote
let auth_enabled = spawn --config {
	authentication: true,
	remotes: { default: { url: $root_remote.url } },
} --name auth-enabled

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

let output = tg remote put default $alice_server.url | complete
failure $output "An unauthenticated request should not be able to manage remotes."
assert ($output.stderr | str contains "unauthenticated") "The error should mention that the request is unauthenticated."

tg user login alice
let alice = current_token
tg user login bob
let bob = current_token

let alice_remotes = tg --token $alice remote list | from json
assert equal $alice_remotes []

tg --token $alice remote put default $alice_server.url
tg --token $bob remote put default $bob_server.url

let alice_remote = tg --token $alice remote get default | from json
assert equal $alice_remote.url $alice_server.url

let bob_remote = tg --token $bob remote get default | from json
assert equal $bob_remote.url $bob_server.url

tg --token $alice remote delete default
let alice_remotes = tg --token $alice remote list | from json
assert equal $alice_remotes []

let bob_remote = tg --token $bob remote get default | from json
assert equal $bob_remote.url $bob_server.url

let auth_disabled = spawn --name auth-disabled

tg remote put default $root_remote.url
tg remote delete default
