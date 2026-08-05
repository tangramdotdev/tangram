use ../../test.nu *

# Runner management enforces owner administration and supports exact-owner listing and token lifecycle operations.

let root_token = "root-token"
let server = spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let organization = tg --token $alice.token organization create tangram | from json

let alice_runner = tg --token $alice.token runner create --owner $alice.user.id | from json
let organization_runner = tg --token $alice.token runner create --owner tangram | from json
assert ($alice_runner.runner.id | str starts-with "rnr_") "a runner should have a runner ID"
assert ($alice_runner.token.data.id | str starts-with "tok_") "the initial token should have a token ID"
assert equal $organization_runner.runner.owner $organization.id

failure (tg --token $bob.token runner create --owner tangram | complete) "a non-admin should not create a runner for the organization"
failure (tg --token $alice.token runner create | complete) "only root should create a global runner"

let runners = tg --token $alice.token runner list | from json
assert equal ($runners | length) 1
assert equal $runners.0.id $alice_runner.runner.id

let runners = tg --token $alice.token runner list --owner tangram | from json
assert equal ($runners | length) 1
assert equal $runners.0.id $organization_runner.runner.id

let global_runner = tg --token $root_token runner create | from json
let runners = tg --token $root_token runner list | from json
assert equal ($runners | length) 1
assert equal $runners.0.id $global_runner.runner.id
let runners = tg --token $root_token runner list --all | from json
assert equal ($runners | length) 3

let created_token = tg --token $alice.token runner token create $alice_runner.runner.id | from json
let tokens = tg --token $alice.token runner token list $alice_runner.runner.id | from json
assert ($created_token.data.id in $tokens.id) "the created runner token should be listed"
tg --token $alice.token runner token delete $alice_runner.runner.id $created_token.data.id
let tokens = tg --token $alice.token runner token list $alice_runner.runner.id | from json
assert not ($created_token.data.id in $tokens.id) "the deleted runner token should not be listed"

tg --token $alice.token runner delete $alice_runner.runner.id
let runners = tg --token $alice.token runner list | from json
assert ($runners | is-empty) "the deleted runner should not be listed"
