use ../../util/test.nu *

# Runner management enforces owner administration and supports exact-owner listing and token lifecycle operations.

let root_token = "root-token"
let local = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let organization = tg --token $alice.token organization create tangram | from json

let alice_runner = tg --token $alice.token runner create --owner $alice.user.id | from json
let organization_runner = tg --token $alice.token runner create --owner tangram | from json
assert ($alice_runner.data.id | str starts-with "rnr_") "a runner should have a runner ID"
assert ($alice_runner.token.id | str starts-with "tok_") "the initial token should have a token ID"
assert equal $organization_runner.data.owner $organization.id

failure (tg --token $bob.token runner create --owner tangram | complete) "a non-admin should not create a runner for the organization"
failure (tg --token $alice.token runner create | complete) "only root should create a global runner"

let runners = tg --token $alice.token runner list | from json
assert equal ($runners | length) 1
assert equal $runners.0.node $alice_runner.data.id

let runners = tg --token $alice.token runner list --owner tangram | from json
assert equal ($runners | length) 1
assert equal $runners.0.node $organization_runner.data.id

let global_runner = tg --token $root_token runner create | from json
let runners = tg --token $root_token runner list | from json
assert equal ($runners | length) 1
assert equal $runners.0.node $global_runner.data.id
let runners = tg --token $root_token runner list --all-owners | from json
assert equal ($runners | length) 3

# Getting a runner returns its data to an admin of its owner only.
let got = tg --token $alice.token runner get $alice_runner.data.id | from json
assert equal $got.id $alice_runner.data.id
assert equal $got.owner $alice.user.id
let output = tg --token $bob.token runner get $alice_runner.data.id | complete
failure $output "a non-admin should not get the runner"
assert ($output.stderr | str contains "failed to find the runner") "a non-admin should not learn that the runner exists"

let created_token = tg --token $alice.token runner token create $alice_runner.data.id | from json

# Getting a runner token returns its ID without its secret.
let got = tg --token $alice.token runner token get $alice_runner.data.id $created_token.id | from json
assert equal $got.id $created_token.id
assert not ("token" in ($got | columns)) "getting a runner token should not return its secret"
failure (tg --token $bob.token runner token get $alice_runner.data.id $created_token.id | complete) "a non-admin should not get the runner token"
failure (tg --token $alice.token runner token get $organization_runner.data.id $created_token.id | complete) "a token should not be found under another runner"
let tokens = tg --token $alice.token runner token list $alice_runner.data.id | from json
assert ($created_token.id in $tokens.node) "the created runner token should be listed"
tg --token $alice.token runner token delete $alice_runner.data.id $created_token.id
let tokens = tg --token $alice.token runner token list $alice_runner.data.id | from json
assert not ($created_token.id in $tokens.node) "the deleted runner token should not be listed"

tg --token $alice.token runner delete $alice_runner.data.id
let runners = tg --token $alice.token runner list | from json
assert ($runners | is-empty) "the deleted runner should not be listed"
