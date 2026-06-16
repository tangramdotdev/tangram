use ../../test.nu *

# Attempting to grant on a resource the actor cannot see does not reveal whether it exists.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let eve_user = tg user login eve | from json
let eve = current_token

# Alice owns a resource that eve has no access to.
tg --token $alice group create secret

# Granting on the invisible-but-existing resource fails the same way as a nonexistent one.
let existing = tg --token $eve grant $eve_user.id read secret | complete
failure $existing "an adversary should not be able to grant on a resource she cannot see"
assert ($existing.stderr | str contains "failed to find the resource") "the error must not reveal that the resource exists"

let missing = tg --token $eve grant $eve_user.id read does-not-exist | complete
failure $missing "granting on a nonexistent resource should fail"
assert ($missing.stderr | str contains "failed to find the resource") "a nonexistent resource should report not found"
