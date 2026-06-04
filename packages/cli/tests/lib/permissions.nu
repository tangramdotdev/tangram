# Shared helpers for the permissions tests.

# Return the token stored in the active configuration.
export def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

# Assert that the command failed because the request was unauthorized.
export def assert_unauthorized [output: record, message: string] {
	failure $output $message
	assert ($output.stderr | str contains "unauthorized") "The error should mention that the request is unauthorized."
}

# Write a configuration with no token and return its path, for anonymous requests.
export def anonymous_config [] {
	let path = mktemp
	{} | to json | save --force $path
	$path
}

# Write a configuration with an invalid token and return its path.
export def invalid_token_config [] {
	let path = mktemp
	{ token: invalid } | to json | save --force $path
	$path
}

# Log in alice, bob, and carol, and return their tokens and user ids.
export def login_users [] {
	let alice_user = tg user login alice | from json
	let alice = current_token
	let bob_user = tg user login bob | from json
	let bob = current_token
	let carol_user = tg user login carol | from json
	let carol = current_token
	{
		alice: $alice,
		bob: $bob,
		carol: $carol,
		alice_id: $alice_user.id,
		bob_id: $bob_user.id,
		carol_id: $carol_user.id,
	}
}
