use ../../test.nu *

# Logging in twice as the same user returns the same user, and each login issues a fresh token.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

let first = tg user login alice | from json
let token1 = current_token
let second = tg user login alice | from json
let token2 = current_token

assert ($first.id == $second.id) "logging in again should return the same user"
assert ($token1 != $token2) "each login should issue a new token"
