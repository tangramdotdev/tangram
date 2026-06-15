use ../../test.nu *

# The organization members subcommand exposes the ls and rm aliases for list and remove.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice organization create acme
tg --token $alice organization members add acme $bob_user.id

# The ls alias lists members.
let members = tg --token $alice organization members ls acme | from json
assert ($bob_user.id in $members) "the ls alias should list the member"

# The rm alias removes a member.
tg --token $alice organization members rm acme $bob_user.id
let after = tg --token $alice organization members ls acme | from json
assert (not ($bob_user.id in $after)) "the rm alias should remove the member"
