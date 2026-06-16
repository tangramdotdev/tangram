use ../../test.nu *

# A private group is not visible to a user without read permission.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
tg user login eve
let eve = current_token

tg --token $alice group create private

let output = tg --token $eve group get private | complete
failure $output "a user without read permission should not be able to get a private group"
assert ($output.stderr | str contains "failed to find the group") "the private group should not be visible without read permission"
