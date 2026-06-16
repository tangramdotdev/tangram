use ../../test.nu *

# An adversary cannot escalate by granting herself a permission on a resource she does not administer.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let eve_user = tg user login eve | from json
let eve = current_token

# Alice owns a private resource that eve has no access to.
tg --token $alice group create secret

# Eve attempts to hand herself admin on the resource.
let output = tg --token $eve grant $eve_user.id admin secret | complete
failure $output "an adversary should not be able to grant herself a permission"

# The failed attempt confers nothing: eve still cannot see the resource.
let check = tg --token $eve group get secret | complete
failure $check "the failed self-grant should not have conferred any access"
assert ($check.stderr | str contains "failed to find the group") "the resource should remain invisible to the adversary"
