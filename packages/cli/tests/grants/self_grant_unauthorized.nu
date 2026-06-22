use ../../test.nu *

# An adversary cannot escalate by granting herself a permission on a resource she does not administer.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice owns a private resource that eve has no access to.
tg --token $alice.token group create secret

# Eve attempts to hand herself admin on the resource.
let output = tg --token $eve.token grant $eve.user.id admin secret | complete
failure $output "an adversary should not be able to grant herself a permission"

# The failed attempt confers nothing: eve still cannot see the resource.
let check = tg --token $eve.token group get secret | complete
failure $check "the failed self-grant should not have conferred any access"
snapshot ($check.stderr | redact) '
	error an error occurred
	-> failed to find the group

'
