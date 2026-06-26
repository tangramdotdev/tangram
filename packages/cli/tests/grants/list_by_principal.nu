use ../../test.nu *

# A user can list their own grants but not another user's grants.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Eve can list her own grants through inherent self-admin.
let own = tg --token $eve.token grants list --principal $eve.user.id | from json
assert (($own | length) == 0) "a user should be able to list their own grants"

# Eve cannot list Alice's grants without admin on Alice.
let output = tg --token $eve.token grants list --principal $alice.user.id | complete
failure $output "a user should not be able to list another user's grants"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the principal

'
