use ../../test.nu *

# A private group is not visible to a user without read permission.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

tg --token $alice.token group create private

let output = tg --token $eve.token group get private | complete
failure $output "a user without read permission should not be able to get a private group"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the group

'
