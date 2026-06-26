use ../../test.nu *

# Getting a group that does not exist fails.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token group get ghost | complete
failure $output "getting a nonexistent group should fail"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the group

'
