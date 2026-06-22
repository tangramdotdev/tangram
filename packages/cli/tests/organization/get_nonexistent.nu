use ../../test.nu *

# Getting an organization that does not exist fails.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token organization get ghost | complete
failure $output "getting a nonexistent organization should fail"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the organization

'
