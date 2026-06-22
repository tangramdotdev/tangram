use ../../test.nu *

# Deleting a group that does not exist fails.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token group delete ghost | complete
failure $output "deleting a nonexistent group should fail"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to delete the group
	   group = ghost
	-> failed to find the group

'
