use ../../test.nu *

# Deleting a leaf group removes it.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

tg --token $alice.token group create leaf
tg --token $alice.token group delete leaf

let output = tg --token $alice.token group get leaf | complete
failure $output "the group should not exist after deletion"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the group

'
