use ../../test.nu *

# Attempting to grant on a resource the actor cannot see does not reveal whether it exists.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice owns a resource that eve has no access to.
tg --token $alice.token group create secret

# Granting on the invisible-but-existing resource fails the same way as a nonexistent one.
let existing = tg --token $eve.token grant $eve.user.id read secret | complete
failure $existing "an adversary should not be able to grant on a resource she cannot see"
snapshot ($existing.stderr | redact) '
	error an error occurred
	-> failed to create the grant
	   principal = <user>
	   resource = secret
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to find the resource

'

let missing = tg --token $eve.token grant $eve.user.id read does-not-exist | complete
failure $missing "granting on a nonexistent resource should fail"
snapshot ($missing.stderr | redact) '
	error an error occurred
	-> failed to create the grant
	   principal = <user>
	   resource = does-not-exist
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to find the resource

'
