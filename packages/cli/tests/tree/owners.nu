use ../../test.nu *

# Users and organizations render their direct child groups and tags.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let id = tg --token $alice.token checkin (artifact "hello")

tg --token $alice.token group create alice/project
tg --token $alice.token tag alice/release $id

let output = tg --token $alice.token tree alice --depth 1
snapshot $output '
	alice
	├╴alice/project
	└╴alice/release
'

tg --token $alice.token organization create acme
tg --token $alice.token group create acme/project
tg --token $alice.token tag acme/release $id

let output = tg --token $alice.token tree acme --depth 1
snapshot $output '
	acme
	├╴acme/project
	└╴acme/release
'
