use ../../util/test.nu *

# A subject keeps a permission until every grantor who granted it has revoked or deleted their grant.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let carol = tg login --verbose --name carol | from json

tg --token $alice.token group create team
tg --token $alice.token grant $carol.user.id admin team

tg --token $alice.token grant $bob.user.id read team
let grant = tg --token $carol.token grant $bob.user.id read team | from json
tg index --database
tg --token $bob.token group get team

tg --token $alice.token revoke $bob.user.id read team
tg index --database
tg --token $bob.token group get team

tg --token $carol.token grants delete $grant.id
tg index --database
let output = tg --token $bob.token group get team | complete
failure $output "Bob should lose access after both grantors remove their grants"
