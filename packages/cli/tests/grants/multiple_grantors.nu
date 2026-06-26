use ../../test.nu *

# Separate grantors can grant overlapping permissions to the same principal on the same resource.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json

tg --token $alice.token group create team
tg --token $alice.token grant $carol.user.id admin team

tg --token $alice.token grant $bob.user.id read team
tg --token $carol.token grant $bob.user.id write team

let grants = tg --token $alice.token grants list --resource team | from json
assert ($grants | any {|g| $g.principal == $bob.user.id and $g.permissions == "group_read" }) "Alice's grant should be listed"
assert ($grants | any {|g| $g.principal == $bob.user.id and $g.permissions == "group_write" }) "Carol's grant should be listed"

tg --token $bob.token group get team
tg --token $alice.token revoke $bob.user.id read team
tg --token $bob.token group get team

tg --token $carol.token revoke $bob.user.id write team
let output = tg --token $bob.token group get team | complete
failure $output "Bob should lose access after both grantors revoke their grants"
