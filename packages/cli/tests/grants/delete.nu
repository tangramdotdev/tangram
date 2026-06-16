use ../../test.nu *

# Deleting a grant removes it, and revoke is an alias for delete.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice group create team

# grants delete removes a grant.
tg --token $alice grant $bob_user.id read team
tg --token $alice grants delete $bob_user.id read team
let grants = tg --token $alice grants list --resource team | from json
assert (not ($grants | any {|g| $g.principal == $bob_user.id and $g.permission == "read" })) "the grant should be gone after delete"

# revoke is an alias for grants delete.
tg --token $alice grant $bob_user.id write team
tg --token $alice revoke $bob_user.id write team
let grants = tg --token $alice grants list --resource team | from json
assert (not ($grants | any {|g| $g.principal == $bob_user.id and $g.permission == "write" })) "revoke should remove the grant"
