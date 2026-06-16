use ../../test.nu *

# Listing a resource's grants returns every grant on it, including the creator's admin grant.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

let alice_user = tg user login alice | from json
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice group create team
tg --token $alice grant $bob_user.id read team

let grants = tg --token $alice grants list --resource team | from json
assert ($grants | any {|g| $g.principal == $alice_user.id and $g.permission == "admin" }) "the creator's admin grant should be listed"
assert ($grants | any {|g| $g.principal == $bob_user.id and $g.permission == "read" }) "bob's read grant should be listed"
