use ../../test.nu *

# Creating a grant returns the grant record and lists it on the resource.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice group create team

let grant = tg --token $alice grant $bob_user.id read team | from json
assert ($grant.permission == "read") "the grant should record the permission"
assert ($grant.principal == $bob_user.id) "the grant should record the principal"

let grants = tg --token $alice grants list --resource team | from json
assert ($grants | any {|g| $g.principal == $bob_user.id and $g.permission == "read" }) "the grant should be listed on the resource"
