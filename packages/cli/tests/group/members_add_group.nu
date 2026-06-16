use ../../test.nu *

# A group can be added as a member of another group.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice group create team
let other = tg --token $alice group create other | from json
tg --token $alice group members add team $other.id

let members = tg --token $alice group members list team | from json
assert ($other.id in $members) "the added group should be listed as a member"
