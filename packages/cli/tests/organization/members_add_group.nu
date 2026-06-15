use ../../test.nu *

# A group can be added as a member of an organization.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice organization create acme
let team = tg --token $alice group create team | from json
tg --token $alice organization members add acme $team.id

let members = tg --token $alice organization members list acme | from json
assert ($team.id in $members) "the added group should be listed as a member"
