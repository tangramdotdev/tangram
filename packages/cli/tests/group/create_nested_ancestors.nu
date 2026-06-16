use ../../test.nu *

# Creating a nested group auto-creates its ancestor groups.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice group create org/team/squad

# Each ancestor exists.
tg --token $alice group get org
tg --token $alice group get org/team
let leaf = tg --token $alice group get org/team/squad | from json
assert ($leaf.specifier == "org/team/squad") "the leaf group should have the full specifier"
