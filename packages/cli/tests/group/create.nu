use ../../test.nu *

# Creating a group returns its record, makes it retrievable, and grants the creator admin.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

let group = tg --token $alice group create project | from json
assert ($group.id | str starts-with "grp_") "create should return a group id"
assert ($group.name == "project") "the group name should match the specifier"
assert ($group.specifier == "project") "the group specifier should match the input"

# The creator can get the group.
let got = tg --token $alice group get project | from json
assert ($got.id == $group.id) "the created group should be retrievable"

# The creator has admin, so it can list the group's grants.
tg --token $alice grants list --resource project
