use ../../test.nu *

# Creating a group returns its record, makes it retrievable, and grants the creator admin.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json

let group = tg --token $alice.token group create --verbose project | from json
assert ($group.data.id | str starts-with "grp_") "create should return a group id"
assert ($group.data.name == "project") "the group name should match the specifier"
assert ($group.data.specifier == "project") "the group specifier should match the input"
assert (($group | get --optional tokens.local) != null) "create should return a token"

# The creator can get the group.
let got = tg --token $alice.token group get project | from json
assert ($got.id == $group.data.id) "the created group should be retrievable"
assert (($got | get --optional tokens) == null) "get should not print tokens to stdout"

# The creator has admin, so it can list the group's grants.
tg --token $alice.token grants list --resource project
