use ../../test.nu *

# Creating a group returns its record, makes it retrievable, and grants the creator admin.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

let group = tg --token $alice.token group create project | from json
assert ($group.id | str starts-with "grp_") "create should return a group id"
assert ($group.name == "project") "the group name should match the specifier"
assert ($group.specifier == "project") "the group specifier should match the input"

# The creator can get the group.
let got = tg --token $alice.token group get project | from json
assert ($got.id == $group.id) "the created group should be retrievable"

# The creator has admin, so it can list the group's grants.
tg --token $alice.token grants list --resource project
