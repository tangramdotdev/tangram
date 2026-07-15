use ../../test.nu *

# Creating an organization returns its record, makes it retrievable, and grants the creator admin.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let organization = tg --token $alice.token organization create acme | from json
assert ($organization.id | str starts-with "org_") "create should return an organization id"
assert ($organization.name == "acme") "the organization name should match the specifier"
assert ($organization.specifier == "acme") "the organization specifier should match the input"

# The creator can get the organization.
let got = tg --token $alice.token organization get acme | from json
assert ($got.id == $organization.id) "the created organization should be retrievable"

# The creator has admin, so it can list the organization's grants.
tg --token $alice.token grants list --resource acme
