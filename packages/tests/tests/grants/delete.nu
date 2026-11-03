use ../../util/test.nu *

# Revoking a grant's permissions removes it, and deleting a grant by its ID removes it.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

tg --token $alice.token group create team

# Revoking every permission of a grant removes it.
tg --token $alice.token grant $bob.user.id read team
tg --token $alice.token revoke $bob.user.id read team
let grants = tg --token $alice.token grants list --resource team --expand | from json | each {|item| $item.data }
assert (not ($grants | any {|g| $g.subject == $bob.user.id and $g.permissions == { kind: group, value: [read] } })) "the grant should be gone after revoke"

# Deleting a grant by its listed referent removes it.
let grant = tg --token $alice.token grant $bob.user.id write team | from json
let listed = tg --token $alice.token grants list --resource team | from json | where node == $grant.id | first
tg --token $alice.token grants delete $"($listed.node)?location=($listed.options.location)"
let grants = tg --token $alice.token grants list --resource team --expand | from json | each {|item| $item.data }
assert (not ($grants | any {|g| $g.subject == $bob.user.id and $g.permissions == { kind: group, value: [write] } })) "deleting the grant should remove it"

# Deleting a grant by its ID removes the subject's access.
let grant = tg --token $alice.token grant $bob.user.id read team | from json
tg --token $bob.token group get team | ignore
tg --token $alice.token grants delete $grant.id
tg index
failure (tg --token $bob.token group get team | complete) "deleting the grant should remove the subject's access"
