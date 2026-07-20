use ../../test.nu *

# The organization members subcommand exposes the ls and rm aliases for list and remove.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id

# The ls alias lists members.
let members = tg --token $alice.token organization members ls acme | from json
assert ($bob.user.id in $members) "the ls alias should list the member"

# The rm alias removes a member.
tg --token $alice.token organization members rm acme $bob.user.id
let after = tg --token $alice.token organization members ls acme | from json
assert (not ($bob.user.id in $after)) "the rm alias should remove the member"
