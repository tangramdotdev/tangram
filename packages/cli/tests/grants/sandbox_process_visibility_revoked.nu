use ../lib/test.nu *

# Revoking group membership removes a group process grant while the builder keeps independent access.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let carol = tg login --verbose --name carol | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id
tg --token $alice.token group members add team $carol.user.id

# Bob builds a process and explicitly grants the team access.
let path = artifact { tangram.ts: 'export default function () { return tg.file("revoked-visibility-team"); }' }
let process = tg --token $bob.token build --detach --group team $path | str trim | split row '?' | first
tg --token $bob.token wait $process | complete | ignore

tg --token $bob.token grant team process_subtree $process

# Carol can read through the explicit group process grant.
success (tg --token $carol.token get $process | complete) "a group member should read the group-owned process"

# After Carol's membership is revoked, she can no longer read it.
tg --token $alice.token group members remove team $carol.user.id
tg --token $alice.token index
failure (tg --token $carol.token get $process | complete) "a former member must not read the group-owned process after revocation"

# Bob, the builder, retains access through his per-subject process grant.
success (tg --token $bob.token get $process | complete) "the builder should retain access after a member is revoked"
