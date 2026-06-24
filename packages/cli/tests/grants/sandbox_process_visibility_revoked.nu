use ../../test.nu *

# Revoking group membership hides a group-owned process from the former member, while the builder keeps access through their own process grant.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id
tg --token $alice.token group members add team $carol.user.id

# Bob builds a process whose sandbox is owned by the team; the process record's visibility follows the team-owned sandbox.
let path = artifact { tangram.ts: 'export default function () { return tg.file("revoked-visibility-team"); }' }
let process = tg --token $bob.token build --detach --owner team $path | str trim
tg --token $bob.token wait $process | complete | ignore

# Carol, a member, can read the team-owned process.
success (tg --token $carol.token get $process | complete) "a group member should read the group-owned process"

# After Carol's membership is revoked, she can no longer read it.
tg --token $alice.token group members remove team $carol.user.id
failure (tg --token $carol.token get $process | complete) "a former member must not read the group-owned process after revocation"

# Bob, the builder, retains access through his per-principal process grant.
success (tg --token $bob.token get $process | complete) "the builder should retain access after a member is revoked"
