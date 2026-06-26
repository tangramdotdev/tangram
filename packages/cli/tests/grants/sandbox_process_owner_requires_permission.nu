use ../../test.nu *

# Building a process with a group owner requires write on that group, so read access alone is not enough.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let eve = tg login --verbose eve | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id
tg --token $alice.token grant $eve.user.id read team

let path = artifact { tangram.ts: 'export default () => tg.file("process-owner-assignment")' }

# Bob, a member, may build a process owned by the team.
success (tg --token $bob.token build --detach --owner team $path | complete) "a member should build a process owned by their group"

# Eve can resolve the team but only has read, so she must not assign it as the owner.
failure (tg --token $eve.token build --detach --owner team $path | complete) "read on the owning group must not allow building a process owned by it"
