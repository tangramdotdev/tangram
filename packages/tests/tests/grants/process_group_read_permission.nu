use ../../util/test.nu *

# Read on a group lets a principal read the group's processes but not control them, and write on the group lets a principal control them.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let dave = tg login --verbose --name dave | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id
tg --token $alice.token grant $dave.user.id read team

# Alice runs a long-running, non-cacheable process owned by the team.
let path = artifact { tangram.ts: 'export default async function () { await tg.sleep(30); return "done"; }' }
let process = tg --token $alice.token run --wait --no-tokens --network=true --detach --group team $path | referent node

# Dave can read the process through read on the team but cannot signal it.
success (tg --token $dave.token get $process | complete) "read on the group should allow reading its process"
failure (tg --token $dave.token process signal $process --signal KILL | complete) "read on the group must not allow signaling its process"
let status = tg --token $alice.token process status $process | from json | get 0
assert ($status == "started") "Dave's signal must not have terminated the process"

# Bob, a member with write on the team, can signal the process.
success (tg --token $bob.token process signal $process --signal KILL | complete) "write on the group should allow signaling its process"
