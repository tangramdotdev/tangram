use ../lib/test.nu *

# Sandbox ownership does not confer process authority; an explicit parent grant does.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

tg --token $alice.token group create team
tg --token $alice.token grant $eve.user.id write team

let parent_path = artifact { tangram.ts: 'export default async () => { console.log(tg.process.env.TANGRAM_TOKEN); await tg.sleep(30); }' }
let parent = tg --token $alice.token run --network=true --detach --verbose --owner team $parent_path | from json
wait_until { (tg --token $alice.token log $parent.process | str trim | str length) > 0 } "the parent should log its token"
let token = tg --token $alice.token log $parent.process | str trim

let child_path = artifact { tangram.ts: 'export default async () => { await tg.sleep(30); }' }
let child_object = tg --token $alice.token checkin $child_path | str trim
tg --token $alice.token grant public object_subtree $child_object
let child = tg --token $token run --network=true --detach $child_object | str trim
tg --token $alice.token index

let signaled = tg --token $eve.token process signal $child --signal KILL | complete
failure $signaled "sandbox ownership must not permit signaling a descendant process"
failure (tg --token $eve.token process get $child | complete) "sandbox ownership must not permit reading a process"

tg --token $alice.token grant $eve.user.id process_parent $parent.process
success (tg --token $eve.token process signal $child --signal KILL | complete) "an explicit parent grant should permit signaling a descendant process"

wait_until { (tg --token $alice.token process status $child | from json | get 0) == "finished" } "the child should finish after Eve signals it"
tg --token $alice.token process signal $parent.process --signal KILL | complete | ignore
