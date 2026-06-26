use ../../test.nu *

# A write grant on the owner of an ancestor process's sandbox confers process_write on descendants.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

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
success $signaled "write on the sandbox owner should permit signaling a descendant process."

wait_until { (tg --token $alice.token process status $child | from json | get 0) == "finished" } "the child should finish after Eve signals it"
tg --token $alice.token process signal $parent.process --signal KILL | complete | ignore
