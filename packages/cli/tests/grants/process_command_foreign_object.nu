use ../../test.nu *

# Referencing a private command by id does not leak it: building tg.Command.withId of a command the builder cannot read must not grant the builder read access to that command.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private process and keeps its command private.
let alice_path = artifact { tangram.ts: 'export default function () { return "alicesecret"; }' }
let alice_process = tg --token $alice.token build --detach $alice_path | str trim
tg --token $alice.token wait $alice_process
let command = (tg --token $alice.token get $alice_process | from json).command

# Eve cannot read Alice's command.
let denied = tg --token $eve.token get $command | complete
failure $denied "Eve should not read Alice's command before the exploit."

# Eve builds a process that references Alice's command by id.
let source = 'export default function () { return tg.build(tg.Command.withId("CMD_ID")); }' | str replace "CMD_ID" $command
let eve_path = artifact { tangram.ts: $source }
let eve_process = tg --token $eve.token build --detach $eve_path | str trim
tg --token $eve.token wait $eve_process

# Eve must not gain read access to Alice's command by referencing it.
let leaked = tg --token $eve.token get $command | complete
failure $leaked "Eve must not read Alice's command after referencing it as her process's command."
