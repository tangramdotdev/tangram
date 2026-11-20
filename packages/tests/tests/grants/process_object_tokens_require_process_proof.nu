use ../../util/test.nu *

# Tokens for a process's objects are issued only for objects that the process itself has proven, so naming an unreadable object in a process does not leak it through the process's tokens.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json
let reader = tg login --verbose --name reader | from json
let stranger = tg login --verbose --name stranger | from json

# Read a referent with the stranger, who has no grants and relies only on the referent's tokens.
def read [token: string, referent: string] {
	tg --token $token get --depth inf $referent | complete
}

# Alice creates a private file, a private log blob, a private command, and a private error.
let file = tg --token $alice.token put --no-tokens 'tg.file("alicesecret")' | referent node
let log = tg --token $alice.token put --no-tokens 'tg.blob("alicelog")' | referent node
let command_path = artifact { tangram.ts: 'export const secret = () => "alicesecret"; export default () => tg.command(secret);' }
let command = tg --token $alice.token build --no-tokens $command_path | from json
let error_path = artifact { tangram.ts: 'export default function () { throw new Error("alicesecret") }' }
let error_process = tg --token $alice.token build --no-tokens --detach $error_path | referent node
tg --token $alice.token wait $error_process | ignore
let error = (tg --token $alice.token get $error_process | from json).error
tg --token $alice.token index

# Eve cannot read any of them.
for id in [$file $command $error] {
	failure (tg --token $eve.token get $id | complete) $"Eve should not read Alice's ($id) before the exploit."
}
failure (tg --token $eve.token cat $log | complete) "Eve should not read Alice's log before the exploit."

# Eve builds a process whose output names Alice's file by ID, and waits for the index to record everything that the process proved.
let source = 'export default function () { return tg.File.withId("FILE_ID"); }' | str replace "FILE_ID" $file
let leak_path = artifact { tangram.ts: $source }
let leak_process = tg --token $eve.token build --detach $leak_path | str trim | referent node
tg --token $eve.token wait $leak_process | ignore
tg --token $eve.token index

# The output of the process get must not carry a token that reads Alice's file.
let got = tg --token $eve.token process get --metadata $leak_process | from json
failure (read $stranger.token $got.output.node.value) "the process get output must not carry a token for an object the process did not prove"
failure (tg --token $eve.token get $got.output.node.value | complete) "Eve must not read the output through the process get output"

# The output of a cache hit must not carry a token that reads Alice's file.
let cached = tg --token $eve.token build $leak_path | str trim
failure (read $stranger.token $cached) "the cache hit output must not carry a token for an object the process did not prove"
failure (tg --token $eve.token get $cached | complete) "Eve must not read the output of a cache hit"

# The output of a child process, reached through its parent, must not carry a token that reads Alice's file, once the index has recorded everything that the processes proved.
let source = 'export default async function () { return await tg.build(leak); }; export function leak() { return tg.File.withId("FILE_ID"); }' | str replace "FILE_ID" $file
let parent_path = artifact { tangram.ts: $source }
let parent = tg --token $eve.token build --detach $parent_path | str trim | referent node
tg --token $eve.token wait $parent | ignore
tg --token $eve.token index
let parent_data = tg --token $eve.token process get --metadata $parent | from json
failure (read $stranger.token $parent_data.output.node.value) "the parent output must not carry a token for an object the parent did not prove"
let child = $parent_data.children.0.process
let child_data = tg --token $stranger.token process get --metadata $child | from json
failure (read $stranger.token $child_data.output.node.value) "the child output must not carry a token for an object the child did not prove"

# Eve puts a finished process that names herself as its creator, and Alice's objects as its command, error, log, and output.
let put_process = 'pcs_00081061050r3gg28a1c60t3gf20'
let data = {
	children: []
	command: $command
	created_at: 0
	creator: $eve.user.id
	error: $error
	exit: 1
	finished_at: 1
	host: 'x86_64-linux'
	log: $log
	output: { node: { kind: object, value: $file } }
	owner: $eve.user.id
	status: finished
}
tg --token $eve.token process put $put_process ($data | to json) | ignore
tg --token $eve.token index
let put = tg --token $eve.token process get --metadata $put_process | from json
failure (read $stranger.token $put.output.node.value) "the put process output must not carry a token for Alice's file"
failure (tg --token $stranger.token cat $put.log | complete) "the put process log must not carry a token for Alice's log"
failure (read $stranger.token $put.error) "the put process error must not carry a token for Alice's error"
failure (read $stranger.token $put.command) "the put process command must not carry a token for Alice's command"

# Eve still cannot read any of Alice's objects.
for id in [$file $command $error] {
	failure (tg --token $eve.token get $id | complete) $"Eve must not read Alice's ($id) after the exploit."
}
failure (tg --token $eve.token cat $log | complete) "Eve must not read Alice's log after the exploit."

# A process's output that the process created itself carries a token that reads it, through a process get and a cache hit, once the index has recorded the process's permission on the object.
let own_path = artifact { tangram.ts: 'export default function () { return tg.directory({ "file": tg.file("evefile") }); }' }
let own_process = tg --token $eve.token build --detach $own_path | str trim | referent node
tg --token $eve.token wait $own_process | ignore
tg --token $eve.token index
let got = tg --token $eve.token process get --metadata $own_process | from json
success (read $stranger.token $got.output.node.value) "the process get output must carry a token for an object the process created"
let cached = tg --token $eve.token build $own_path | str trim
success (read $stranger.token $cached) "the cache hit output must carry a token for an object the process created"

# A process's output that was passed into the process carries a token that reads it, once the index has recorded the process's permission on the object.
let input = tg --token $eve.token put --no-tokens 'tg.file("eveinput")' | referent node
let identity_path = artifact { tangram.ts: 'export default function (file: tg.File) { return file; }' }
let identity_process = tg --token $eve.token build --detach $identity_path --arg-value $input | str trim | referent node
tg --token $eve.token wait $identity_process | ignore
tg --token $eve.token index
let got = tg --token $eve.token process get --metadata $identity_process | from json
assert equal ($got.output.node.value | referent node) $input
success (read $stranger.token $got.output.node.value) "the process get output must carry a token for an object passed into the process"

# Another user granted the process's output can read the output through the process.
for permission in [process_node process_node_output_objects] {
	tg --token $eve.token grant $reader.user.id $permission $own_process | ignore
}
let got = tg --token $reader.token process get $own_process | from json
success (tg --token $reader.token get --depth inf $got.output.node.value | complete) "a user granted the process output must read it"
