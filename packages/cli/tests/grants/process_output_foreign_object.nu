use ../../test.nu *

# A process cannot leak a private object by naming it as its output: building tg.File.withId of a file the builder cannot read must not grant the builder read access to that file.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private file.
let alice_path = artifact { tangram.ts: 'export default function () { return tg.file("topsecret"); }' }
let alice_process = tg --token $alice.token build --detach $alice_path | str trim
let file = (tg --token $alice.token wait $alice_process | from json).output.value.id

# Eve cannot read Alice's private file.
let denied = tg --token $eve.token get $file | complete
failure $denied "Eve should not read Alice's private file before the exploit."

# Eve builds a process whose output is Alice's private file, referenced by id.
let source = 'export default function () { return tg.File.withId("FILE_ID"); }' | str replace "FILE_ID" $file
let eve_path = artifact { tangram.ts: $source }
let eve_process = tg --token $eve.token build --detach $eve_path | str trim
tg --token $eve.token wait $eve_process

# Eve must not gain read access to Alice's private file by naming it as her process output.
let leaked = tg --token $eve.token get $file | complete
failure $leaked "Eve must not read Alice's private file after naming it as her process output."
snapshot ($leaked.stderr | redact | normalize_ids) '
	error an error occurred
	-> failed to load the object

'
