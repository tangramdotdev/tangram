use ../../test.nu *

# A process can return a readable wrapper around an unreadable object without gaining access to the child.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice builds a private file.
let alice_path = artifact { tangram.ts: 'export default function () { return tg.file("topsecret"); }' }
let alice_process = tg --token $alice.token build --detach $alice_path | str trim
let file = (tg --token $alice.token wait $alice_process | from json).output.value | split row '?' | first

# Eve cannot read Alice's private file.
let denied = tg --token $eve.token get $file | complete
failure $denied "Eve should not read Alice's private file before the exploit."

# Eve builds a process whose output is a directory that nests Alice's private file, referenced by id.
let source = 'export default function () { return tg.directory({ "leak": tg.File.withId("FILE_ID") }); }' | str replace "FILE_ID" $file
let eve_path = artifact { tangram.ts: $source }
let eve_process = tg --token $eve.token build --detach $eve_path | str trim
let wait = tg --token $eve.token wait $eve_process | from json
assert equal $wait.exit 0 "a process may return a readable wrapper around an unreadable object."
let directory = $wait.output.value | split row '?' | first

# The relationship is stored on the process, and Eve can read the wrapper node.
let process = tg --token $eve.token get $eve_process | from json
assert equal ($process.output.value | split row '?' | first) $directory
let wrapper = tg --token $eve.token get $directory | complete
success $wrapper "Eve should be able to read the wrapper node."

# Reading the wrapper's full subtree fails only when Eve requests the unreadable child.
let nested = tg --token $eve.token get $directory --depth inf | complete
failure $nested "Eve must not read Alice's private file through the wrapper."

# Eve must not gain read access to Alice's private file by nesting it in her process output.
let leaked = tg --token $eve.token get $file | complete
failure $leaked "Eve must not read Alice's private file after nesting it in her process output."
snapshot --normalize-ids $leaked.stderr '
	error an error occurred
	-> failed to load the object

'
