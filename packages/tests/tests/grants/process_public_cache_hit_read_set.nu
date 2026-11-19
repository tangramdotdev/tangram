use ../../util/test.nu *

# A public spawn that gets a cache hit on another user's process makes public only what the cache hit proved for the caller, which is the process, its error objects, and its output objects, and not its children or its log.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

# Run a command as an anonymous client.
let config = mktemp
{} | to json | save -f $config
let anonymous = {|command| with-env { TANGRAM_CONFIG: $config } { do $command | complete } }

# Alice builds a process that logs, spawns a child, and returns a file.
let path = artifact {
	tangram.ts: 'export default async function () { console.log("alicelog"); await tg.build(child); return tg.file("aliceoutput"); }
export function child() { return tg.file("alicechild"); }'
}
let process = tg --token $alice.token build --no-tokens --detach $path | referent node
tg --token $alice.token wait $process | ignore
let child = tg --token $alice.token process children $process | from json | first | get process | referent node

# Alice gives Bob only the permissions that a cache hit requires.
for permission in [process_node process_node_error_objects process_node_output_objects] {
	tg --token $alice.token grant $bob.user.id $permission $process | ignore
}
tg --token $alice.token index

# Bob builds the same command publicly and gets a cache hit on Alice's process.
let build = tg --token $bob.token build --detach --verbose --public $path | from json
assert equal ($build.process | referent node) $process "Bob should get a cache hit on Alice's process"
assert ($build.cached? | default false) "Bob's build should be a cache hit"
tg --token $bob.token wait $build.process | ignore
tg --token $alice.token index

# An anonymous client can read the process and its output.
success (do $anonymous { tg get $process }) "an anonymous client should read the process"
let output = (do $anonymous { tg process output $process }).stdout | from json | get value
assert equal (do $anonymous { tg cat $output } | get stdout | str trim) 'aliceoutput' "an anonymous client should read the output"

# An anonymous client cannot read the children or the log.
failure (do $anonymous { tg get $child }) "an anonymous client must not read the child"
failure (do $anonymous { tg process log $process }) "an anonymous client must not read the log"

# Bob still cannot read the children or the log.
failure (tg --token $bob.token get $child | complete) "Bob must not read the child"
failure (tg --token $bob.token process log $process | complete) "Bob must not read the log"
