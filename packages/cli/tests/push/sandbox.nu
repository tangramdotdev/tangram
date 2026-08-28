use ../../test.nu *

# A destroyed sandbox can be pushed without its processes, then pushed again with its processes.

let remote = server spawn --cloud --name remote
let local = server spawn --name local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("output")
		}
	',
}
let process = tg build --detach $path | str trim
tg wait $process
let sandbox = tg get $process | from json | get sandbox
tg wait $sandbox
tg index

tg push $sandbox
let remote_sandbox = tg --url $remote.url sandbox get $sandbox | from json
assert equal $remote_sandbox.id $sandbox
assert equal $remote_sandbox.status destroyed
assert (($remote_sandbox | get --optional tokens.local) != null) "sandbox get should return a token"
failure (tg --url $remote.url process get $process | complete)

tg push --sandbox-processes $sandbox
success (tg --url $remote.url process get $process | complete)
