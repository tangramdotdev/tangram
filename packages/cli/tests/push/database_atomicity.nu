use ../../test.nu *

# A failed sync leaves objects and processes as cache entries but does not commit any database nodes.

let remote = spawn --cloud --name remote
let local = spawn --busybox --name local
tg remote put default $remote.url

# Create a destroyed sandbox that can be staged before the failure.
let finished_module = artifact {
	tangram.ts: 'export default function () { return tg.file("finished"); }',
}
let finished_process = tg build --detach $finished_module | str trim
tg wait $finished_process
tg index
let sandbox = tg get $finished_process | from json | get sandbox

# Start a process that remains running long enough for its sync to fail.
let module = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			await tg.run`sleep 60`.env(tg.build(busybox)).sandbox();
		}
	',
}
let output = tg build --detach --verbose $module | from json

# Stage a group and tag before encountering the running process.
tg tag put -p atomic/incomplete $output.process
let pushed = tg push --group-children $sandbox atomic | complete
failure $pushed
assert ($pushed.stderr | str contains "expected a finished process")

# Neither database node nor the sandbox is committed after the sync fails.
failure (tg --url $remote.url group get atomic | complete)
failure (tg --url $remote.url sandbox get $sandbox | complete)
failure (tg --url $remote.url tag get atomic/incomplete | complete)

tg cancel $output.process $output.lease
tg wait $output.process
