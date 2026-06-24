use ../../test.nu *

# Pushing a process's logs lazily makes them readable from the remote, and a lazy log push of a process with no logs still succeeds.

let remote = spawn --name remote
let local = spawn --name local

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("stdout line 1");
			console.error("stderr line 1");
		}
	'
}

let id = tg build --detach $path | str trim
tg wait $id

tg remote put default $remote.url
tg push --lazy --logs $id

let remote_combined = tg --url $remote.url process log $id o+e>| complete
let remote_combined = $remote_combined.stdout | lines | where $it != "" | sort | str join "\n"
let remote_combined = $remote_combined + "\n"

snapshot $remote_combined '
	stderr line 1
	stdout line 1

'

let no_log_path = artifact {
	tangram.ts: '
		export default function () {
			return 1;
		}
	'
}

let no_log_id = tg build --detach $no_log_path | str trim
tg wait $no_log_id

let output = tg push --lazy --logs $no_log_id | complete
success $output
