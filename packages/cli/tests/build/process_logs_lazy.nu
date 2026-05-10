use ../../test.nu *

let remote = spawn -n remote
let local = spawn -n local

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("stdout line 1");
			console.error("stderr line 1");
		};
	'
}

let id = tg build -d $path | str trim
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
		export default () => {
			return 1;
		};
	'
}

let no_log_id = tg build -d $no_log_path | str trim
tg wait $no_log_id

let output = tg push --lazy --logs $no_log_id | complete
success $output
