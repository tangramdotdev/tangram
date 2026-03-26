use ../../test.nu *

let remote = spawn -n remote
let local = spawn -n local

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("stdout line 1");
			console.error("stderr line 1");
			console.log("stdout line 2");
			console.error("stderr line 2");
		};
	'
}

let id = tg build -d $path | str trim
tg wait $id

let combined = tg process log $id o+e>| complete
let stdout = tg process log --stream stdout $id | complete
let stderr = tg process log --stream stderr $id | complete
let combined = $combined.stdout | lines | where $it != "" | sort | str join "\n"
let combined = $combined + "\n"

snapshot $combined '
	stderr line 1
	stderr line 2
	stdout line 1
	stdout line 2

'

snapshot $stdout.stdout '
	stdout line 1
	stdout line 2

'

snapshot $stderr.stderr '
	stderr line 1
	stderr line 2

'

# Read from the middle of stdout stream only (stdout positions: 0-13 "stdout line 1\n", 14-27 "stdout line 2\n").
let stdout_partial = tg process log --stream stdout --position 7 $id | complete
snapshot $stdout_partial.stdout '
	line 1
	stdout line 2

'

# Push to remote and test log reading.
tg remote put default $remote.url | complete
tg push --logs $id

# Read logs from remote.
let remote_combined = tg --url $remote.url process log $id o+e>| complete
let remote_stdout = tg --url $remote.url process log --stream stdout $id | complete
let remote_stderr = tg --url $remote.url process log --stream stderr $id | complete
let remote_combined = $remote_combined.stdout | lines | where $it != "" | sort | str join "\n"
let remote_combined = $remote_combined + "\n"

snapshot $remote_combined '
	stderr line 1
	stderr line 2
	stdout line 1
	stdout line 2

'

snapshot $remote_stdout.stdout '
	stdout line 1
	stdout line 2

'

snapshot $remote_stderr.stderr '
	stderr line 1
	stderr line 2

'

# Read from the middle of stdout stream only from remote.
let remote_stdout_partial = tg --url $remote.url process log --stream stdout --position 7 $id | complete
snapshot $remote_stdout_partial.stdout '
	line 1
	stdout line 2

'
