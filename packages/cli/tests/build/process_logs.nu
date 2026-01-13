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

snapshot $combined.stdout '
	stdout line 1
	stdout line 2
	stderr line 1
	stderr line 2

'

snapshot $stdout.stdout '
	stdout line 1
	stdout line 2

'

snapshot $stderr.stderr '
	stderr line 1
	stderr line 2

'

# Test reading with position.
# Combined log layout (each line is 14 bytes including newline):
#   0-13:  "stdout line 1\n"
#   14-27: "stdout line 2\n"
#   28-41: "stderr line 1\n"
#   42-55: "stderr line 2\n"

# Read starting at position 7 (middle of "stdout line 1\n").
let partial = tg process log --position 7 $id o+e>| complete
snapshot $partial.stdout '
	line 1
	stdout line 2
	stderr line 1
	stderr line 2

'

# Read at position 14 (start of "stdout line 2\n" based on actual log layout).
let boundary = tg process log --position 14 $id o+e>| complete
snapshot $boundary.stdout '
	stdout line 2
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

snapshot $remote_combined.stdout '
	stdout line 1
	stdout line 2
	stderr line 1
	stderr line 2

'

snapshot $remote_stdout.stdout '
	stdout line 1
	stdout line 2

'

snapshot $remote_stderr.stderr '
	stderr line 1
	stderr line 2

'

# Test position-based reading from remote.
let remote_partial = tg --url $remote.url process log --position 7 $id o+e>| complete
snapshot $remote_partial.stdout '
	line 1
	stdout line 2
	stderr line 1
	stderr line 2

'

# Read at position 14 (boundary between lines) from remote.
let remote_boundary = tg --url $remote.url process log --position 14 $id o+e>| complete
snapshot $remote_boundary.stdout '
	stdout line 2
	stderr line 1
	stderr line 2

'

# Read from the middle of stdout stream only from remote.
let remote_stdout_partial = tg --url $remote.url process log --stream stdout --position 7 $id | complete
snapshot $remote_stdout_partial.stdout '
	line 1
	stdout line 2

'
