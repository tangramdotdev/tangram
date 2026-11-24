use ../../util/test.nu *

# A clean while a process runs removes an unreferenced object that was last touched well before the process was created, and reports that it kept the newer items.

let local = server spawn --now '2026-01-01T00:00:00Z'

# Create the FIFO on which the process waits.
let directory = mktemp --directory
let resume = $directory | path join resume
^mkfifo $resume
let module = '
	export default async () => {
		await tg.host.readFile(RESUME);
		return "done";
	};
' | str replace 'RESUME' ($resume | to json --raw)
let path = artifact { tangram.ts: $module }

# Store an object, and advance the clock past the time to touch.
let object = tg put --no-tokens $"tg.file\((random chars | to json --raw)\)" | referent node
tg index
advance_time $local 2hr

# Spawn the process, and wait for it to start.
let process = tg spawn --no-tokens --sandbox --mount $'($directory):($directory)' $path | str trim
tg process status --until started $process | ignore

# Clean while the process runs. The clean removes the object and reports the time after which it kept every item, which is the process's creation less the time to touch and one second.
let output = tg --no-quiet clean | complete
success $output 'the clean should succeed'
assert ($output.stderr | str contains 'kept the items touched after 2026-01-01T00:59:59Z because processes are running') $'the clean should report the limit, got: ($output.stderr)'
let get = tg object get $object | complete
failure $get 'the old object should be removed'

# Let the process finish, and clean again. Nothing is running, so the clean reports no limit.
success (timeout 60s sh -c 'echo resume > "$1"' sh $resume | complete) 'the process should resume'
let outcome = tg wait $process | from json
assert equal $outcome.exit 0 'the process should succeed'
let output = tg --no-quiet clean | complete
success $output 'the clean should succeed'
assert (not ($output.stderr | str contains 'because processes are running')) $'the clean should not be limited, got: ($output.stderr)'
