use ../../util/test.nu *

# A clean while a process runs keeps an object that the process stored again shortly after it was last touched, because the time to touch skips the touch of the second store.

let local = server spawn --now '2026-01-01T00:00:00Z'

# Create the FIFOs through which the process reports that it stored the object and waits to continue.
let directory = mktemp --directory
let started = $directory | path join started
let resume = $directory | path join resume
^mkfifo $started $resume
let module = '
	export default async (contents: string) => {
		let file = await tg.file(contents);
		let id = await file.store();
		await tg.host.readFile(STARTED);
		await tg.host.readFile(RESUME);
		return tg.File.withId(id);
	};
' | str replace 'STARTED' ($started | to json --raw) | str replace 'RESUME' ($resume | to json --raw)
let path = artifact { tangram.ts: $module }

# Store the object, and create the process within the time to touch of the object's touch.
let contents = random chars
let object = tg put --no-tokens $"tg.file\(($contents | to json --raw)\)" | referent node
tg index
advance_time $local 30min

# Spawn the process, and wait for it to store the object again.
let process = tg spawn --no-tokens --sandbox --mount $'($directory):($directory)' $path --arg-string $contents | str trim
success (timeout 60s sh -c 'echo started > "$1"' sh $started | complete) 'the process should store the object'

# Clean while the process runs, and then let it return the object.
success (tg clean | complete) 'the clean should succeed'
success (timeout 60s sh -c 'echo resume > "$1"' sh $resume | complete) 'the process should resume'

# The process finishes successfully with the object, and its output is readable.
let outcome = tg wait $process | from json
assert equal $outcome.exit 0 'the process should succeed'
let output = tg output $process | from json
assert equal ($output.value | referent node) $object 'the process should return the object'
assert equal (tg cat $output.value) $contents 'the output should be readable'
