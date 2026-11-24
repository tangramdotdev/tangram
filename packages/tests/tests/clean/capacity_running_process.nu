use ../../util/test.nu *

# Capacity cleaning while a process runs keeps an object that the process created and has not yet returned, and removes an object that was last touched well before the process was created.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true },
	indexer: {
		cleaning: {
			capacity: {
				kind: bytes
				start_above: 1
				stop_at: 0
			}
			poll_interval: 10800
		}
	}
	object: { ttl: 86400 }
	scheduler: { runner_ttl: 86400 }
}

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

# Store an old object, and advance the clock past the time to touch but not to the next cleaning.
let old = tg put --no-tokens $"tg.file\((random chars | to json --raw)\)" | referent node
tg index
advance_time $local 2hr

# Spawn the process, and wait for it to store its object.
let contents = random chars
let process = tg spawn --no-tokens --sandbox --mount $'($directory):($directory)' $path --arg-string $contents | str trim
success (timeout 60s sh -c 'echo started > "$1"' sh $started | complete) 'the process should store the object'
tg index

# Advance the clock to wake cleaning, and wait for it to delete the old object, which it decides in the same batch as the process's object.
let delete = checkpoint watch cleaning.object.delete
advance_time $local 1hr
mut hit = 0
while (checkpoint pass $delete $hit | get params.object) != $old {
	$hit += 1
}
checkpoint unwatch $delete
let get = tg object get $old | complete
failure $get 'the old object should be removed'

# Let the process return its object. The process finishes successfully, and its output is readable.
success (timeout 60s sh -c 'echo resume > "$1"' sh $resume | complete) 'the process should resume'
let outcome = tg wait $process | from json
assert equal $outcome.exit 0 'the process should succeed'
let output = tg output $process | from json
assert equal (tg cat $output.value) $contents 'the output should be readable'
