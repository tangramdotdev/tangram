use ../lib/test.nu *

# Each lookup retains its own subtree token, so descending does not search the authorization index for parents.

let local = server spawn --config {
	advanced: { checkpoints: true }
	tracing: {
		filter: 'tangram=info,tangram_index::verify::facts=debug'
		stderr_format: 'json'
	}
}

# Exclude background capture from the descent measurement.
tg checkpoint watch permission_capture.started | ignore

let path = artifact {
	tangram.ts: '
		const nest = async (depth: number) => {
			let directory = await tg.directory();
			for (let index = depth - 1; index >= 0; index--) {
				directory = await tg.directory({ [`d${index}`]: directory });
			}
			return directory;
		};

		export const descend = async (directory: tg.Directory, depth: number) => {
			const directories = [];
			for (let index = 0; index < depth; index++) {
				if (index > 0) directories.push(directory.id);
				directory = tg.Directory.expect(await directory.get(`d${index}`));
			}
			return directories;
		};

		export default async () => tg.build(descend, await nest(8), 8);
	'
}

let start_watch = tg checkpoint watch runner.process.start | from json | get watch
let build = job spawn {
	let job_id = job id
	let output = tg build $path | complete
	$output | job send --tag $job_id 0
}

# Run the parent so it can create the directory and spawn the descent process.
tg checkpoint wait runner.process.start $start_watch 0 | ignore
tg checkpoint continue runner.process.start $start_watch 0

# The child has checked out its inputs but has not executed its directory lookups.
tg checkpoint wait runner.process.start $start_watch 1 | ignore
let started_at = date now
tg checkpoint continue runner.process.start $start_watch 1
tg checkpoint unwatch runner.process.start $start_watch
let output = job recv --tag $build --timeout 10sec
success $output
let directories = $output.stdout | from json
assert equal ($directories | length) 7
server stop $local
let reads = open $local.log
	| lines
	| where ($it | str starts-with '{')
	| each { |line| $line | from json }
	| where { |event| ($event.timestamp | into datetime) >= $started_at }
	| where $it.fields.message? in ['check object parent for authorization', 'read object parents for authorization']
	| where { |event| $event.fields.object? in $directories }
	| length
assert equal $reads 0
