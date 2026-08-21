use ../../test.nu *

# One parent spawns the same command twice concurrently. The first spawn inserts
# the child into the parent's children and then indexes the parent-child edge.
# The second spawn sees the child already present and returns without indexing,
# so if it waits before the first spawn's edge is written, the wait is denied.

let server = spawn --config {
	advanced: {
		checkpoints: true,
	},
	tracing: {
		stderr_format: 'json',
	},
}

let path = artifact {
	tangram.ts: '
		export function shared() {
			return tg.run`echo shared > $TANGRAM_OUTPUT`.then(tg.File.expect);
		}
		export default async function () {
			await Promise.all([
				tg.build(shared).named("first"),
				tg.build(shared).named("second"),
			]);
			return "ok";
		}
	',
}

let index_watch = (
	tg checkpoint watch process.spawn.child.index
	| from json
	| get watch
)
let add_watch = (
	tg checkpoint watch process.spawn.child.add --params '{"cached":true}'
	| from json
	| get watch
)

def build_background [path: string] {
	job spawn {
		let job_id = job id
		let output = tg build $path | complete
		$output | job send --tag $job_id 0
	}
}

let build = build_background $path

# Hold the first spawn immediately before it indexes the parent-child edge.
let indexing = tg checkpoint wait process.spawn.child.index $index_watch 0 | from json
print $"holding edge index: ($indexing.params)"

# Let the second spawn take the duplicate path, which does not index the edge.
let duplicate = tg checkpoint wait process.spawn.child.add $add_watch 0 | from json
print $"duplicate child add: ($duplicate.params)"
tg checkpoint continue process.spawn.child.add $add_watch 0
tg checkpoint unwatch process.spawn.child.add $add_watch

# Give the second spawn time to wait on the child while the edge is missing.
sleep 5sec

tg checkpoint continue process.spawn.child.index $index_watch 0
tg checkpoint unwatch process.spawn.child.index $index_watch

let output = job recv --tag $build --timeout 120sec
print $"exit: ($output.exit_code)"
print $output.stderr
print (server_errors $server)
success $output "the duplicate spawn should be able to wait on the shared child"
