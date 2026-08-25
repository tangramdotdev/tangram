use ../../test.nu *

# Releasing a candidate's lease after selecting a remote cache hit does not
# cancel the candidate while another caller holds a lease on it.
#
# The first caller starts a local candidate and finds the completed process on
# the remote. The second caller then acquires a lease on the local candidate
# before the first caller releases its lease.
#
# Regression test for f347184b (#838).

let remote = server spawn --name remote
let primary = server spawn --name primary
tg remote put default $remote.url

let shared = artifact {
	tangram.ts: '
		export default function () {
			return tg.directory({ "result.txt": tg.file("shared result") });
		}
	'
}

# Build the shared module on the primary and push only that process.
let shared_process = tg build --detach $shared | str trim
tg wait $shared_process
tg index
tg push --eager --process-outputs --process-children $shared_process

let wrapper_ts = [
	$'import shared from "shared" with { source: "($shared)" };'
	'export default async function (_name: string) {'
	'	await tg.build(shared);'
	'	return "done";'
	'}'
] | str join "\n"
let wrapper = artifact { tangram.ts: $wrapper_ts }

let fresh = server spawn --name fresh --config {
	advanced: {
		checkpoints: true,
	},
}
tg remote put default $remote.url

def build_background [path: string, name: string] {
	job spawn {
		let job_id = job id
		let output = tg build $path --arg-string $name | complete
		$output | job send --tag $job_id 0
	}
}

let acquire_watch = (
	tg checkpoint watch process.spawn.cached.lease.acquire
	| from json
	| get watch
)
let cancel_watch = (
	tg checkpoint watch process.spawn.candidate.cancel
	| from json
	| get watch
)
let cancelled_watch = (
	tg checkpoint watch process.spawn.candidate.cancelled
	| from json
	| get watch
)
let start_watch = (
	tg checkpoint watch runner.process.start
	| from json
	| get watch
)

# Start the first wrapper, then hold its local shared candidate before execution.
let first = build_background $wrapper first
tg checkpoint wait runner.process.start $start_watch 0 | ignore
tg checkpoint continue runner.process.start $start_watch 0
tg checkpoint wait runner.process.start $start_watch 1 | ignore

# Hold the first caller immediately before it releases the candidate's lease.
let cancel = tg checkpoint wait process.spawn.candidate.cancel $cancel_watch 0 | from json

# Prevent the second caller from selecting the remote result.
tg remote delete default

# Start the second wrapper and let it acquire a lease on the local candidate.
let second = build_background $wrapper second
tg checkpoint wait runner.process.start $start_watch 2 | ignore
tg checkpoint continue runner.process.start $start_watch 2
let acquire = tg checkpoint wait process.spawn.cached.lease.acquire $acquire_watch 0 | from json
assert equal $acquire.params.process $cancel.params.process

# Release the first lease, then start the candidate while the second lease remains.
tg checkpoint continue process.spawn.candidate.cancel $cancel_watch 0
tg checkpoint wait process.spawn.candidate.cancelled $cancelled_watch 0 | ignore
tg checkpoint continue process.spawn.candidate.cancelled $cancelled_watch 0
tg checkpoint continue process.spawn.cached.lease.acquire $acquire_watch 0
tg checkpoint continue runner.process.start $start_watch 1
tg checkpoint unwatch process.spawn.cached.lease.acquire $acquire_watch
tg checkpoint unwatch process.spawn.candidate.cancel $cancel_watch
tg checkpoint unwatch process.spawn.candidate.cancelled $cancelled_watch
tg checkpoint unwatch runner.process.start $start_watch

for build in [$first $second] {
	let output = job recv --tag $build --timeout 10sec
	success $output
}
