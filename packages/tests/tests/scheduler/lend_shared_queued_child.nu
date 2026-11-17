use ../../util/test.nu *

# A process that a parent spawns while a child borrows the parent's capacity runs when the child spawns the same process, instead of waiting forever for capacity.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux sandboxes' }

let local = server spawn --config {
	advanced: { checkpoints: true },
	runner: { sandbox_pool_size: 0, sandboxes: 1 },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			let child = await tg.spawn(waiter).sandbox();
			let [outcome, output] = await Promise.all([child.wait(), tg.build(leaf)]);
			return [outcome.output, output];
		}
		export let waiter = () => tg.build(leaf);
		export let leaf = () => "leaf";
	',
}

# Hold the waiter before execution while it borrows the parent's capacity.
let start = checkpoint watch runner.process.start
let enqueued = checkpoint watch scheduler.sandbox.enqueued
let build = job spawn {
	let job_id = job id
	tg build $path | complete | job send --tag $job_id 0
}
checkpoint pass $start | ignore
checkpoint wait $start 1 | ignore

# Queue and index the parent's leaf before the waiter asks to build the same command.
checkpoint pass $enqueued | ignore
checkpoint unwatch $enqueued
tg index
checkpoint unwatch $start

let output = job recv --tag $build --timeout 30sec
success $output 'the shared child should run on the capacity that its parent lends'
assert equal ($output.stdout | from json) ["leaf" "leaf"]
