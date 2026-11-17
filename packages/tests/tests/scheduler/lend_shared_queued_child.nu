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
let root = tg build --detach --verbose $path | from json | get process
checkpoint pass $start | ignore
checkpoint wait $start 1 | ignore

# Queue and index the parent's leaf before the waiter asks to build the same command.
checkpoint pass $enqueued | ignore
checkpoint unwatch $enqueued
tg index
checkpoint unwatch $start

let output = timeout 30s tg wait $root | complete
success $output 'the shared child should run on the capacity that its parent lends'
assert equal ($output.stdout | from json | get output) ["leaf" "leaf"]

# The parent and the waiter share one leaf process, so the command runs once.
let children = tg process children --no-tokens $root | from json | get process | each { split row '?' | first }
let waiter = $children | get 0
let leaf = $children | get 1
let shared = tg process children --no-tokens $waiter | from json | get 0.process | split row '?' | first
assert equal $shared $leaf 'the waiter should reuse the leaf that its parent queued'
