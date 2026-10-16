use ../lib/test.nu *

# A queued child borrows its parent's capacity when sandboxes have no CPU limit.

let local = server spawn --config {
	advanced: { checkpoints: true },
	runner: { memory: 1_073_741_824 },
	scheduler: { default_memory: 1_073_741_824 },
	tracing: { filter: 'tangram_server::scheduler=info', stderr_format: 'json' },
}

let path = artifact {
	tangram.ts: '
		export function child(id: string) {
			return id;
		}

		export default async function () {
			return await Promise.all([tg.build(child, "a"), tg.build(child, "b")]);
		}
	',
}

let watch = tg checkpoint watch runner.sandbox.capacity.release | from json | get watch
let process = tg build --no-tokens --detach $path | referent node

# Hold the first child's allocation until the other child has queued.
let borrower = tg checkpoint wait runner.sandbox.capacity.release $watch 0 | from json | get params.sandbox
let parent = tg process get --source runner $process | from json | get sandbox
wait_until {
	open --raw $local.log
		| lines
		| each { try { from json } catch { null } }
		| compact
		| any { |event|
			$event.fields.message? == 'enqueued the sandbox' and $event.fields.sandbox? not-in [$parent $borrower]
		}
} 'the second child should queue before the first child releases its allocation'
tg checkpoint unwatch runner.sandbox.capacity.release $watch

let output = timeout 30s tg wait $process | complete
success $output "the queued child should run on the parent's capacity"
let result = $output.stdout | from json
assert equal $result.exit 0
assert equal $result.output [a b]
