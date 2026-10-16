use ../lib/test.nu *

# A queued child borrows its parent's capacity when sandboxes have no CPU limit.

let local = server spawn --config {
	runner: { memory: 1_073_741_824 },
	scheduler: { default_memory: 1_073_741_824 },
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

let output = timeout 30s tg build $path | complete
success $output "the queued child should run on the parent's capacity"
assert equal ($output.stdout | from json) [a b]
