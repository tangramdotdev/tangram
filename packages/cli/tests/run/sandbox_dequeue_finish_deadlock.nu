use ../../test.nu *

# Reproduces an async deadlock between an in-flight dequeue_sandbox_process and finish_sandbox.

let server = spawn --config {
	runner: {
		concurrency: 4,
	},
}

# Many short-lived sandboxed children.
let parent = artifact {
	tangram.ts: '
		export default async () => {
			const children = [];
			for (let i = 0; i < 256; i++) {
				children.push(tg.run`true ${i.toString()}`.sandbox());
			}
			await Promise.allSettled(children);
			return "done";
		};
	',
}

let output = timeout 45s tg build $parent | complete
success $output "fan-out build should not deadlock between dequeue_sandbox_process and finish_sandbox"
