use ../../test.nu *

# A deep chain of sandboxed builds completes with a single runner allocation.
# Each parent process waits for a sandboxed child, so the child must be able
# to borrow the parent's capacity.

let server = spawn --config {
	runner: {
		cpus: 1,
		memory: (1e9 | into int),
	},
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(step, 24).sandbox();
		}

		export async function step(depth: number): Promise<string> {
			if (depth === 0) {
				return "done";
			}
			return await tg.build(step, depth - 1).sandbox();
		}
	',
}

let output = timeout 15s tg build $path | complete
success $output "a deep sandboxed process tree should complete by borrowing parent capacity"
