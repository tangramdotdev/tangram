use ../../util/test.nu *

# A process that a parent spawns while a child borrows the parent's capacity runs when the child spawns the same process, instead of waiting forever for capacity.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux sandboxes' }

let local = server spawn --config {
	runner: { sandbox_pool_size: 0, sandboxes: 1 },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			let child = await tg.spawn(waiter).sandbox();
			while (true) {
				await child.reload();
				if (child.state!.status === "started") {
					break;
				}
				await tg.sleep(0.1);
			}
			let [outcome, output] = await Promise.all([child.wait(), tg.build(leaf)]);
			return [outcome.output, output];
		}
		export let waiter = async () => {
			await tg.sleep(3);
			return tg.build(leaf);
		};
		export let leaf = () => "leaf";
	',
}

let output = timeout 60s tg build $path | complete
success $output 'the shared child should run on the capacity that its parent lends'
assert equal ($output.stdout | from json) ["leaf" "leaf"]
