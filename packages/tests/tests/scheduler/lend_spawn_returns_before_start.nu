use ../../util/test.nu *

# A spawn of a child that must wait to borrow its parent's capacity returns while the child is queued, and the child runs on the parent's capacity once the capacity is free.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux sandboxes' }

let local = server spawn --config {
	runner: { sandbox_pool_size: 0, sandboxes: 1 },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			// Spawn a child that borrows the capacity of the parent until the parent closes its stdin.
			let first = await tg.spawn`read line`.stdin("pipe").sandbox();

			// Spawn a child that must wait for the capacity of the parent, and get its status once the spawn returns.
			let second = await tg.spawn(leaf).sandbox();
			let output = await tg.client.getProcess(second.id, { tokens: second.tokens.toData() });
			let status = output.data.status;

			// Free the capacity of the parent, and wait for the second child, which runs on it.
			await first.stdin.close();
			await first.wait();
			let outcome = await second.wait();
			return [status, outcome.output];
		}
		export let leaf = () => "leaf";
	',
}

let output = timeout 30s tg build $path | complete
success $output 'the parent should get the output of the child that borrows its capacity'
assert equal ($output.stdout | from json) ["queued" "leaf"] 'the spawn should return while the child is queued'
