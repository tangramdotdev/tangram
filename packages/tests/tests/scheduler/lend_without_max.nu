use ../../util/test.nu *

# A waiting parent with a CPU maximum lends its capacity to children without a CPU maximum, which take the parent's maximum.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux sandboxes' }

let local = server spawn --busybox --config {
	runner: { sandbox_pool_size: 0, sandboxes: 1 },
}

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.build(parent).sandbox().cpu({ max: 1 });
		export let parent = async () => {
			let env = await tg.build(busybox);
			let output = await tg.build`nproc > ${tg.output}`.env(env);
			return output.text;
		};
	',
}

let output = timeout 60s tg build $path | complete
success $output 'the child should run on the capacity that its parent lends'
assert equal ($output.stdout | from json) "1\n" 'the child should take the CPU maximum of its parent'
