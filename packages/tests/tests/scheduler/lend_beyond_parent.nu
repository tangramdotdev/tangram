use ../../util/test.nu *

# A waiting parent lends what it can to its child, which acquires the rest from the runner. The parent lends all of its CPU, including its minimum, and the memory above its memory minimum.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux sandboxes' }

if (sys cpu | length) < 2 {
	skip_test 'this test requires at least two CPUs'
}

let local = server spawn --busybox --config {
	runner: { sandbox_pool_size: 0, sandboxes: 1 },
}

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		let mib = 1024 ** 2;
		export let memory = () => tg.build(parent, "memory", {}).sandbox({ memory: { min: 64 * mib } });
		export let max = () => tg.build(nproc).sandbox({ cpu: { max: 1 } });
		export let nproc = async () => {
			let env = await tg.build(busybox);
			let output = await tg.build`nproc > ${tg.output}`.env(env).sandbox({ cpu: { max: 2 } });
			return output.text;
		};
		export let cpu = () => tg.build(parent, "cpu", { cpu: { min: 0.5 }, memory: { min: 32 * mib } }).sandbox({ cpu: { min: 1 } });
		export let parent = async (name: string, child: tg.Sandbox.Arg) => {
			let env = await tg.build(busybox);
			let output = await tg.build`echo ${name} > ${tg.output}`.env(env).sandbox(child);
			return output.text;
		};
	',
}

# A parent with a memory minimum lends to a child without minima.
let output = timeout 60s tg build $'($path)#memory' | complete
success $output 'a parent with a memory minimum should lend to its child'
assert equal ($output.stdout | from json) "memory\n"

# A parent with a CPU minimum lends it to a child, which acquires its own memory minimum.
let output = timeout 60s tg build $'($path)#cpu' | complete
success $output 'a parent with a CPU minimum should lend it to its child'
assert equal ($output.stdout | from json) "cpu\n"

# A parent with a CPU maximum lends it to a child with a larger CPU maximum, which acquires the rest.
let output = timeout 60s tg build $'($path)#max' | complete
success $output 'a parent with a CPU maximum should lend it to a child with a larger CPU maximum'
assert equal ($output.stdout | from json) "2\n" 'the child should see its own CPU maximum'
