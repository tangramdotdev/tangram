use ../../lib/test.nu *

# Concurrent sandboxes with a CPU maximum are pinned to different hardware threads.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux CPU limits' }

if (sys cpu | length) < 2 {
	skip_test 'this test requires at least two CPUs'
}

let local = server spawn --busybox --config {
	runner: { sandbox_pool_size: 0 },
}

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function () {
			let env = await tg.build(busybox);
			let child = (name: string) => tg.build`echo ${name} > /dev/null && sleep 2 && grep Cpus_allowed_list /proc/self/status > ${tg.output}`.env(env).cpu({ max: 1 });
			let outputs = await Promise.all([child("a"), child("b")]);
			return Promise.all(outputs.map((output) => output.text));
		}
	',
}

let output = tg build $path | complete
success $output
let lists = $output.stdout | from json | each { str trim }
assert (($lists | uniq | length) == 2) $'the sandboxes should be pinned to different threads, but they were pinned to ($lists)'
