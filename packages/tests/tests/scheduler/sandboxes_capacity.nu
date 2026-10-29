use ../../lib/test.nu *

# A runner runs no more sandboxes at once than its sandbox capacity, and a waiting parent lends its sandbox capacity to its children.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux sandboxes' }

let local = server spawn --busybox --config {
	runner: { sandbox_pool_size: 0, sandboxes: 1 },
}

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function () {
			let env = await tg.build(busybox);
			let child = (name: string) => tg.build`sleep 2 && echo ${name} > ${tg.output}`.env(env);
			let outputs = await Promise.all([child("a"), child("b"), child("c")]);
			return Promise.all(outputs.map((output) => output.text));
		}
	',
}

let start = date now
let output = timeout 60s tg build $path | complete
let elapsed = (date now) - $start
success $output 'the children should run on the capacity that their parent lends'
assert equal ($output.stdout | from json) ["a\n" "b\n" "c\n"]
assert ($elapsed >= 6sec) $'the children should run one at a time, but they finished in ($elapsed)'
