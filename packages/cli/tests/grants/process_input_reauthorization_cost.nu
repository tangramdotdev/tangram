use ../../test.nu *

# What re-authorizing an input costs. A process loads one directory from its own input repeatedly, first against a server that authorizes normally and then against one configured to grant everything. Authorization reaches the input by walking up to the command, which holds the process's only grant, so the price is set by how many objects lie between the two.

let source = '
	const DISTANCE = 256;
	const N = 100;

	export const measure = async (wrapper: tg.Directory) => {
		let dir = wrapper;
		for (let i = 0; i < DISTANCE; i++) {
			dir = await dir.get(`d${i}`).then(tg.Directory.expect);
		}
		const id = dir.id;
		const start = Date.now();
		for (let i = 0; i < N; i++) {
			await tg.Directory.withId(id).load();
		}
		return tg.file(`${N} ${Date.now() - start}`);
	};

	export default async () => {
		let dir = await tg.directory({ "libfoo.so": tg.file("foo") });
		for (let i = DISTANCE - 1; i >= 0; i--) {
			dir = await tg.directory({ [`d${i}`]: dir });
		}
		return tg.build(measure, dir);
	};
'

let path = artifact { tangram.ts: $source }

def elapsed_ms []: nothing -> float {
	let fields = tg build $path | str trim | tg cat $in | str trim | split row ' '
	($fields | last | into float) / ($fields | first | into float)
}

let authorizing = spawn --name authorizing
let authorizing_ms = elapsed_ms

let granting = spawn --name granting --config {
	advanced: { authorize_always_unsafe: true },
}
let granting_ms = elapsed_ms

print $"($authorizing_ms)ms per load authorizing, ($granting_ms)ms per load granting everything"

assert ($authorizing_ms < $granting_ms * 3) $"authorizing an input the process already holds cost ($authorizing_ms)ms per load against ($granting_ms)ms when authorization is skipped"
