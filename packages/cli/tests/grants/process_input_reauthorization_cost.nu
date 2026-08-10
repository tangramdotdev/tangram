use ../../test.nu *

# What re-authorizing an input costs. A process loads one directory from its own input repeatedly, first against a server that authorizes normally and then against one configured to grant everything. The walk reaches the input by going up to the command, which holds the process's only grant, so the price tracks how many parents the input has, and a shared library directory has many.

let source = '
	const PARENTS = 256;
	const N = 100;

	export const measure = async (wrapper: tg.Directory) => {
		const dir = await wrapper.get("p0/lib").then(tg.Directory.expect);
		const id = dir.id;
		const start = Date.now();
		for (let i = 0; i < N; i++) {
			await tg.Directory.withId(id).load();
		}
		return tg.file(`${N} ${Date.now() - start}`);
	};

	export default async () => {
		const lib = await tg.directory({ "libfoo.so": tg.file("foo") });
		const entries: Record<string, tg.Unresolved<tg.Directory>> = {};
		for (let i = 0; i < PARENTS; i++) {
			entries[`p${i}`] = tg.directory({ [`u${i}.o`]: tg.file(`unit ${i}`), lib });
		}
		return tg.build(measure, await tg.directory(entries));
	};
'

# Every parent holds a distinct file, because parents with identical contents would collapse to one object and the input would have a single parent.
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
