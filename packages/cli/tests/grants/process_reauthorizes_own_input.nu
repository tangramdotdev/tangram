use ../../test.nu *

# What the missing token costs. A single authorization is too small to see against the cost of spawning a client, so the input is a directory of eight subdirectories with a closure of 8465 objects each, and reading it to depth two authorizes the whole top of that tree in one call. Root short circuits authorization entirely, so the excess over root is the price of re-proving entitlement the process already held. In a real linker proxy run this was 21654 authorize calls, 33.7 of 40 seconds, over five directories re-authorized between 613 and 1481 times each.

# Building the input creates 67000 objects, and this measures a per call latency, so the server needs the machine rather than the harness default of a single thread.
let server = spawn --busybox --config {
	index: { kind: 'lmdb', map_size: 8_589_934_592 },
	object: { store: { kind: 'lmdb', map_size: 8_589_934_592 } },
	tokio_single_threaded: false,
	v8_thread_pool_size: 8,
}

const window = 4

# Every leaf is unique, because identical subtrees would be deduplicated by content address and collapse the closure. A failed read exits non zero rather than being counted, so a denial cannot be mistaken for a fast read.
let source = '
	import busybox from "busybox";
	const build = async (depth: number, tag: string): Promise<tg.Artifact> => {
		if (depth === 0) {
			return tg.file(`leaf ${tag}`);
		}
		const entries: Record<string, tg.Unresolved<tg.Artifact>> = {};
		for (let i = 0; i < 16; i++) {
			entries[`e${i}`] = await build(depth - 1, `${tag}_${i}`);
		}
		return tg.directory(entries);
	};
	export default async function () {
		const entries: Record<string, tg.Unresolved<tg.Artifact>> = {};
		for (let i = 0; i < 8; i++) {
			entries[`s${i}`] = await build(3, `s${i}`);
		}
		const target = await tg.directory(entries);
		return tg.run`
			target=$(basename ${target})
			start=$(date +%s)
			n=0
			while [ $(( $(date +%s) - start )) -lt WINDOW ]; do
				tg get $target --depth 2 > /dev/null || exit 1
				n=$(( n + 1 ))
			done
			echo "$target $n" > $TANGRAM_OUTPUT
		`.env(tg.build(busybox)).then(tg.File.expect);
	}
' | str replace 'WINDOW' ($window | into string)

let fields = tg build (artifact { tangram.ts: $source }) | str trim | tg cat $in | str trim | split row ' '
let process_ms = ($window * 1000 | into float) / ($fields | last | into int)

mut reads = 0
let start = date now
while ((date now) - $start) < ($window * 1sec) {
	tg get ($fields | first) --depth 2 | ignore
	$reads = $reads + 1
}
let excess = $process_ms - (($window * 1000 | into float) / $reads)

assert ($excess < 1) $"a process must not pay to re-prove entitlement to its own input, but paid ($excess)ms per read"
