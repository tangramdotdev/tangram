use ../../test.nu *

# Identical builds should cost the same. Here each is slower than the last, because the indexer
# propagates every grant across an object's entire ancestry, and that cost is super-linear in the
# grants already accumulated on a shared object. The template combines the two conditions that
# trigger the blow-up:
#   1. `shared` returns a constant, so its outputs are identical content-addressed objects in every
#      build — a fixed set of shared objects the builds keep re-granting.
#   2. `wrap` embeds a per-build salt, so nothing caches: every build re-runs and wraps those shared
#      objects in fresh parents, adding one more grant and one more ancestor to each.
# Every grant write fans out one visibility entry per ancestor (packages/index/src/lmdb/grant/put.rs),
# so ten identical builds compound; a constant-cost indexer keeps their times flat. This is the
# pathology behind the multi-minute std build "hang".

let server = spawn --config { tokio_single_threaded: false, v8_thread_pool_size: 8 }

let template = '
export function shared(i: number) { return i; }
export function wrap(w: number) {
	let _ = __SALT__;
	let children = [];
	for (let i = 0; i < 8; i++) { children.push(tg.build(shared, i)); }
	return Promise.all(children).then(() => w);
}
export default async function () {
	let wrappers = [];
	for (let w = 0; w < 16; w++) { wrappers.push(tg.build(wrap, w)); }
	await Promise.all(wrappers);
	return "done";
}
'

# Build the same graph ten times, salting only the wrappers, and time each build end to end.
mut rows = []
for trial in 0..<10 {
	let src = ($template | str replace --all "__SALT__" ($trial | into string))
	let path = (artifact { tangram.ts: $src })
	let t0 = (date now)
	let out = (tg build $path | complete)
	let ns = (((date now) - $t0) | into int)
	$rows = ($rows | append { trial: $trial, secs: ($ns / 1_000_000_000 | math round -p 2), exit: $out.exit_code, ns: $ns })
}
print ($rows | select trial secs exit)

# The timing is only meaningful if every build completed.
let failed = ($rows | where exit != 0)
assert (($failed | length) == 0) $"($failed | length) of 10 builds failed"

# Compare slowest to fastest, not an absolute time, so the bound is machine-independent: a
# constant-cost indexer keeps the ratio near one; super-linear grant propagation makes it grow.
let fastest = ($rows | get ns | math min)
let slowest = ($rows | get ns | math max)
let ratio = ($slowest / $fastest)
let ratio_rounded = ($ratio | math round -p 2)
print $"fastest=(($fastest / 1_000_000_000) | math round -p 2)s slowest=(($slowest / 1_000_000_000) | math round -p 2)s ratio=($ratio_rounded)x"
assert ($ratio < 2.5) $"identical builds compounded ($ratio_rounded)x: grant propagation is super-linear in the grants accumulated on the shared objects"
