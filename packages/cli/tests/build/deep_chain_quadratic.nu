use ../../test.nu *

# Awaiting a child subscribes to its "processes.<id>.status" subject. A depth-D chain with D live awaits should scale linearly with the chain depth.

let server = spawn --config { tokio_single_threaded: false }

# The salt threads through every level, so a given (depth, salt) build is fully independent: nothing caches from a shallower build, and each measurement pays the full depth.
let path = artifact {
	tangram.ts: '
		export const chain = async (n: number, salt: string): Promise<tg.Directory> => {
			if (n <= 0) {
				return tg.directory({ leaf: tg.file("base-" + salt) });
			}
			const inner = await tg.build(chain, n - 1, salt);
			return tg.directory({ ["l" + n]: inner, sib: tg.file("s" + n) });
		};
		export default (arg: string) => {
			const [depth, salt] = arg.split(":");
			return tg.build(chain, Number(depth), salt);
		};
	',
}

# Pay the one-time JS runtime bootstrap cost so it does not skew the first timing.
tg build $"($path)#default" --arg-string "10:warm" | complete

# Measure depth 50 and depth 200 -- a 4x deeper chain -- and compare their ratio rather than an absolute time, so the bound is machine-independent. Linear costs about 4x, quadratic about 16x.
mut rows = []
for depth in [50, 200] {
	let t0 = (date now)
	let out = (tg build $"($path)#default" --arg-string $"($depth):d($depth)" | complete)
	let ns = (((date now) - $t0) | into int)
	$rows = ($rows | append { depth: $depth, secs: ($ns / 1_000_000_000 | math round -p 2), exit: $out.exit_code, ns: $ns })
}
print ($rows | select depth secs exit)

let failed = ($rows | where exit != 0)
assert (($failed | length) == 0) $"($failed | length) of 2 builds failed"

let first = ($rows | first | get ns)
let last = ($rows | last | get ns)
let ratio = ($last / $first)
let ratio_rounded = ($ratio | math round -p 2)
print $"depth50=(($first / 1_000_000_000) | math round -p 2)s depth200=(($last / 1_000_000_000) | math round -p 2)s ratio=($ratio_rounded)x"
assert ($ratio < 5.0) $"the build compounded ($ratio_rounded)x over a 4x deeper chain: it is super-linear in the chain depth"
