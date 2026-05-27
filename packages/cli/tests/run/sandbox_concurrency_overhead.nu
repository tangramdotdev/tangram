use ../../test.nu *

# Measures the effective parallelism of a concurrent sandbox fan-out, as a
# harness for investigating the per-process overhead blow-up that a cold proxied
# cargo build hits. The outer process fans out `count` unique sandboxed children
# at once against a `concurrency`-limited runner; the overhead ratio is how much
# slower the fan-out is than the contention-free ideal (waves * per-child cost).
#
# Findings so far (2026-05-27): an in-process JS fan-out does NOT reproduce the
# real build's severity (~2x compute on ~14 permits). Here, clean long-lived
# children parallelize near-perfectly (~7.95/8), heartbeat on vs off is
# identical, and the instrumented sqlite write permit stays fast. The only
# degradation is a fixed per-process overhead (~tens of ms of spawn/finalize)
# that bites only when children are very short. The real slowdown likely needs
# client-level oversubscription (cargo runs ~256 concurrent rustc, each a
# separate tgrustc process doing its own checkin + spawn + wait), which this
# single-outer fan-out does not model. See sandbox_spawn_overhead.nu for the
# sequential (no-contention) baseline.
#
# Each child mounts the busybox artifact (input checkout, like rustc mounting a
# toolchain), optionally sleeps (so the sandbox stays alive across heartbeat
# intervals), and optionally writes an output (output checkin).
#
# Knobs (read from the environment so the reproduction can be swept):
#   TG_COUNT        total concurrent children to fan out (default 64)
#   TG_SLEEP        seconds each child sleeps (default "1")
#   TG_OUTPUT_KB    kilobytes each child writes to its output (default 0)
#   TG_CONC         runner.concurrency on the server (default 8)
#   TG_THREADS      "multi" (default) or "single" tokio runtime
#   TG_DISABLE_HB   set to "1" to disable the heartbeat (experiment, theory 2)

let count = ($env.TG_COUNT? | default "64" | into int)
let sleep_secs = ($env.TG_SLEEP? | default "1")
let output_kb = ($env.TG_OUTPUT_KB? | default "0" | into int)
let concurrency = ($env.TG_CONC? | default "8" | into int)
let single_threaded = (($env.TG_THREADS? | default "multi") == "single")
let disable_heartbeat = (($env.TG_DISABLE_HB? | default "") == "1")

# Disable the heartbeat in the spawned server if requested. The server job
# inherits this environment variable.
if $disable_heartbeat {
	$env.TANGRAM_DISABLE_HEARTBEAT = "1"
}

# Spawn a server that mirrors a real one: multi-threaded tokio, an explicit
# runner concurrency limit, and warn-level tracing so the write-permit
# acquisition summary (instrumented in tangram_database) is visible. Tag the
# busybox package so children have a real `sleep` and `dd`, and give the lmdb
# stores enough room for the children's outputs.
let server = spawn --busybox --config {
	runner: {
		concurrency: $concurrency,
	},
	tokio_single_threaded: $single_threaded,
	tracing: {
		filter: "warn",
	},
	index: {
		kind: 'lmdb',
		map_size: 4_294_967_296,
	},
	object: {
		store: {
			kind: 'lmdb',
			map_size: 4_294_967_296,
		},
	},
	logs: {
		store: {
			kind: 'lmdb',
			map_size: 4_294_967_296,
		},
	},
}

# A package whose default export fans out the requested number of unique
# sandboxed children concurrently and awaits them all. The salt makes every
# phase's processes distinct so that the baseline, single, and fan-out
# measurements do not become cache hits of each other.
let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async (arg: string) => {
			const { salt, count, sleep, outputKb } = JSON.parse(arg);
			const env = await tg.build(busybox);
			const children = [];
			for (let i = 0; i < count; i++) {
				const s = String(sleep);
				const kb = String(outputKb);
				const tag = `${salt}_${i}`;
				children.push(
					tg.run`sleep ${s}; dd if=/dev/zero of=${tg.output} bs=1024 count=${kb} 2>/dev/null; true ${tag}`
						.env(env)
						.sandbox(),
				);
			}
			await Promise.all(children);
			return "done";
		};
	',
}

# Run one phase: a fan-out of `count` children tagged with `salt`, returning the
# wall time.
def run_phase [path: string, salt: string, count: int, sleep: string, output_kb: int] {
	let arg = { salt: $salt, count: $count, sleep: $sleep, outputKb: $output_kb } | to json
	let start = date now
	let output = tg run $path -a $arg | complete
	success $output $"the ($salt) phase should succeed"
	(date now) - $start
}

# Warm up the busybox build so it is not counted in any measured phase.
run_phase $path "warm" 1 $sleep_secs $output_kb

# Baseline: the fixed cost of an outer run with no children (warm busybox).
let baseline = run_phase $path "base" 0 $sleep_secs $output_kb

# Single child: subtracting the baseline gives the uncontended serial cost of
# one child (sleep + output checkin + sandbox setup/teardown), the unit of work
# the fan-out should parallelize.
let serial_one = (run_phase $path "one" 1 $sleep_secs $output_kb) - $baseline

# Fan out all of the children concurrently.
let total = run_phase $path "many" $count $sleep_secs $output_kb
let net = $total - $baseline

# A contention-free fan-out runs ceil(count / concurrency) waves, each about the
# serial cost of one child, so the ideal net is waves * serial_one. The overhead
# ratio is how much slower the real fan-out is than that ideal, and the
# effective parallelism is how many children were truly in flight at once. With
# the slowdown present, overhead_ratio is well above 1 and effective parallelism
# is well below the concurrency limit.
let waves = ($count + $concurrency - 1) // $concurrency
let ideal_net = $waves * $serial_one
let overhead_ratio = ($net / $ideal_net)
let effective_parallelism = ($count * $serial_one) / $net

print -e $"concurrency=($concurrency) count=($count) sleep=($sleep_secs)s output_kb=($output_kb) single_threaded=($single_threaded) disable_heartbeat=($disable_heartbeat)"
print -e $"baseline=($baseline) serial_one=($serial_one) total=($total) net=($net)"
print -e $"waves=($waves) ideal_net=($ideal_net) overhead_ratio=($overhead_ratio) effective_parallelism=($effective_parallelism)"

# Regression ceiling on the overhead ratio. This passes today for the workloads
# above (the in-process fan-out does not reproduce the severe slowdown); it
# guards against a regression in concurrent-sandbox efficiency.
const max_overhead_ratio = 2.0
assert ($overhead_ratio < $max_overhead_ratio) $"the overhead ratio \(($overhead_ratio)\) exceeded the maximum \(($max_overhead_ratio)\)"
