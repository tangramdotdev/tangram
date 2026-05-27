use ../../test.nu *

# Reproduces the per-process overhead blow-up that a cold proxied cargo build
# hits: when many sandboxed processes are spawned concurrently and oversubscribe
# the runner concurrency limit, the wall time grows far beyond the
# contention-free ideal (waves * per-process duration). The sequential
# sandbox_spawn_overhead.nu test does not reproduce this because it never runs
# two processes at once, and trivial `true` children do not reproduce it either
# because they neither check inputs out into the sandbox nor live long enough to
# heartbeat. Each child here mounts the busybox artifact (real input checkout,
# like rustc mounting a toolchain) and sleeps (so the sandbox stays alive across
# heartbeat intervals), which is what a real rustc process does.
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
# busybox package so children have a real `sleep`.
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
# sandboxed children concurrently and awaits them all. Each child mounts the
# busybox artifact as its environment (forcing an input checkout into the
# sandbox), sleeps for the requested duration, and runs `true <i>` whose unique
# argument keeps each process from being a cache hit.
let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async (arg: string) => {
			const { count, sleep, outputKb } = JSON.parse(arg);
			const env = tg.build(busybox);
			const children = [];
			for (let i = 0; i < count; i++) {
				// Each child sleeps (to stay alive across heartbeat intervals),
				// writes outputKb kilobytes to its output (to exercise output
				// checkin into the object store), and runs `true ${i}` to keep
				// its process unique. busybox provides sleep and dd on the PATH.
				children.push(
					tg.run`sleep ${sleep}; dd if=/dev/zero of=${tg.output} bs=1024 count=${outputKb} 2>/dev/null; true ${i.toString()}`
						.env(env)
						.sandbox(),
				);
			}
			await Promise.all(children);
			return "done";
		};
	',
}

# Measure the fixed overhead of the outer process with no children (this also
# warms the busybox build so it is not counted in the fan-out timing).
let baseline_arg = { count: 0, sleep: $sleep_secs, outputKb: $output_kb } | to json
let baseline_start = date now
let baseline_output = tg run $path -a $baseline_arg | complete
success $baseline_output "the baseline run should succeed"
let baseline = (date now) - $baseline_start

# Measure the time for a single child. Subtracting the baseline gives the
# uncontended serial cost of one child (sleep + output checkin + sandbox
# setup/teardown), which is the unit of work the fan-out should parallelize.
let single_arg = { count: 1, sleep: $sleep_secs, outputKb: $output_kb } | to json
let single_start = date now
let single_output = tg run $path -a $single_arg | complete
success $single_output "the single-child run should succeed"
let serial_one = (date now) - $single_start - $baseline

# Measure the time to fan out all of the children concurrently.
let total_arg = { count: $count, sleep: $sleep_secs, outputKb: $output_kb } | to json
let total_start = date now
let total_output = tg run $path -a $total_arg | complete
success $total_output "the fan-out run should succeed"
let total = (date now) - $total_start

# The net time spent on the children.
let net = $total - $baseline

# A contention-free fan-out runs ceil(count / concurrency) waves, each taking
# about the serial cost of one child, so the ideal net is waves * serial_one.
# The overhead ratio is how much slower the real fan-out is than that ideal,
# and the effective parallelism is how many children were truly in flight at
# once. With the slowdown present, overhead_ratio is well above 1 and effective
# parallelism is well below the concurrency limit.
let waves = ($count + $concurrency - 1) // $concurrency
let ideal_net = $waves * $serial_one
let overhead_ratio = ($net / $ideal_net)
let effective_parallelism = ($count * $serial_one) / $net

print -e $"concurrency=($concurrency) count=($count) sleep=($sleep_secs)s output_kb=($output_kb) single_threaded=($single_threaded) disable_heartbeat=($disable_heartbeat)"
print -e $"baseline=($baseline) serial_one=($serial_one) total=($total) net=($net)"
print -e $"waves=($waves) ideal_net=($ideal_net) overhead_ratio=($overhead_ratio) effective_parallelism=($effective_parallelism)"

# Regression ceiling on the overhead ratio. With the slowdown present this is
# expected to fail, demonstrating the issue; once the contention is fixed it
# guards against it.
const max_overhead_ratio = 2.0
assert ($overhead_ratio < $max_overhead_ratio) $"the overhead ratio \(($overhead_ratio)\) exceeded the maximum \(($max_overhead_ratio)\)"
