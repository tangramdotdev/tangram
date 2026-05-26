use ../../test.nu *

# This test measures the per-process overhead of the default sandbox. Each
# process that the server runs is placed in its own sandbox, so workloads that
# spawn many short-lived processes, such as compiling a Rust crate graph with
# tgrustc, pay this overhead once per process. The test guards against
# regressions in that overhead.
#
# Note that this measures only the cost of creating a sandbox and spawning a
# trivial process. It does not measure the cost of checking a process's input
# artifacts out into the sandbox or checking its outputs back in, which a real
# workload such as rustc also pays per process.

# The number of trivial sandboxed processes to spawn sequentially.
const count = 20

# The maximum acceptable per-process sandbox overhead. This is a regression
# ceiling. The warm per-process overhead is tens of milliseconds in practice,
# so this leaves ample margin for slower machines and cold caches while still
# catching a meaningful regression. Tighten it as the overhead improves.
const max_per_process = 500ms

let server = spawn

# A package whose default export spawns the requested number of trivial
# sandboxed processes sequentially. Each process runs `true`, which does no
# work, so the wall clock time is dominated by the per-process sandbox
# overhead.
let path = artifact {
	tangram.ts: '
		export default async (count: string) => {
			const n = Number(count);
			for (let i = 0; i < n; i++) {
				await tg.run`true`.sandbox();
			}
		};
	',
}

# Measure the fixed overhead of running the package with no inner processes.
# This includes spawning the outer process, creating its sandbox, and starting
# the JavaScript runtime.
let baseline_start = date now
let baseline_output = tg run $path -a '0' | complete
success $baseline_output "the baseline run should succeed"
let baseline = (date now) - $baseline_start

# Measure the time to run the package with the inner processes.
let total_start = date now
let total_output = tg run $path -a $"($count)" | complete
success $total_output "the run should succeed"
let total = (date now) - $total_start

# Compute the per-process overhead by subtracting the fixed overhead and
# dividing by the number of processes.
let per_process = ($total - $baseline) / $count

print -e $"sandbox spawn overhead: ($per_process) per process \(count = ($count), baseline = ($baseline), total = ($total)\)"

assert ($per_process < $max_per_process) $"the per-process sandbox overhead \(($per_process)\) exceeded the maximum \(($max_per_process)\)"
