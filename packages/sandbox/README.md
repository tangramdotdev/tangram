# Sandbox resources and usage

CPU requests can combine shared and dedicated CPUs:

```typescript
await using sandbox = await tg.Sandbox.create()
	.cpu({ dedicated: 2, shared: 4 })
	.memory(1024 * 1024 * 1024);
```

`.cpu(2)` is shorthand for `.cpu({ shared: 2 })`. Counts must be nonnegative
integers, and their sum must be positive. Shared CPUs set an execution limit.
Dedicated CPUs reserve physical cores for the sandbox. The CLI accepts `--cpu` for shared CPUs and `--dedicated-cpu` for
dedicated CPUs, including both flags together.

`tangram_sandbox` measures usage; the runner schedules resources and forwards
the finalized measurements. The runner calls `start()` when it claims a sandbox.
`usage()` samples current cumulative usage without stopping accounting. `destroy()`
stops the sandbox and returns an `Output` containing finalized `usage`; later
`usage()` calls return the same totals. Destroying an unclaimed sandbox returns
zero usage. Sandbox usage contains:

- `cpu.shared`: actual shared CPU execution in milliseconds, including user and
  kernel execution. Sleeping and waiting for I/O do not accrue execution time.
- `cpu.dedicated`: assigned physical cores multiplied by elapsed milliseconds,
  from allocation until teardown, including idle time.
- `memory`: the integral of sampled memory consumption, in MiB-milliseconds.
  Adjacent samples are integrated using their actual elapsed time. The memory
  limit remains an enforcement and admission limit.

The usage index exposes shared CPU time as `sandbox_cpu`, dedicated core-time as
`sandbox_cpu_dedicated`, and sampled memory-time as `sandbox_memory`.

On Linux, each sandbox owns a cgroup. Accounting starts when a sandbox is claimed,
before workloads start, and finishes after all processes in its cgroup stop.
Prewarming execution and memory are excluded. Dedicated allocation time includes
startup. Helpers placed inside this cgroup are included. Separate sandboxes own
separate cgroups, regardless of how their creating processes are related.

Shared-only requests use `cpu.stat`. Mixed requests use cgroup-scoped perf CPU
clock events on the assigned shared CPUs, so execution on dedicated cores does
not also accrue shared CPU time. Mixed requests require permission to open these
events, typically `CAP_PERFMON`, and fail explicitly if it is unavailable.

Memory samples use `memory.current`, including memory the kernel charges to the
cgroup, such as page cache. Brief changes between samples can be missed. Configure
the sampling interval in seconds; the default is one second, and zero is rejected:

```json
{
	"runner": {
		"memory_sampling_interval": 0.1
	}
}
```

## Dedicated CPU configuration

Dedicated CPUs require a host-managed cgroup v2 cpuset partition. Configure an
empty, writable partition owned exclusively by this runner:

```json
{
	"runner": {
		"cpu_pool": "/sys/fs/cgroup/tangram-cpus",
		"dedicated_cpus": [2, 3],
		"memory_sampling_interval": 0.1
	}
}
```

The partition must have valid `root` or `isolated` status, matching effective and
exclusive CPU sets, no processes or child cgroups, and the `cpu`, `cpuset`,
`memory`, and `pids` controllers enabled for children. The host must keep the
partition exclusive to this runner throughout its lifetime.

`dedicated_cpus` lists one logical CPU representative for each physical core to
reserve. Every SMT sibling of those cores must be in the partition. All siblings
are removed from the shared pool, and each assigned dedicated core exposes one
hardware thread. The remaining logical CPUs form the shared pool. Capacity
advertisements and admission distinguish the two pools. Dedicated allocations
are never borrowed by another sandbox and are released after the cgroup empties.

This reserves CPU scheduling capacity. Frequency scaling, thermal limits, memory
bandwidth, and interrupts can still affect performance.

On macOS, shared accounting uses process and child user/system CPU counters and
sampled resident memory. Process enumeration can race with exits or reparenting,
and resident memory can count shared pages more than once. Explicit CPU/memory
limits and dedicated pools remain unsupported with Seatbelt isolation.
