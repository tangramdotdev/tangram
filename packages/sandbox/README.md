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
		"cpu_oversubscription": 4,
		"memory_sampling_interval": 0.1
	}
}
```

The partition must have valid `root` or `isolated` status, matching effective and
exclusive CPU sets, no processes or child cgroups, and the `cpu`, `cpuset`,
`memory`, and `pids` controllers enabled for children. The host must keep the
partition exclusive to this runner throughout its lifetime.

The partition must contain every SMT sibling of each physical core. All cores
start available for dedicated allocation; no fixed shared/dedicated split is
configured. An allocation exposes one hardware thread per physical core while
excluding all siblings of dedicated cores from other sandboxes.

`cpu_oversubscription` defaults to 4 and must be a positive integer. Each core
can serve at most that many shared sandbox CPU allocations. Shared requests use
distinct physical cores, preserving their requested parallelism. The allocator
packs shared requests, moves them by updating their cgroup CPU sets, and frees
whole cores for dedicated requests. Existing placements are retained where
possible, and dedicated requests prefer idle cores. Live CPU-set updates keep
sandboxes runnable while shared workloads move; dedicated cores remain in each
sandbox's CPU set throughout the transition. Dedicated cores return to shared
eligibility when released after their cgroups empty. Mixed accounting monitors every potential shared core, so
CPU-set changes preserve execution accounting. Failed recovery disables pool
admission and causes runner heartbeats to advertise zero capacity until restart.

Runner capacity uses `cpu: { dedicated, shared }`: free exclusive cores and free
slots on currently shared cores. These are convertible resources. For example,
four idle cores advertise `{ dedicated: 4, shared: 0 }`; admitting one shared CPU
at factor 4 leaves `{ dedicated: 3, shared: 3 }`. Scheduler reservations account
for that conversion, mixed requests, memory, and the distinct-core requirement.
Children can borrow their parents' reservations without consuming additional
runner capacity. Each dedicated parent core can supply one dedicated child CPU
or up to `cpu_oversubscription` shared child slots. Shared parent slots can supply
only shared child slots. A borrowed child receives its requested capacity and
cannot lend more to its own children. CPU leases keep ancestor reservations
alive until their borrowers finish, and borrowed cgroups follow ancestor CPU
placement changes. Borrowing can place multiple shared slots on one physical
core; these slots do not promise simultaneous execution on distinct cores.

Sandbox prewarming is disabled for exclusive pools because unclaimed sandboxes would consume physical slots outside admission.
Without an exclusive pool, `runner.cpus` selects the shared CPU base capacity, and the factor determines the shared admission limit.

This reserves CPU scheduling capacity. Frequency scaling, thermal limits, memory
bandwidth, and interrupts can still affect performance.

On macOS, shared accounting uses process and child user/system CPU counters and
sampled resident memory. Process enumeration can race with exits or reparenting,
and resident memory can count shared pages more than once. Explicit CPU/memory
limits and dedicated pools remain unsupported with Seatbelt isolation.
