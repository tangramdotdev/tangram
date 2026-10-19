# Sandbox resources

Each sandbox has a minimum and maximum for CPU, memory, local storage, and swap.
The minimum reserves backed runner capacity for the sandbox's lifetime. The maximum
limits consumption, and an omitted maximum means no limit. A scalar specifies a
maximum with a zero minimum, and an object containing only a minimum has no maximum.

The scheduler fills in the bounds that a sandbox omits from `scheduler.default_cpu`,
`scheduler.default_memory`, `scheduler.default_storage`, and `scheduler.default_swap`,
raising a default maximum to meet a larger minimum. These defaults are unset, so a
sandbox can use every resource on the runner that other sandboxes have not reserved.
Each sandbox also reserves one unit of the runner's sandbox capacity,
`runner.sandboxes`, which defaults to the runner's number of hardware threads. A
parent waiting for a child lends its reserved capacity, including its sandbox, to the
child.

Admission charges a minimum in full and the rest of a maximum divided by the
resource's overcommit factor. A sandbox without a maximum is charged its minimum.

```sh
tg sandbox create --cpu 1 --memory 1GiB --storage 10GiB --swap 2GiB
tg sandbox create --cpu-min 0.25 --cpu-max 1 --memory-min 256MiB --memory-max 1GiB
```

```ts
await using sandbox = await tg.Sandbox.create()
    .cpu({ min: 0.25, max: 1 })
    .memory({ min: 256 * 1024 ** 2, max: 1024 ** 3 })
    .storage({ min: 1024 ** 3, max: 10 * 1024 ** 3 })
    .swap({ max: 2 * 1024 ** 3 });
await sandbox.run`echo hello`;
```

CPU units are hardware threads, with precision of 0.001 CPU. A sandbox with a CPU
maximum is pinned with `sched_setaffinity` to that many of the hardware threads that
the fewest sandboxes are pinned to, so `nproc` reports its maximum, and `cpu.max` limits its execution time. The runner sets each
sandbox's `cpu.weight` to its share of its minimum, and divides the remaining weight
among the sandboxes in proportion to their capacity above their minima, so that each
sandbox receives its minimum when the CPUs are contended.

Memory minima reserve capacity in scheduler admission and set `memory.min`, which
the kernel honors only as far as the runner's ancestor cgroups protect memory;
maxima set `memory.max`.
Above the minimum, memory can be reclaimed, swapped, or subject to an OOM kill.
Swap is a separate resource, limited with `memory.swap.max`. Local storage uses
ext4 or XFS project quotas, and includes the writable
overlay, scratch space, outputs, and temporary files. Linux needs delegated CPU,
memory, and pids controllers;
filesystem quota enforcement needs enabled quotas and CAP_SYS_ADMIN. Storage limits
are disabled by default;
set `runner.backend.storage_limits` to `true` to enforce requested storage bounds.
macOS neither applies nor enforces resource bounds. VM isolation supports whole CPU maxima
and memory maxima, and rejects the other bounds.

Storage usage tracking and limits require XFS or ext4 with project quotas enabled.
Other filesystems can be used with storage limits disabled.

```json
{
  "runner": {
    "overcommit": { "cpu": 4, "memory": 2, "storage": 2, "swap": 2 },
    "memory": 68719476736,
    "storage": 1099511627776,
    "swap": 68719476736
  }
}
```

Overcommit factors are positive integers. Only capacity above minima is
overcommitted. Capacity admission charges `min * factor + (max - min)` against
`backed capacity * factor`. Runner budgets cannot exceed available host backing.
Without explicit budgets, RAM and disk use 75% of available backing and swap uses
its available capacity. Disk and swap pools assign kernel limits within their
backed budgets; capacity above minima is best effort and may be unavailable even
before a sandbox reaches its requested maximum. Persistent data prevents a new
minimum from being admitted until enough backing becomes available. Sandbox
prewarming is disabled so allocation happens after capacity admission.

Usage has a `{ reservation, consumption }` counter for each resource. Reservation
accrues as `min * duration`, including idle time. Consumption accrues above the
minimum in each sampling interval; idle periods do not offset later bursts.
CPU consumption settles over one-second bandwidth windows matching the CPU quota
period, with the final partial window settled when the sandbox is destroyed.
Short bursts retain their actual execution time, even if their average over that
partial window exceeds the configured maximum. Equal CPU bounds accrue only a
reservation.
CPU counters use hardware thread-milliseconds. Memory, storage, and swap use
MiB-milliseconds. Memory and swap come from cgroup counters, disk from project
quota space usage, and CPU from total execution time in `cpu.stat`. No disk I/O
byte counters are billed. Aggregate indexes expose the eight counters as
`sandbox_{resource}_{reservation|consumption}`. Both counters are needed to
calculate charges; consumption already excludes the minimum.
