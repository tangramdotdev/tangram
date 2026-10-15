# Sandbox resources

Each sandbox has a minimum and maximum for CPU, memory, local storage, and swap.
The minimum reserves backed runner capacity for the sandbox's lifetime. The maximum
limits consumption. A scalar specifies a maximum with a zero minimum; an object
containing only a minimum uses that value for its maximum too.

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

CPU units are hardware threads, with precision of 0.001 CPU. Each sandbox stays
within a fixed CPU affinity domain containing enough threads for its maximum.
CPU minima require an empty delegated exclusive cpuset partition configured with
`runner.cpu_pool`; the pool includes every SMT sibling of its physical cores.
Fractional minima reserve execution time within that fixed domain. The allocator
accounts for reservations before distributing scheduler weight to other workloads.
A minimum in a domain wider than one thread can hold additional physical capacity
to preserve the promise for a single busy worker. Runner heartbeats include this
affinity padding; billing uses the requested minimum. Domains stay fixed, so
placement can fail because of fragmentation even when aggregate capacity remains.

Memory minima reserve capacity and set `memory.min`; maxima set `memory.max`.
Runner ancestor cgroups must provide enough memory protection for a minimum.
Admission checks the sum of sibling minima at each ancestor and rejects requests
when that protection is insufficient. An exclusive runner pool sets its
own memory protection and physical memory/swap budgets before accepting sandboxes.
Above the minimum, memory can be reclaimed, swapped, or subject to an OOM kill.
Swap is a separate resource, limited with `memory.swap.max`. Local storage uses
ext4 or XFS project quotas or Btrfs subvolume quotas, and includes the writable
overlay, scratch space, outputs, and temporary files. Linux needs delegated CPU,
cpuset, memory, and pids controllers; filesystem quota enforcement needs enabled
quotas and CAP_SYS_ADMIN. The default container maxima are one CPU, 1 GiB RAM,
unlimited local storage, and zero swap. Storage limits are disabled by default;
set `runner.backend.storage_limits` to `true` to enforce requested storage bounds.
macOS rejects explicit resource bounds. VM isolation supports whole CPU maxima
and memory maxima, and rejects the other bounds.

Btrfs requires consistent quotas and the quota counters exposed under
`/sys/fs/btrfs` on Linux 6.1 or later. Both qgroups and
simple quotas are supported. Sandbox project IDs identify level-one quota groups;
configure distinct `runner.resources.filesystem_project_ids` ranges for runners
sharing a filesystem. Empty level-one groups are retained for reuse within that
range; remove them with `btrfs qgroup destroy` when retiring a runner, after its
subvolumes and retained data have been deleted. Byte limits
include pending data and metadata allocations. Btrfs does not support inode limits,
so hardened isolation, which requires an inode limit, still requires ext4 or XFS.

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
