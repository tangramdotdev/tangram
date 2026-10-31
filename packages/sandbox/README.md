# Sandbox resources

Each sandbox has a minimum and maximum for CPU, memory, and local storage.
The minimum reserves backed runner capacity for the sandbox's lifetime. The maximum
limits consumption, and an omitted maximum means no limit. A scalar specifies a
maximum with a zero minimum, and an object containing only a minimum has no maximum.

The scheduler fills in the bounds that a sandbox omits from `scheduler.default_cpu`,
`scheduler.default_memory`, and `scheduler.default_storage`, raising a default maximum to meet a larger minimum. These defaults are unset, so a
sandbox can use every resource on the runner that other sandboxes have not reserved.
Each sandbox also reserves one unit of the runner's sandbox capacity,
`runner.sandboxes`, which defaults to the runner's number of hardware threads. A
parent waiting for a child lends its reserved capacity, including its sandbox, to the
child.

Admission charges a minimum in full and the rest of a maximum divided by the
resource's overcommit factor. A sandbox without a maximum is charged its minimum.

```sh
tg sandbox create --cpu 1 --memory 1GiB --storage 10GiB
tg sandbox create --cpu-min 0.25 --cpu-max 1 --memory-min 256MiB --memory-max 1GiB
```

```ts
await using sandbox = await tg.Sandbox.create()
    .cpu({ min: 0.25, max: 1 })
    .memory({ min: 256 * 1024 ** 2, max: 1024 ** 3 })
    .storage({ min: 1024 ** 3, max: 10 * 1024 ** 3 });
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
Above the minimum, memory can be reclaimed or subject to an OOM kill. Sandboxes do
not swap: the runner sets each sandbox's `memory.swap.max` to zero, so a sandbox that
exceeds its memory maximum is killed instead of slowed. Local storage uses
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
    "overcommit": { "cpu": 4, "memory": 2, "storage": 2 },
    "memory": 68719476736,
    "storage": 1099511627776
  }
}
```

Overcommit factors are positive integers. Only capacity above minima is
overcommitted. Capacity admission charges `min * factor + (max - min)` against
`backed capacity * factor`. Runner budgets cannot exceed available host backing.
Without explicit budgets, RAM and disk use 75% of available backing. The disk pool
assigns kernel limits within its backed budget; capacity above minima is best effort and may be unavailable even
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
CPU counters use hardware thread-milliseconds. Memory and storage use
MiB-milliseconds. Memory comes from cgroup counters, disk from project
quota space usage, and CPU from total execution time in `cpu.stat`. No disk I/O
byte counters are billed. Aggregate indexes expose the six counters as
`sandbox_{resource}_{reservation|consumption}`. Both counters are needed to
calculate charges; consumption already excludes the minimum.

# Stopping and starting

`tg sandbox stop` stops a sandbox and keeps its files. Its running processes
finish with an error, it releases its CPU, memory and sandbox capacity, and a
spawn into it fails with "the sandbox is not started". `tg sandbox start`
starts it again on the same runner with the same files, and a process spawned
after `start` returns runs in it. Both are idempotent, and a stopped sandbox
can still be destroyed. `tg sandbox list --status stopped` lists the stopped
sandboxes.

A graceful server shutdown, such as `tangram server stop`, stops the server's
sandboxes rather than destroying them, and so does the expiration of a
runner's heartbeat. Usage accrues only while a sandbox runs, and the usage of
every start is summed.

A sandbox handle destroys its sandbox when it is disposed. To keep a stopped
sandbox beyond the life of its handle, call `detach()` on the handle:

```ts
const sandbox = await tg.Sandbox.create();
await sandbox.stop();
sandbox.detach();
```
