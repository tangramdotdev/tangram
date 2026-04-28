use ../../test.nu *

if $nu.os-info.name != 'linux' {
	return
}

# When the server is killed ungracefully while sandboxed builds are running
# (the common case after Ctrl-C-C or a crash), the sandbox rows are left in
# `status='started'` and `process_finalize_queue` rows that were already
# claimed are stuck at `status='started'`. After restart in the same data
# directory there is no startup-time reset for either, so the stuck rows
# persist. The watchdog must clear the dangling sandboxes via heartbeat-TTL
# expiration; the finalize-queue rows are stuck forever (the dequeue path
# selects only `status='created'`).
#
# Today's symptom: `tg build` hung for ~24 minutes before being assigned a
# process id, and the eventually-assigned process expired with
# `heartbeat_expiration` ~1.5 minutes after spawn. The on-disk state had 89
# stuck `process_finalize_queue` rows and 11 sandboxes with stale
# heartbeats from a prior session, none of which the running server's
# watchdog had cleaned up.
#
# This test exercises the kill-and-restart path. With the bug we hit
# directly (finalize-queue rows stuck at `status='started'` after restart),
# this test still passes because nothing else reads them. The test does
# guard the user-facing invariant: a fresh build must not stall after a
# restart that left dangling sandbox state. If a future change re-introduces
# a permit / WAL deadlock in that path, this test catches it.

let dir = mktemp -d

let server1 = spawn --directory $dir --config {
	runner: {
		concurrency: 4,
	},
	watchdog: {
		ttl: 5,
		interval: 1,
	},
}

# Spawn several long-running sandboxed builds, detached.
let long = artifact {
	tangram.ts: '
		export default async (tag: string) => {
			await tg.run`sleep 120; echo ${tag}`.sandbox();
			return "done";
		};
	',
}
for i in 0..6 {
	tg build -dv $"($long)#default" -a $"iter-($i)" | from json | ignore
}

# Give the server time to move the sandboxes to the started state.
sleep 5sec

# SIGKILL every tangram process whose cmdline references this test's data
# directory. The bash wrapper used by spawn is a parent of `tangram serve`,
# so killing only the job's bash pid leaves an orphaned tangram holding the
# lock file.
do -i { ^pkill -KILL -f $"tangram .*-d ($dir) .*serve" }
do -i { ^pkill -KILL -f $"tangram .*-d ($dir)" }
do -i { ^pkill -KILL -f $"sandbox.*($dir)" }

# Reap the now-defunct server job.
for job in (job list | where { ($in.description? | default '') == 'server' }) {
	for pid in ($job.pids? | default []) {
		try { ^kill -KILL $pid }
	}
	try { job kill $job.id }
}

# Wait for the lock fd to actually close in the kernel.
sleep 3sec

# Second server in the same data directory.
let server2 = spawn --directory $dir --config {
	runner: {
		concurrency: 4,
	},
	watchdog: {
		ttl: 5,
		interval: 1,
	},
}

# A trivial build should be assigned a process id and complete promptly.
let tiny = artifact {
	tangram.ts: '
		export default () => "hello";
	',
}

let output = timeout 30s tg build $tiny | complete
success $output "build should not hang after server restart with dangling sandboxes"
