use ../lib/test.nu *

# An idle sandbox is charged for actual memory but does not accrue allocated CPU-time.
if $nu.os-info.name != 'linux' {
 skip_test 'this test requires linux cgroup accounting'
}

let local = server spawn --config {
 runner: { memory_sampling_interval: 0.05 },
}
let sandbox = tg sandbox create --no-tokens --no-ttl --cpu 2 --memory 268435456 | referent node
sleep 2sec
tg sandbox destroy $sandbox
let data = tg sandbox get $sandbox | from json | get data
assert ($data.usage.cpu.shared < 1000) 'an idle sandbox must not accrue allocated CPU-time'
assert equal $data.usage.cpu.dedicated 0
assert ($data.usage.memory > 0) 'actual sandbox memory must be charged'
assert ($data.usage.memory < 512000) 'the memory limit must not be charged as actual usage'

# Workload CPU remains accounted after the process has exited.
let path = artifact {
 tangram.ts: '
  export default async function () {
   await using sandbox = await tg.Sandbox.create().cpu({ shared: 1 });
   await sandbox.run`sh -c "i=0; while [ $i -lt 100000 ]; do i=$((i+1)); done"`;
   console.log(sandbox.id);
  }
 ',
}
let output = tg exec $path | complete
success $output
let sandbox = $output.stdout | str trim
tg sandbox wait $sandbox | ignore
let data = tg sandbox get $sandbox | from json | get data
assert ($data.usage.cpu.shared > 0) 'exited workloads must retain their CPU usage'

# Sampling intervals must be positive.
{ runner: { memory_sampling_interval: 0 } } | to json | save invalid.json
let output = tg -c invalid.json --directory invalid server run | complete
failure $output
assert ($output.stderr | str contains 'the memory sampling interval must be greater than zero')
