use ../lib/test.nu *

# Every runner backend supports the same sandbox lifecycle and workload API.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
let quota_root = $env.TANGRAM_TEST_PROJECT_QUOTA_PATH? | default ''
if ($quota_root | is-empty) { skip_test 'TANGRAM_TEST_PROJECT_QUOTA_PATH is required for sandbox storage accounting' }
for kind in [builtin bubblewrap gvisor-systrap gvisor-kvm] {
    let backend = match $kind {
        builtin => { kind: builtin },
        bubblewrap => {
            if (which bwrap | is-empty) { continue }
            { kind: bubblewrap, path: (which bwrap | first | get path) }
        },
        _ => {
            if (which runsc | is-empty) { continue }
            let platform = if $kind == gvisor-kvm { 'kvm' } else { 'systrap' }
            if $platform == kvm and ((^test -r /dev/kvm -a -w /dev/kvm | complete).exit_code != 0) { continue }
            { kind: gvisor, path: (which runsc | first | get path), platform: $platform }
        },
    }
    let directory = mktemp --directory --tmpdir-path $quota_root
    let local = server spawn --directory $directory --config { runner: { backend: $backend } }
    let path = artifact {
        tangram.ts: '
            export default async function () {
                await using sandbox = await tg.Sandbox.create().cpu(1).network(true);
                await sandbox.run`sh -c "i=0; while [ $i -lt 10000 ]; do i=$((i+1)); done"`;
                console.log(sandbox.id);
            }
        ',
    }
    let output = tg exec $path | complete
    success $output $"($kind): the sandbox workload should succeed"
    let sandbox = $output.stdout | str trim
    tg sandbox wait $sandbox | ignore
    let data = tg sandbox get $sandbox | from json | get data
    assert equal $data.status destroyed
    assert ($data.usage.cpu.consumption > 0) $"($kind): the workload CPU should be accounted"
}

# External launchers reject subordinate ranges before starting a runner.
{
    runner: {
        backend: { kind: gvisor },
        resources: { uid_map: { host: 100000, count: 65536 }, gid_map: { host: 100000, count: 65536 } },
    },
} | to json | save invalid.json
let output = tg -c invalid.json --directory invalid server run | complete
failure $output
assert ($output.stderr | str contains 'subordinate identity maps require the builtin backend')
