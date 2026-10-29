use ../../util/test.nu *

# Every runner backend supports the same sandbox lifecycle and workload API.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
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
    let backend = $backend | merge { max_open_files: 512 }
    let backend = if $kind in [builtin bubblewrap] { $backend | merge { seccomp: default } } else { $backend }
    let directory = mktemp --directory
    let local = server spawn --directory $directory --config { runner: { backend: $backend } }
    let path = artifact {
        tangram.ts: '
            export default async function () {
                await using sandbox = await tg.Sandbox.create().cpu(1).network(true);
                await sandbox.run`sh -c "test $(ulimit -n) -eq 512; i=0; while [ $i -lt 10000 ]; do i=$((i+1)); done"`;
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
        backend: { kind: gvisor, uid_map: { host: 100000, count: 65536 }, gid_map: { host: 100000, count: 65536 } },
    },
} | to json | save invalid.json
let output = tg -c invalid.json --directory invalid server run | complete
failure $output
assert ($output.stderr | str contains 'unknown field')
