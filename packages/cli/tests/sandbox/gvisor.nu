use ../lib/test.nu *

# gVisor receives its own OCI bundle and native runtime arguments.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
if (which runsc | is-empty) { skip_test 'runsc is not installed' }
let runtime = which runsc | first | get path
for platform in [systrap kvm] {
    if $platform == kvm and ((^test -r /dev/kvm -a -w /dev/kvm | complete).exit_code != 0) { continue }
    let bundle = mktemp --directory
    let root = $bundle | path join root
    mkdir $root
    let mounts = [usr bin lib lib64] | each { |name|
        { destination: $'/($name)', type: bind, source: ($'/($name)' | path expand), options: [rbind ro] }
    }
    {
        ociVersion: '1.2.0',
        root: { path: $root, readonly: false },
        process: {
            args: [/bin/sh -ec 'read input; test "$input" = provided; echo native; echo written > /result'],
            cwd: '/', env: ['PATH=/usr/bin:/bin'], user: { uid: 0, gid: 0 }, noNewPrivileges: true,
        },
        mounts: $mounts,
        linux: { uidMappings: [{ containerID: 0, hostID: 0, size: 1 }], gidMappings: [{ containerID: 0, hostID: 0, size: 1 }], namespaces: [{ type: user } { type: mount } { type: pid } { type: ipc } { type: uts }] },
    } | to json | save ($bundle | path join config.json)
    let state = $bundle | path join state
    let output = "provided\n" | ^unshare --user --map-root-user --mount --pid --fork --mount-proc tangram sandbox run --backend gvisor --runtime-path $runtime -- $'--platform=($platform)' --rootless=false --network=none --ignore-cgroups=true --overlay2=none --root $state run --bundle $bundle sandbox | complete
    success $output $"($platform): native runsc arguments should launch the OCI workload"
    assert equal ($output.stdout | str trim) native
    assert equal (open --raw ($root | path join result) | str trim) written
}
