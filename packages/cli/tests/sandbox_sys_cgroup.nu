use lib/test.nu *

# Common setup exposes the sandbox cgroup independently of the builtin launcher.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
let uid = ^id -u | str trim | into int
let gid = ^id -g | str trim | into int
let tangram = which tangram | first | get path

for readonly in [false true] {
    let inner = $env.PWD | path join $'inner-($readonly).json'
    {
        cgroup: { name: $'tangram-test-(random uuid)' },
        gid: $gid,
        process: { arguments: [/bin/true] },
        uid: $uid,
    } | insert kind builtin | wrap backend | to json | save $inner
    let outer = $env.PWD | path join $'outer-($readonly).json'
    {
        cgroup: { name: $'tangram-test-(random uuid)' },
        cgroup_readonly: $readonly,
        gid: $gid,
        mounts: { tmpfs: [/sys] },
        process: { arguments: [$tangram sandbox run --config $inner] },
        uid: $uid,
    } | insert kind builtin | wrap backend | to json | save $outer
    let output = ^tangram sandbox run --config $outer | complete
    if $readonly {
        failure $output 'a nested sandbox should not create a cgroup through a read-only mount'
    } else {
        success $output 'a nested sandbox should create its cgroup through the common mount'
    }
}
