use ../../util/test.nu *

# A host-networked sandbox whose /sys is a tmpfs starts, as does a sandbox nested inside it.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
let uid = ^id -u | str trim | into int
let gid = ^id -g | str trim | into int
let tangram = which tangram | first | get path
let program = which 'true' | first | get path
let inner = $env.PWD | path join 'inner.json'
{
	backend: { kind: builtin, gid: $gid, uid: $uid },
	command: { args: [$program] },
} | to json | save $inner
let outer = $env.PWD | path join 'outer.json'
{
	backend: { kind: builtin, gid: $gid, uid: $uid },
	command: { args: [$tangram sandbox run --config $inner] },
	options: { mounts: { tmpfs: [/sys] }, network: { kind: host } },
} | to json | save $outer
let output = ^tangram sandbox run --config $outer | complete
success $output 'a sandbox nested in a host-networked sandbox should start'
