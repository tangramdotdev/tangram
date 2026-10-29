use ../../lib/test.nu *

# Container teardown kills a surviving descendant before returning, so the descendant releases the inherited standard output.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let cgroup = $'tangram-test-(random uuid)'
let script = '/bin/sleep 60 &'
{ backend: { kind: builtin }, command: { args: [/bin/sh -c $script] }, options: { cgroup: { name: $cgroup } } } | to json | save setup.json

# Reading the output to EOF requires every holder of the standard output pipe, including the descendant, to exit.
let output = ^timeout 10s tangram sandbox run --config setup.json | complete
success $output 'container teardown should kill the surviving descendant'
