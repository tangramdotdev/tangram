use ../lib/test.nu *

# A container applies its open-file resource limit before executing the command.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

{ rlimit_nofile: 32, process: { arguments: [/bin/sh -c 'test "$(ulimit -n)" = 32'] } } | insert kind builtin | wrap backend | to json | save setup.json
let output = ^tangram sandbox run --config setup.json | complete
success $output
