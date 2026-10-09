use ../lib/test.nu *

# A container applies its open-file resource limit before executing the command.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let output = ^tangram sandbox container run --index 0 --unshare-all --uid 0 --gid 0 --chdir / --rlimit-nofile 32 -- /bin/sh -c 'test "$(ulimit -n)" = 32' | complete
success $output
