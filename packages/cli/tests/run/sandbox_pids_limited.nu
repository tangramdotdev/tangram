use ../lib/test.nu *

# A container's process limit makes forking beyond the limit fail inside the container.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
if (which python3 | is-empty) {
	skip_test 'this test requires python3'
}

container_cgroup_parent [pids] | ignore
let python = which python3 | where type == external | get path | first

# Fork paused children until a fork fails, then report how many succeeded and why the next one failed.
let script = '
import errno, os
count = 0
while count < 64:
    try:
        pid = os.fork()
    except OSError as error:
        print(count, errno.errorcode[error.errno])
        break
    if pid == 0:
        os.pause()
    count += 1
else:
    print(count, "none")
'
let cgroup = $'tangram-test-(random uuid)'
{ backend: { kind: builtin }, command: { args: [$python -c $script] }, options: { cgroup: { name: $cgroup, pids: 32 } } } | to json | save setup.json
let output = ^timeout 30s tangram sandbox run --config setup.json | complete
success $output
let result = $output.stdout | str trim | split row ' '
assert equal $result.1 'EAGAIN' 'a fork beyond the process limit should fail'
assert (($result.0 | into int) < 32) 'the container should not exceed its process limit'
