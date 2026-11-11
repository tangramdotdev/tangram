use ../../util/test.nu *

# Interrupting the test wrapper stops a test that runs in a Tangram sandbox before the wrapper exits.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

const repository_path = path self ../../../..
let server = server spawn

# Give the wrapper a tangram that uses the test server, because the wrapper removes the inherited Tangram variables from the client's environment.
let directory = mktemp -d
let tangram = $directory | path join 'tangram'
$"#!/bin/sh\nexec '(which tangram | get 0.path)' --mode client --url '($server.url)' \"$@\"\n" | save $tangram
chmod +x $tangram

# Run a test that updates a marker in the temporary directory that the wrapper preserves, from the repository, which the sandbox mounts.
let temp_path = $directory | path join 'temp'
mkdir $temp_path
let script = 'i=0; while true; do i=$((i+1)); echo $i 1<> "$TMPDIR/marker"; sleep 0.1; done'
let job = job spawn {
	let job_id = job id
	cd $repository_path
	let output = with-env { TANGRAM_TEST_PRESERVE_TEMPS: '1', TANGRAM_TEST_SANDBOX: $tangram, TMPDIR: $temp_path } {
		tangram_tests_runner wrap -- sh -c $script | complete
	}
	$output | job send --tag $job_id 0
}
let pattern = $temp_path | path join 'tangram_test_*/marker'
wait_until --timeout 30sec { glob $pattern | is-not-empty } 'the sandboxed test should start'
let marker = glob $pattern | first

# Send SIGTERM to the wrapper's process group, as nextest does to end a test, and wait for the wrapper to exit.
let pid = job list | where id == $job | get 0.pids.0
^kill --signal TERM -- $'-($pid)'
job recv --tag $job --timeout 30sec | ignore

# Check that the sandboxed test no longer updates the marker.
let previous = open --raw $marker
sleep 1sec
assert equal (open --raw $marker) $previous 'the sandboxed test should stop before the wrapper exits'
