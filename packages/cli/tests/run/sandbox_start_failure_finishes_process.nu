use ../../test.nu *

# When a sandbox fails to start because of an invalid mount, the process still finishes with a nonzero exit and an error rather than hanging, and the sandbox is eventually destroyed.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let server = spawn

let mount = mktemp --directory
let path = artifact {
	tangram.ts: '
		export default async function () {
			return "unreachable";
		}
	',
}

let process = tg process spawn --sandbox --mount $"($mount):/etc/passwd,ro" $path | str trim

let output = timeout 10s tg process wait $process | complete
success $output "the process wait should not time out"

let wait = $output.stdout | from json
assert ($wait.exit == 1)
assert ($wait.error != null)

let state = tg process get $process | from json
assert ($state.status == "finished")

let sandbox = $state.sandbox
wait_until {
	let output = tg sandbox get $sandbox | complete
	$output.exit_code != 0 or (($output.stdout | from json | get status) == "destroyed")
} "the sandbox should be destroyed"
