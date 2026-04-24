use ../../test.nu *

if $nu.os-info.name != 'linux' {
	return
}

let server = spawn

let missing = mktemp -d | path join 'missing'
let path = artifact {
	tangram.ts: '
		export default async function () {
			return "unreachable";
		}
	',
}

let process = tg process spawn --sandbox --mount $"($missing):/missing,ro" $path | str trim

let output = timeout 10s tg process wait $process | complete
success $output "the process wait should not time out"

let wait = $output.stdout | from json
assert ($wait.exit == 1)
assert (($wait.error | to json) | str contains "failed to create the sandbox")

let state = tg process get $process | from json
assert ($state.status == "finished")

let sandbox = $state.sandbox
mut output = { exit_code: 0, stdout: '', stderr: '' }
for _ in 0..100 {
	$output = (tg sandbox get $sandbox | complete)
	if $output.exit_code != 0 {
		break
	}
	let state = $output.stdout | from json
	if $state.status == "finished" {
		break
	}
	sleep 0.05sec
}
if $output.exit_code == 0 {
	let state = $output.stdout | from json
	assert ($state.status == "finished")
}
