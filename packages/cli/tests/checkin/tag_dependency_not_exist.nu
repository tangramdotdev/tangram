use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import * as a from "a/^1.2";
	'
}

let output = tg checkin $path | complete
failure $output "the checkin should fail when the tag does not exist"
let stdout = $output.stdout | str replace -a $path ''
let stderr = $output.stderr | str replace -a $path ''
snapshot -n stderr $stderr
snapshot -n stdout $stdout
