use ../../test.nu *

# Checking in a package that depends on a nonexistent tag fails with the expected output.

let server = spawn

let path = artifact {
	tangram.ts: '
		import * as a from "a/^1.2";
	'
}

let output = tg checkin $path | complete
failure $output "the checkin should fail when the tag does not exist"
let stdout = $output.stdout | redact $path
let stderr = $output.stderr | redact $path
snapshot --name stderr $stderr
snapshot --name stdout $stdout
