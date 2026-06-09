use ../../test.nu *

# Checking in a package with conflicting tag version constraints that have no solution fails with the expected output.

let server = spawn

# Tag the dependencies.
let c1_path = artifact {
	tangram.ts: ''
}
tg tag c/1.0.0 $c1_path

let c2_path = artifact {
	tangram.ts: ''
}
tg tag c/2.0.0 $c2_path

let a_path = artifact {
	tangram.ts: '
		import * as c from "c/^1"
	'
}
tg tag a/1.0.0 $a_path

let b_path = artifact {
	tangram.ts: '
		import * as c from "c/^2"
	'
}
tg tag b/1.0.0 $b_path

let path = artifact {
	tangram.ts: '
		import * as a from "a/*";
		import * as b from "b/*";
	'
}

let output = tg checkin $path | complete
failure $output "the checkin should fail when no solution exists"

let stdout = $output.stdout | redact $path | normalize_ids
let stderr = $output.stderr | redact $path | normalize_ids

snapshot --name stderr $stderr
snapshot --name stdout $stdout
