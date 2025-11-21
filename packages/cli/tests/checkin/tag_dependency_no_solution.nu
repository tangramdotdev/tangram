use ../../test.nu *

let server = spawn

# Tag the dependencies.
let c1_path = artifact {
	tangram.ts: ''
}
run tg tag c/1.0.0 $c1_path

let c2_path = artifact {
	tangram.ts: ''
}
run tg tag c/2.0.0 $c2_path

let a_path = artifact {
	tangram.ts: '
		import * as c from "c/^1"
	'
}
run tg tag a/1.0.0 $a_path

let b_path = artifact {
	tangram.ts: '
		import * as c from "c/^2"
	'
}
run tg tag b/1.0.0 $b_path

let path = artifact {
	tangram.ts: '
		import * as a from "a/*";
		import * as b from "b/*";
	'
}

let output = tg checkin $path | complete
failure $output "the checkin should fail when no solution exists"

let stdout = $output.stdout | str replace -a $path ''
let stderr = $output.stderr | str replace -a $path ''

snapshot -n stderr $stderr
snapshot -n stdout $stdout
