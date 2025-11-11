use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': '
		import * as a from "a/^1.2";
	'
}

let output = tg checkin $path | complete
assert ($output.exit_code != 0) 'should fail when tag does not exist'

let stdout = $output.stdout | str replace -a $path ''
let stderr = $output.stderr | str replace -a $path ''

assert (snapshot -n stderr $stderr)
assert (snapshot -n stdout $stdout)
