use ../../test.nu *
use std assert

let server = spawn

# Create and tag dependencies.
let foo_1_0_0_path = artifact {
	tangram.ts: '// foo 1.0.0'
}
let output = tg tag foo/1.0.0 $foo_1_0_0_path | complete
success $output

let foo_1_1_0_path = artifact {
	tangram.ts: '// foo 1.1.0'
}
let output = tg tag foo/1.1.0 $foo_1_1_0_path | complete
success $output

let bar_path = artifact {
	tangram.ts: 'import * as foo from "foo/^1"'
}
let output = tg tag bar $bar_path | complete
success $output

# Create the main artifact.
let path = artifact {
	tangram.ts: '
		import * as foo from "foo/=1.0.0;
		import * as bar from "bar";
	'
}

# Checkin.
let first_id = tg checkin $path | complete | get stdout | str trim

# Checkout.
let temp_dir = mktemp -d
let checkout_path1 = $temp_dir | path join "checkout1"
let output = tg checkout $first_id $checkout_path1 | complete
success $output

# Clean.
let output = tg tag delete foo/1.0.0 | complete
success $output
let output = tg tag delete foo/1.1.0 | complete
success $output
let output = tg tag delete bar | complete
success $output
let output = tg clean | complete
success $output

# Checkin again.
let second_id = tg checkin $checkout_path1 | complete | get stdout | str trim

# Checkout again.
let checkout_path2 = $temp_dir | path join "checkout2"
let output = tg checkout $second_id $checkout_path2 | complete
success $output

# Verify IDs match.
assert equal $first_id $second_id
