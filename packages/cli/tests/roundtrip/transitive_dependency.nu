use ../../test.nu *

let server = spawn

# Create and tag dependencies.
let foo_1_0_0_path = artifact {
	tangram.ts: '// foo 1.0.0'
}
run tg tag foo/1.0.0 $foo_1_0_0_path

let foo_1_1_0_path = artifact {
	tangram.ts: '// foo 1.1.0'
}
run tg tag foo/1.1.0 $foo_1_1_0_path

let bar_path = artifact {
	tangram.ts: 'import * as foo from "foo/^1"'
}
run tg tag bar $bar_path

# Create the main artifact.
let path = artifact {
	tangram.ts: '
		import * as foo from "foo/=1.0.0;
		import * as bar from "bar";
	'
}

# Checkin.
let first_id = run tg checkin $path

# Checkout.
let temp_dir = mktemp -d
let checkout_path1 = $temp_dir | path join "checkout1"
run tg checkout $first_id $checkout_path1

# Clean.
run tg tag delete foo/1.0.0
run tg tag delete foo/1.1.0
run tg tag delete bar
run tg clean

# Checkin again.
let second_id = run tg checkin $checkout_path1

# Checkout again.
let checkout_path2 = $temp_dir | path join "checkout2"
run tg checkout $second_id $checkout_path2

# Verify IDs match.
assert equal $first_id $second_id
