use ../../test.nu *

let server = spawn

# Create and tag the foo dependency.
let foo_path = artifact {
	tangram.ts: ''
}
run tg tag foo/1.0.0 $foo_path

# Create the main artifact.
let path = artifact {
	tangram.ts: 'import * as foo from "foo/*"'
}

# Checkin.
let first_id = run tg checkin $path

# Checkout.
let temp_dir = mktemp -d
let checkout_path1 = $temp_dir | path join "checkout1"
run tg checkout $first_id $checkout_path1

# Clean.
run tg tag delete foo/1.0.0
run tg clean

# Checkin again.
let second_id = run tg checkin $checkout_path1

# Checkout again.
let checkout_path2 = $temp_dir | path join "checkout2"
run tg checkout $second_id $checkout_path2

# Verify IDs match.
assert equal $first_id $second_id
