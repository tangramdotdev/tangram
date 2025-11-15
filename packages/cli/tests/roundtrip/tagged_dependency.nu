use ../../test.nu *
use std assert

let server = spawn

# Create and tag the foo dependency.
let foo_path = artifact {
	tangram.ts: ''
}
let output = tg tag foo/1.0.0 $foo_path | complete
success $output

# Create the main artifact.
let path = artifact {
	tangram.ts: 'import * as foo from "foo/*"'
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
