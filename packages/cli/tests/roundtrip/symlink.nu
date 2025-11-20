use ../../test.nu *
use std assert

let server = spawn

let path = artifact {
	file: 'hello!'
	link: (symlink 'file')
}

# Checkin.
let first_id = tg checkin $path | complete | get stdout | str trim

# Checkout.
let temp_dir = mktemp -d
let checkout_path1 = $temp_dir | path join "checkout1"
let output = tg checkout $first_id $checkout_path1 | complete
success $output

# Clean.
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
