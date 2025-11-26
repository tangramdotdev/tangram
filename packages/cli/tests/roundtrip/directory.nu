use ../../test.nu *

let server = spawn

let path = artifact {
	file: 'hello!'
	link: (symlink 'file')
}

# Checkin.
let first_id = run tg checkin $path

# Checkout.
let temp_dir = mktemp -d
let checkout_path1 = $temp_dir | path join "checkout1"
run tg checkout $first_id $checkout_path1

# Clean.
run tg clean

# Checkin again.
let second_id = run tg checkin $checkout_path1

# Checkout again.
let checkout_path2 = $temp_dir | path join "checkout2"
run tg checkout $second_id $checkout_path2

# Verify IDs match.
assert equal $first_id $second_id
