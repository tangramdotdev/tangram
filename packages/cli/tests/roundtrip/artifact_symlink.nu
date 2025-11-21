use ../../test.nu *
use std assert

let server = spawn

let path = artifact {
	.tangram: (directory {
		artifacts: (directory {
			dir_01ds3dt46yzjdndgmtdv2ppm4c47tmr20s46ae9qs5qwvf1je3r9wg: (directory {})
		})
	})
	link: (symlink '.tangram/artifacts/dir_01ds3dt46yzjdndgmtdv2ppm4c47tmr20s46ae9qs5qwvf1je3r9wg')
}

# Checkin.
let first_id = tg checkin $path

# Checkout.
let temp_dir = mktemp -d
let checkout_path1 = $temp_dir | path join "checkout1"
run tg checkout $first_id $checkout_path1

# Clean.
let output =  tg clean | complete
success $output

# Checkin again.
let second_id = tg checkin $checkout_path1

# Checkout again.
let checkout_path2 = $temp_dir | path join "checkout2"
run tg checkout $second_id $checkout_path2

# Verify IDs match.
assert equal $first_id $second_id
