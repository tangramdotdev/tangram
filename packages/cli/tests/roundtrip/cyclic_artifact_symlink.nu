use ../../test.nu *
use std assert

let server = spawn

let path = artifact {
	.tangram: (directory {
		artifacts: (directory {
			sym_01jxvmh7z5daw3yztgjbrr3hmjv9cp0jhg1mjatcqccvyez83ff2eg: (symlink 'sym_01tf70d3w3nm5tx0ghnhmcb6kcvms71967febephw12qmd9zkc1pvg')
			sym_01tf70d3w3nm5tx0ghnhmcb6kcvms71967febephw12qmd9zkc1pvg: (symlink 'sym_01jxvmh7z5daw3yztgjbrr3hmjv9cp0jhg1mjatcqccvyez83ff2eg')
		})
	})
	link: (symlink '.tangram/artifacts/sym_01jxvmh7z5daw3yztgjbrr3hmjv9cp0jhg1mjatcqccvyez83ff2eg')
}

# Checkin.
let first_id = tg checkin $path

# Checkout.
let temp_dir = mktemp -d
let checkout_path1 = $temp_dir | path join "checkout1"
run tg checkout $first_id $checkout_path1

# Clean.
run tg clean

# Checkin again.
let second_id = tg checkin $checkout_path1

# Checkout again.
let checkout_path2 = $temp_dir | path join "checkout2"
run tg checkout $second_id $checkout_path2

# Verify IDs match.
assert equal $first_id $second_id
