use ../../test.nu *
use std assert

let server = spawn

let temp_file = mktemp -t
"hello, world!" | save -f $temp_file

# Checkin.
let first_id = tg checkin $temp_file

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
