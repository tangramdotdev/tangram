use ../../test.nu *
use std assert

let server = spawn

# Create and tag the empty directory dependency.
let empty_path = artifact {}
let output = tg tag empty $empty_path | complete
success $output

# Create a file with xattr dependency.
let temp_dir = mktemp -d
let file_path = $temp_dir | path join "file"
'' | save -f $file_path
xattr -w user.tangram.dependencies '[\"dir_01ezfk780pwqd3cp3zxan1tycvgtnse2j1nh34y840xh8633rt3a00\"]' $file_path

# Checkin with deterministic flag.
let first_id = tg checkin --deterministic $file_path | complete | get stdout | str trim

# Checkout.
let checkout_path1 = $temp_dir | path join "checkout1"
let output = tg checkout $first_id $checkout_path1 | complete
success $output

# Clean.
let output = tg tag delete empty | complete
success $output
let output = tg clean | complete
success $output

# Checkin again with deterministic flag.
let second_id = tg checkin --deterministic $checkout_path1 | complete | get stdout | str trim

# Checkout again.
let checkout_path2 = $temp_dir | path join "checkout2"
let output = tg checkout $second_id $checkout_path2 | complete
success $output

# Verify IDs match.
assert equal $first_id $second_id
