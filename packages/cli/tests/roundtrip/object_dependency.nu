use ../../test.nu *

let tmp = mktemp -d

let server = spawn

# Create and tag the empty directory dependency.
let empty_path = artifact {}
run tg tag empty $empty_path

# Create a file with xattr dependency.
let path = artifact {
  tangram.ts: (file --xattrs {
    user.tangram.dependencies: '["dir_01ezfk780pwqd3cp3zxan1tycvgtnse2j1nh34y840xh8633rt3a00"]'
  })
}

# Checkin with deterministic flag.
let left = run tg checkin --deterministic $path

# Checkout.
let checkout_path1 = $tmp | path join "checkout1"
run tg checkout $left $checkout_path1 

# Clean.
run tg tag delete empty
run tg clean

# Checkin again with deterministic flag.
let right = run tg checkin --deterministic $checkout_path1

# Checkout again.
let checkout_path2 = $tmp | path join "checkout2"
run tg checkout $right $checkout_path2

# Verify IDs match.
assert equal $left $right
