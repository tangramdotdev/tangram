use ../lib/test.nu *

# A checkin with different options replaces incompatible watch state instead of reusing it under the previous options.

let local = server spawn

let dependency_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag -p a/1.0.0 $dependency_path

let path = artifact {
	tangram.ts: 'import a from "a/*";'
}

# Establish an unsolved watcher.
let unsolved = tg checkin --no-tokens $path --watch --no-checkout-pointers --no-lock --no-solve | referent node

# Replace it with solved watch state using the default solve option.
let solved = tg checkin --no-tokens $path --watch --no-checkout-pointers --no-lock | referent node
assert ($solved != $unsolved) "solving the dependency should change the id"

# Returning to --no-solve must produce the original unsolved artifact.
let watched = tg checkin --no-tokens $path --watch --no-checkout-pointers --no-lock --no-solve | referent node
assert ($watched == $unsolved) "the watched no-solve checkin should match the original unsolved checkin"

let cold = tg checkin --no-tokens $path --no-checkout-pointers --no-lock --no-solve | referent node
assert ($watched == $cold) "the watched no-solve checkin should match a cold checkin"
