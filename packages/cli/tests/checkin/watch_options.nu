use ../../test.nu *

# A checkin with different options replaces incompatible watch state instead of reusing it under the previous options.

let server = spawn

let dependency_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag -p a/1.0.0 $dependency_path

let path = artifact {
	tangram.ts: 'import a from "a/*";'
}

# Establish an unsolved watcher.
let unsolved = tg checkin $path --watch --no-checkout-pointers --no-lock --no-solve

# Replace it with solved watch state using the default solve option.
let solved = tg checkin $path --watch --no-checkout-pointers --no-lock
assert ($solved != $unsolved) "solving the dependency should change the id"

# Returning to --no-solve must produce the original unsolved artifact.
let watched = tg checkin $path --watch --no-checkout-pointers --no-lock --no-solve
assert ($watched == $unsolved) "the watched no-solve checkin should match the original unsolved checkin"

let cold = tg checkin $path --no-checkout-pointers --no-lock --no-solve
assert ($watched == $cold) "the watched no-solve checkin should match a cold checkin"
