use ../../lib/test.nu *

# Solving fails when an explicit dependency is unavailable. Without solving, the checkin succeeds.

let local = server spawn --config { remotes: {} }
let dependency = "dir_01jtktnnk7yqqp1vvf0dceavydpwazedzb30w8y1wcvx4nh1kcxet0"
let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } missing)
}
let path = $directory | path join input
failure (tg checkin --no-lock --root $path | complete) "the checkin should fail when the dependency is unavailable"
let reference = tg checkin --no-lock --root --no-solve $path | str trim
assert equal (tg read $reference) 'missing' "the checkin should succeed without solving"
