use ../../test.nu *
use ../lib/checkin.nu checkin-output

# Solving fails when an explicit dependency is unavailable. Without solving, it remains opaque.

let server = server spawn --config { remotes: {} }
let dependency = "dir_01jtktnnk7yqqp1vvf0dceavydpwazedzb30w8y1wcvx4nh1kcxet0"
let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } missing)
}
let path = $directory | path join input
let failed = try {
	checkin-output $server $path | ignore
	false
} catch {
	true
}
assert $failed "the checkin should fail when the dependency is unavailable"
let output = checkin-output $server $path --no-solve
assert equal $output.permissions [object_node] "the checkin token should confer only node permission"
