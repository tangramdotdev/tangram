use ../../../test.nu *

# An internal checkout accepts multiple artifacts, while an external checkout accepts exactly one.

let server = server spawn

let first = artifact 'first'
let second = artifact 'second'
let first_id = tg checkin --no-checkout-pointers $first
let second_id = tg checkin --no-checkout-pointers $second

let paths = tg checkout $first_id $second_id | lines
assert (($paths | length) == 2) "expected two checkout paths"
assert ((open $paths.0) == 'first') "expected the first artifact"
assert ((open $paths.1) == 'second') "expected the second artifact"

let path = $env.TMPDIR | path join output
let output = tg checkout $first_id $second_id --path $path | complete
failure $output "expected an external checkout of multiple artifacts to fail"

for option in [
	{ args: ["--dependencies=true"], message: "the dependencies option cannot be set" }
	{ args: ["--dependencies=false"], message: "the dependencies option cannot be set" }
	{ args: ["--lock=auto"], message: "the lock option cannot be set" }
	{ args: ["--no-lock"], message: "the lock option cannot be set" }
	{ args: ["--force"], message: "the following required arguments were not provided" }
] {
	let output = tg checkout $first_id ...$option.args | complete
	failure $output $option.message
}
