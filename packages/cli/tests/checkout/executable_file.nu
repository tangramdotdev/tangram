use ../lib/test.nu *

# Checking out an executable file preserves writable and executable permissions with and without a prior internal checkout.

let tmp = mktemp --directory

# Keep the server and destinations on the same filesystem so internal checkouts can be reflinked.
let server = server spawn --directory ($tmp | path join 'server')

let artifact = artifact {
	tangram.ts: '
		export default function () {
			return tg.file({
				contents: "Hello, World!",
				executable: true,
			})
		}
	'
}
let id = tg build $artifact

for internal_checkout in [false true] {
	if $internal_checkout {
		tg checkout $id
	}
	let path = $tmp | path join $'checkout_($internal_checkout)'
	tg checkout $id --path $path
	assert equal (ls -l $path | first | get mode) 'rwxr-xr-x' $'incorrect permissions with internal_checkout=($internal_checkout)'
	snapshot --path $path
}
