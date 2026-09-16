use ../../test.nu *

# Checking out a simple file preserves writable permissions with and without a prior internal checkout.

let tmp = mktemp --directory

# Keep the server and destinations on the same filesystem so internal checkouts can be reflinked.
let server = server spawn --directory ($tmp | path join 'server')

let artifact = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("Hello, World!")
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
	assert equal (ls -l $path | first | get mode) 'rw-r--r--' $'incorrect permissions with internal_checkout=($internal_checkout)'
	snapshot --path $path
}
