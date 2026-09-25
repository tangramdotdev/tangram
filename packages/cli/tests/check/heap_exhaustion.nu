use ../lib/test.nu *

# A check that exhausts the compiler's heap fails without aborting the server.

let server = server spawn

# Each alias is a union of 90,000 strings, so checking a thousand of them exceeds any default V8 heap limit.
let aliases = 0..999 | each { |i| $'type A($i) = `($i)${T}`;' }
let path = artifact {
	tangram.ts: ([
		'type D = "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9";'
		'type E = "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9";'
		'type T = `${D}${D}${D}${D}${E}`;'
		...$aliases
		'export default function () {}'
	] | str join "\n")
}

let output = tg check $path | complete
failure $output
snapshot $output.stderr '
	error an error occurred
	-> failed to check
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to check the modules
	-> failed to check the modules
	-> the compiler ran out of memory

'
let output = tg health | complete
success $output "the server must survive a check that exhausts the compiler's heap"
