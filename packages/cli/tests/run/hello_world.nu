use ../lib/test.nu *

# Running a basic default export that logs to the console succeeds and returns the logged output.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("Hello, World!");
		}
	'
}

let output = tg run $path
assert ($output == "Hello, World!")
