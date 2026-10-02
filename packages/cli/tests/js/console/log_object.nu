use ../../lib/test.nu *

# console.log formats an object argument as compact JSON.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log({ a: 1, b: [2, 3] });
		}
	'
}

let id = tg build --no-tokens -d $path | referent node
tg wait $id
tg index
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	{"a":1,"b":[2,3]}

'
