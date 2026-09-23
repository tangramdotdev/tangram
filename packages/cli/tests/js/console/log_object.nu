use ../../lib/test.nu *

# console.log formats an object argument as compact JSON.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log({ a: 1, b: [2, 3] });
		}
	'
}

let id = tg build -d $path | str trim
tg wait $id
tg process log --position end.0 --no-timeout $id | ignore
tg index
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	{"a":1,"b":[2,3]}

'
