use ../../../test.nu *

# console.log formats an object argument as compact JSON.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log({ a: 1, b: [2, 3] });
		};
	'
}

let id = tg build -d $path | str trim
tg wait $id
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	{"a":1,"b":[2,3]}

'
