use ../../../test.nu *

# console.log formats an array argument as compact JSON.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log([1, "x", false]);
		}
	'
}

let id = tg build -d $path | str trim
tg wait $id
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	[1,"x",false]

'
