use ../../lib/test.nu *

# console.log formats booleans, null, and undefined by name.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log(true, null, undefined);
		}
	'
}

let id = tg build -d $path | str trim
tg wait $id
tg process log --position end.0 --no-timeout $id | ignore
tg index
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	true null undefined

'
