use ../../lib/test.nu *

# console.log formats booleans, null, and undefined by name.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log(true, null, undefined);
		}
	'
}

let id = tg build --no-tokens -d $path | referent node
tg wait $id
tg index
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	true null undefined

'
