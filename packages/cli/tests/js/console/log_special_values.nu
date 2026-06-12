use ../../../test.nu *

# console.log formats booleans, null, and undefined by name.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log(true, null, undefined);
		};
	'
}

let id = tg build -d $path | str trim
tg wait $id
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	true null undefined

'
