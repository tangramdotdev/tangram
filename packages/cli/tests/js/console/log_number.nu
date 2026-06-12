use ../../../test.nu *

# console.log formats a number argument as its decimal representation.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log(42);
		};
	'
}

let id = tg build -d $path | str trim
tg wait $id
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	42

'
