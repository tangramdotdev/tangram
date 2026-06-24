use ../../../test.nu *

# console.log joins multiple arguments with a single space.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("a", "b", "c");
		}
	'
}

let id = tg build -d $path | str trim
tg wait $id
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	a b c

'
