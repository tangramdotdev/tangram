use ../../../test.nu *

# console.log writes its message to the process's stdout stream.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("hello world");
		}
	'
}

let id = tg build -d $path | str trim
tg wait $id
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	hello world

'
