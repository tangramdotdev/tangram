use ../../../test.nu *

# console.error writes its message to the process's stderr stream.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.error("oops");
		}
	'
}

let id = tg build -d $path | str trim
tg wait $id
let stderr = tg process log --stream stderr $id | complete
snapshot $stderr.stderr '
	oops

'
