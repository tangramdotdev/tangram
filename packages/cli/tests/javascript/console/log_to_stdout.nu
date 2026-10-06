use ../../lib/test.nu *

# console.log writes its message to the process's stdout stream.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("hello world");
		}
	'
}

let id = tg build --no-tokens -d $path | referent node
tg wait $id
tg index
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	hello world

'
