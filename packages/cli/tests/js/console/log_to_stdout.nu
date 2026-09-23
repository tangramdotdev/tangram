use ../../lib/test.nu *

# console.log writes its message to the process's stdout stream.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("hello world");
		}
	'
}

let id = tg build -d $path | str trim
tg wait $id
tg process log --position end.0 --no-timeout $id | ignore
tg index
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	hello world

'
