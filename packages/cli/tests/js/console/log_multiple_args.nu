use ../../lib/test.nu *

# console.log joins multiple arguments with a single space.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("a", "b", "c");
		}
	'
}

let id = tg build -d $path | str trim
tg wait $id
tg process log --position end.0 --no-timeout $id | ignore
tg index
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	a b c

'
