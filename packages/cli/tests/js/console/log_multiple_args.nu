use ../../lib/test.nu *

# console.log joins multiple arguments with a single space.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("a", "b", "c");
		}
	'
}

let id = tg build --no-tokens -d $path | referent node
tg wait $id
tg index
let stdout = tg process log --stream stdout $id | complete
snapshot $stdout.stdout '
	a b c

'
