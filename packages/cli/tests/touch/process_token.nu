use ../../test.nu *

# A process may touch an object it created but may not touch another process.

let server = server spawn

let target_path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run`sleep 60`.sandbox();
		}
	'
}
let target = tg build --detach --verbose $target_path | from json

let path = artifact {
	tangram.ts: '
		export default function (process: string) {
			return tg.run`
				if tg process touch ${process} > /dev/null 2> "$TANGRAM_OUTPUT"; then
					exit 1
				fi
				object=$(tg put \x27tg.file("object for token")\x27)
				tg object touch "$object"
			`
				.sandbox()
				.then(tg.File.expect);
		}
	'
}
let output = tg build $path --arg-string $target.process | str trim | tg cat $in
assert ($output | str contains "failed to touch the process")

tg cancel $target.process $target.lease
tg wait $target.process
