use ../../test.nu *

# A process can return a file whose contents are extracted from a child build's output file, and the builder can read the contents. The child build returns a file; the parent reads its contents blob and wraps it in a new file.

let server = spawn --config { authentication: true }
let alice = tg login --verbose alice | from json

let path = artifact {
	tangram.ts: '
		export default async function () {
			return tg.file(tg.build(f).then((file) => file.contents)).executable(true);
		}
		export function f() {
			return tg.run`echo hello > $TANGRAM_OUTPUT`.then(tg.File.expect);
		}
	',
}

let process = tg --token $alice.token build --detach $path | str trim
let result = tg --token $alice.token wait $process | from json
assert ($result.exit == 0) "the build should succeed."
let contents = tg --token $alice.token cat $result.output.value.id | str trim
assert ($contents == "hello") "the builder should read the file contents extracted from the child build's output."
