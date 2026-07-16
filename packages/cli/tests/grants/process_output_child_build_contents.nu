use ../../test.nu *

# A process can return a file whose contents are a child build's output blob, and the builder can read the contents.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json

let path = artifact {
	tangram.ts: '
		export default async function () {
			return tg.file(tg.build(f));
		}
		export function f() {
			return tg.run`echo hello > $TANGRAM_OUTPUT`.then(tg.File.expect).then((file) => file.contents);
		}
	',
}

let process = tg --token $alice.token build --detach $path | str trim
let result = tg --token $alice.token wait $process | from json
assert ($result.exit == 0) "the build should succeed."
let contents = tg --token $alice.token cat $result.output.value.id | str trim
assert ($contents == "hello") "the builder should read the file contents produced by the child build."
