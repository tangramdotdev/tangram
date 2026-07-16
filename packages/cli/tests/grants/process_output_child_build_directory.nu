use ../../test.nu *

# A process can return a directory that nests a child build's output file, and the builder can read the nested file.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json

let path = artifact {
	tangram.ts: '
		export default async function () {
			return tg.directory({ file: tg.build(f) });
		}
		export function f() {
			return tg.run`echo hello > $TANGRAM_OUTPUT`.then(tg.File.expect);
		}
	',
}

let process = tg --token $alice.token build --detach $path | str trim
let result = tg --token $alice.token wait $process | from json
assert ($result.exit == 0) "the build should succeed."
let file = tg --token $alice.token get $result.output.value.id | parse --regex '(fil_[0-9a-z]+)' | get capture0.0
let contents = tg --token $alice.token cat $file | str trim
assert ($contents == "hello") "the builder should read the file produced by the child build and nested in the directory output."
