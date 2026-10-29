use ../../lib/test.nu *

# A built file's printed reference carries the authorization to read it, so another user can read the file through the reference but not through its bare ID.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let path = artifact {
	tangram.ts: 'export default () => tg.file("Hello, World!");'
}
tg --token $alice.token build $path | ignore
let output = tg --token $alice.token build $path | str trim
assert equal (tg --token $alice.token read $output) 'Hello, World!'
assert equal (tg --token $bob.token read $output) 'Hello, World!'
let id = $output | referent node
failure (tg --token $bob.token read $id | complete) 'another user should not read the file without its tokens'

# A reference printed by the JavaScript client for a child build output carries the inherited authorization.
let wrapper = artifact {
	tangram.ts: '
		export default async () => {
			await tg.build(child);
			const file = await tg.build(child);
			return tg.Value.print(file, { color: true });
		};
		export const child = () => tg.file("Hello, World!");
	'
}
let printed = tg --token $alice.token build $wrapper | from json
assert ($printed | str starts-with $'(char --integer 27)[94mfil_')
assert ($printed | str contains $'(char --integer 27)[0m(char --integer 27)[38;5;244m?')
let reference = $printed | ansi strip
assert equal (tg --token $bob.token read $reference) 'Hello, World!'
