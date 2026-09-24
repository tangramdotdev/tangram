use ../lib/test.nu *

# A built file retains its subtree token without redundant process authorization.
let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let path = artifact {
	tangram.ts: 'export default () => tg.file("Hello, World!");'
}
tg --token $alice.token build $path | ignore
let output = tg --token $alice.token build $path | str trim
let uri = $'http://localhost/($output)' | url parse
let tokens = $uri.params | where key starts-with 'tokens[local]'
assert equal ($tokens | length) 1
let body = $tokens.0.value | split row '.' | get 1 | decode base64 | decode utf-8 | from json
assert equal $body.resource ($uri.path | str substring 1..)
assert equal $body.permissions [object_subtree]
assert equal (tg --token $alice.token read $output) 'Hello, World!'

# Reading a child build exercises token inheritance in the JavaScript client.
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
let tokens = $'http://localhost/($reference)' | url parse | get params | where key starts-with 'tokens[local]'
assert equal ($tokens | length) 1
