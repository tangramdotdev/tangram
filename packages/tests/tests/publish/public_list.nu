use ../../util/test.nu *

# Publishing publicly lets an anonymous client list the package's versions.

let remote = server spawn --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $remote.url login --verbose --name alice | from json
let root_token = random chars
let local = server spawn --name local --config {
	authentication: { root: { token: $root_token } }
	remotes: { default: { url: $remote.url, token: $alice.token } }
}
let config = mktemp
{} | to json | save -f $config

let path = artifact {
	'tangram.ts': '
		export default function () { return "public package"; }
		export let metadata = { tag: "main/1.0.0" };
	'
}
tg --token $root_token publish --public $path
tg --url $remote.url --token $alice.token index

# The publisher lists the published version.
let specifiers = tg --url $remote.url --token $alice.token list main | from json | get specifier
assert ('main/1.0.0' in $specifiers)

# An anonymous client lists the published version.
with-env { TANGRAM_CONFIG: $config } {
	let output = tg --url $remote.url list main | complete
	assert ($output.exit_code == 0) $output.stderr
	let specifiers = $output.stdout | from json | get specifier
	assert ('main/1.0.0' in $specifiers)
}
