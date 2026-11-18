use ../../util/test.nu *

# Publishing publicly grants anonymous read access to the package and its dependency tags.

let remote = server spawn --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $remote.url login --verbose --name alice | from json
let root_token = random chars
let local = server spawn --name local --config {
	authentication: { root: { token: $root_token } }
	remotes: { default: { url: $remote.url, token: $alice.token } }
}
let config = mktemp
{} | to json | save -f $config

let dependency = artifact {
	'tangram.ts': '
		export default function () { return "public dependency"; }
		export let metadata = { tag: "dep/1.0.0" };
	'
}
let path = artifact {
	'tangram.ts': '
		import dep from "dep";
		export default function () { return dep(); }
		export let metadata = { tag: "main/1.0.0" };
	'
}
tg --token $root_token tag put -p dep/1.0.0 $dependency
tg --token $root_token index
tg --token $root_token publish --public $path
tg --url $remote.url --token $alice.token index

with-env { TANGRAM_CONFIG: $config } {
	let main_id = tg --url $remote.url tag get main/1.0.0 | from json | get target.id
	let main = tg --url $remote.url get --no-tokens --depth=inf --blobs $main_id
	assert ($main | str contains 'import dep')
	let dependency_id = tg --url $remote.url tag get dep/1.0.0 | from json | get target.id
	let dependency = tg --url $remote.url get --no-tokens --depth=inf --blobs $dependency_id
	assert ($dependency | str contains 'public dependency')
}
