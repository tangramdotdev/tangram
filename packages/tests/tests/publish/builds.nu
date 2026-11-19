use ../../util/test.nu *

# Publishing a package whose metadata requests builds builds its exports on the requested hosts, and tags each process on the local and remote servers.

let host = current_host
let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# With `builds: true`, publishing builds the default export, restricted here to this machine's host.
let path = artifact {
	tangram.ts: '
		export default function () { return "default"; }

		export let metadata = {
			tag: "builds-true/1.0.0",
			builds: true,
		};
	'
}
let output = tg publish --host $host $path | complete
success $output "the publish should succeed"
let tag = $'builds-true/builds/1.0.0/default/($host)'
let local_target = tg tag get $tag | from json | get target.id
assert ($local_target | str starts-with 'pcs_') "the local build tag should target a process"
let remote_target = tg --url $remote.url tag get $tag | from json | get target.id
assert equal $remote_target $local_target "the remote build tag should target the same process"
let output = tg --url $remote.url process output $remote_target | from json
assert equal $output "default"

# With the object form, publishing builds each export and names a tag by its path.
let typescript = [
	'export default function () { return "default"; }'
	'export function other() { return "other"; }'
	'export let metadata = {'
	'	tag: "builds-object/1.0.0",'
	$'	builds: { hosts: ["($host)"], exports: ["default", { export: "other", path: "custom/path" }] },'
	'};'
] | str join "\n"
let path = artifact { tangram.ts: $typescript }
let output = tg publish $path | complete
success $output "the publish should succeed"
for tag in [$'builds-object/builds/1.0.0/default/($host)' $'builds-object/builds/1.0.0/custom/path/($host)'] {
	let target = tg --url $remote.url tag get $tag | from json | get target.id
	assert ($target | str starts-with 'pcs_') $"the remote tag ($tag) should target a process"
}
let target = tg --url $remote.url tag get $'builds-object/builds/1.0.0/custom/path/($host)' | from json | get target.id
assert equal (tg --url $remote.url process output $target | from json) "other"

# Without the builds key, publishing builds nothing.
let path = artifact {
	tangram.ts: '
		export default function () { return "default"; }

		export let metadata = {
			tag: "builds-none/1.0.0",
		};
	'
}
let output = tg publish $path | complete
success $output "the publish should succeed"
failure (tg tag get $'builds-none/builds/1.0.0/default/($host)' | complete) "no build tag should exist"
