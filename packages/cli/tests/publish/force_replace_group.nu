use ../../test.nu *

# Publishing with force replaces conflicting local and remote groups and their descendants.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
let local_group = tg group create test-pkg | from json
let local_child = tg group create test-pkg/child | from json
let remote_group = tg --url $remote.url group create test-pkg | from json
let remote_child = tg --url $remote.url group create test-pkg/child | from json

let path = artifact {
	tangram.ts: '
		export default function () { return "Hello, World!"; }

		export let metadata = {
			tag: "test-pkg",
		};
	'
}
let id = tg checkin $path

let output = tg publish $path | complete
failure $output "publishing should not replace a conflicting group without force"
assert equal (tg group get test-pkg | from json | get id) $local_group.id
assert equal (tg --url $remote.url group get test-pkg | from json | get id) $remote_group.id

tg publish --force $path

assert equal (tg tag get test-pkg | from json | get target.id) $id
assert equal (tg --url $remote.url tag get test-pkg | from json | get target.id) $id
failure (tg group get $local_group.id | complete) "the local group should be deleted"
failure (tg group get $local_child.id | complete) "the local descendant should be deleted"
failure (tg --url $remote.url group get $remote_group.id | complete) "the remote group should be deleted"
failure (tg --url $remote.url group get $remote_child.id | complete) "the remote descendant should be deleted"
