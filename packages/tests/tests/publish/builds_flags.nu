use ../../util/test.nu *

# Publishing with `--no-builds` builds nothing, and `--dry-run` lists the build tags that publishing would create without creating them.

let host = current_host
let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
let typescript = [
	'export default function () { return "default"; }'
	'export function other() { return "other"; }'
	'export let metadata = {'
	'	tag: "builds-flags/1.0.0",'
	$'	builds: { hosts: ["($host)"], exports: ["default", "other"] },'
	'};'
] | str join "\n"
let path = artifact { tangram.ts: $typescript }

# A dry run lists the build tags, restricted to the requested export.
let output = tg publish --dry-run --export other $path | complete
success $output "the dry run should succeed"
let plan = $output.stdout | from json
assert equal ($plan | get 0.builds.tag) [$'builds-flags/builds/1.0.0/other/($host)']
failure (tg tag get $'builds-flags/builds/1.0.0/other/($host)' | complete) "a dry run should not create a build tag"

# A publish with `--no-builds` publishes the package without building it.
let output = tg publish --no-builds $path | complete
success $output "the publish should succeed"
success (tg tag get builds-flags/1.0.0 | complete) "the package tag should exist"
failure (tg tag get $'builds-flags/builds/1.0.0/default/($host)' | complete) "no build tag should exist"
