use ../../util/test.nu *

# Publishing warns about and skips a build that fails, tags the builds that succeed, and exits successfully.

let host = current_host
let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
let typescript = [
	'export default function () { return "default"; }'
	'export function broken() { throw new Error("broken"); }'
	'export let metadata = {'
	'	tag: "builds-failure/1.0.0",'
	$'	builds: { hosts: ["($host)"], exports: ["default", "broken"] },'
	'};'
] | str join "\n"
let path = artifact { tangram.ts: $typescript }
let output = tg publish $path | complete
success $output "the publish should succeed although a build fails"
assert ($output.stderr | str contains 'skipped the build of builds-failure/1.0.0#broken')
success (tg --url $remote.url tag get $'builds-failure/builds/1.0.0/default/($host)' | complete) "the successful build should be tagged"
failure (tg --url $remote.url tag get $'builds-failure/builds/1.0.0/broken/($host)' | complete) "the failed build should not be tagged"
