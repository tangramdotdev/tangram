use ../../lib/test.nu *

# A local spawn that selects a remote cached process forwards the connection, and the cached process's logs remain readable.
const driver = path self ../../lib/process_attach.mjs
let remote = server spawn --name remote
let path = artifact {
	tangram.ts: '
		export default () => {
			console.error("cached log");
			return "cached output";
		};
	'
}
let process = tg --url $remote.url build --no-tokens --detach $path | referent node
tg --url $remote.url wait $process | ignore
let local = server spawn --name local --config { remotes: { default: { url: $remote.url } } }
let command = tg --url $remote.url process get --no-tokens $process | from json | get command | to json --raw
let output = node $driver ($local.directory | path join socket) $command none cached local | complete
success $output "a connection should follow the selected cached process"
let selected = $output.stdout | str trim
let log = tg --url $local.url log $selected | complete
success $log
assert equal $log.stderr "cached log\n"
