use ../../test.nu *

# A local spawn that selects a remote cached process forwards the connection before reading its logs.
const driver = path self ../lib/process_connect.mjs
let remote = server spawn --name remote
let path = artifact {
	tangram.ts: '
		export default () => {
			console.error("cached log");
			return "cached output";
		};
	'
}
let process = tg --url $remote.url build --detach $path | str trim
tg --url $remote.url wait $process | ignore
let local = server spawn --name local --config { remotes: { default: { url: $remote.url } } }
let command = tg --url $remote.url process get --no-tokens $process | from json | get command
let output = node $driver ($local.directory | path join socket) $command none cached local | complete
success $output "a connection should follow the selected cached process and read its logs"
