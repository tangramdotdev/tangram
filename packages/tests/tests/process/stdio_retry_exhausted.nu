use ../../util/test.nu *

# A client whose stdio stream cannot reconnect fails once its stream retries are exhausted, instead of reconnecting forever.

const proxy_path = path self ../../util/stdio_reset_proxy.js

let server = server spawn
let path = artifact {
	tangram.ts: '
		export default async () => {
			let reader = await tg.spawn`while read -r line; do :; done`.stdin("pipe").sandbox();
			let writer = await tg.spawn`x=x; for i in 1 2 3 4 5 6 7 8 9 10; do x=$x$x; done; while :; do echo $x; done`.stdout("pipe").sandbox();
			console.log(reader.id);
			console.log(writer.id);
			await tg.sleep(60);
		};
	',
}
let parent = tg run --sandbox --detach --verbose $path | from json

# Follow the parent's log until it contains both child process IDs.
let length = (($parent.process | str length) + 1) * 2
let output = timeout 30s tg log --no-timeout --length $length $parent.process | complete
success $output "the parent should log the child process ids"
let children = $output.stdout | lines

let config = mktemp
{ client: { stream_retry: { backoff: 0.01, jitter: 0, max_delay: 0.05, max_retries: 3 } } } | to json | save -f $config

for case in [[kind command]; [read [process stdio read ($children | get 1) --stream stdout --no-timeout]] [write [process stdio write ($children | get 0) --stream stdin]]] {
	let port = port
	let log = mktemp
	let ready = mktemp -d | path join ready
	^mkfifo $ready
	let socket = $server.directory | path join socket
	let proxy = job spawn { node $proxy_path $socket $port $case.kind 1048576 $log $ready }
	success (timeout 30s cat $ready | complete) "the proxy should listen"

	# Stop the proxy once it resets the stream, so that every reconnect fails.
	job spawn {
		wait_for_growth $log 0 --timeout 30sec | ignore
		job kill $proxy
	} | ignore

	let input = "" | fill --character x --width 8388608
	let output = with-env { TANGRAM_CONFIG: $config } {
		$input | timeout 30s tg --url $'http://127.0.0.1:($port)' ...$case.command | complete
	}
	assert ((open --raw $log | lines | length) == 1) $"the stdio ($case.kind) must be reset"
	assert ($output.exit_code != 124) $"the stdio ($case.kind) must fail instead of reconnecting forever"
	failure $output $"the stdio ($case.kind) must fail once its retries are exhausted"
}
