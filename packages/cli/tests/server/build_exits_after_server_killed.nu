use ../lib/test.nu *

# A build exits with an error after its server is killed, instead of retrying the stdio stream until the viewer is closed.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			console.log("started");
			await tg.sleep(600);
			return tg.file("hello");
		};
	'
}

# Start the build in the background.
let output_path = mktemp -t
let build = job spawn {
	let job_id = job id
	let exit_code = ^bash -c 'env -u TANGRAM_QUIET timeout 30s tg build "$1" > "$2" 2>&1; echo $?' _ $path $output_path | str trim | into int
	$exit_code | job send --tag $job_id 0
}

# Wait until the process is running.
wait_until --timeout 30sec { open --raw $server.log | str contains 'started the process' } "the process must start"
sleep 2sec

# Kill the server.
let pid = open --raw ($server.directory | path join 'lock') | str trim | into int
kill --signal 9 $pid

# The build must exit with an error rather than be killed by the timeout.
let exit_code = job recv --tag $build
let reconnects = (^grep -a -c 'failed to reconnect the stdio read stream' $output_path | complete).stdout | str trim | into int
print -e $'exit code: ($exit_code), reconnect errors: ($reconnects)'
assert ($exit_code != 124) $'the build did not exit within 30 seconds of the server being killed, and it logged ($reconnects) reconnect errors'
assert ($exit_code != 0) 'the build must fail after the server is killed'
