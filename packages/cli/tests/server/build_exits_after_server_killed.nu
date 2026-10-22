use ../lib/test.nu *

# A build with an active viewer exits with a wait error after its server is killed.

let server = server spawn --config {
	advanced: { checkpoints: true },
}
let watch = tg checkpoint watch process.stdio.read.ack | from json | get watch

let path = artifact {
	tangram.ts: '
		export default async () => {
			console.log("started");
			await tg.sleep(600);
		};
	'
}

# Start the build in the background.
let build = job spawn {
	let job_id = job id
	let output = tg --no-quiet build $path | complete
	$output | job send --tag $job_id 0
}

# Wait until the viewer has received the process's log message.
success (tg checkpoint wait --timeout 30s process.stdio.read.ack $watch 0 | complete) 'the viewer must receive the process log'

# Kill the server.
let pid = open --raw ($server.directory | path join 'lock') | str trim | into int
kill --signal 9 $pid

# The build must report a wait error within 30 seconds of the server being killed.
let output = try { job recv --tag $build --timeout 30sec } catch { null }
if $output == null {
	error make { msg: 'the build did not exit within 30 seconds of the server being killed' }
}
failure $output 'the build must fail after the server is killed'
assert ($output.stderr | str contains 'failed to await the process') $output.stderr
