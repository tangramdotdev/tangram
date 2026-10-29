use ../../lib/test.nu *

# A stdio writer streams its request body without waiting for the response headers.

let server = server spawn --config { advanced: { checkpoints: true } }

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn`read line; echo "got:$line"`.stdin("pipe").stdout("log").sandbox().network(true);
			console.log(process.id);
			await process.wait();
		};
	'
}
let parent = tg run --network=true --detach --verbose $path | from json

# Follow the parent's log until it contains the child process ID.
let length = ($parent.process | str length) + 1
let output = timeout 30s tg log --no-timeout --length $length $parent.process | complete
success $output "the parent should log the child process id"
let child = $output.stdout | str trim

# Hold the write response before it is returned, then observe the first write request arriving on the request body.
let response_watch = tg checkpoint watch process.stdio.write.response | from json | get watch
let request_watch = tg checkpoint watch process.stdio.write.request | from json | get watch
let write = job spawn {
	let job_id = job id
	let output = "hello\n" | tg process stdio write $child --stream stdin | complete
	$output | job send --tag $job_id 0
}
success (tg checkpoint wait --timeout 30s process.stdio.write.response $response_watch 0 | complete) "the write response should be held"
success (tg checkpoint wait --timeout 30s process.stdio.write.request $request_watch 0 | complete) "the first write should arrive before the response headers"
tg checkpoint unwatch process.stdio.write.request $request_watch
tg checkpoint unwatch process.stdio.write.response $response_watch
let output = job recv --tag $write --timeout 30sec
success $output "the write should succeed"
tg wait $parent.process
