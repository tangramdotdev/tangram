use ../lib/test.nu *

# Both initialization orders work, and tg index waits for queued terminal writes.
for order in [process sandbox] {
	let root_token = random chars
	let remote = server spawn --preserve-keys --name $'remote-($order)' --config {
		advanced: { checkpoints: true },
		authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
		roles: [api indexer scheduler],
	}
	let created = tg --url $remote.url --token $root_token runner create | from json
	let runner = server spawn --name $'runner-($order)' --config {
		advanced: { checkpoints: true },
		remotes: { default: { token: $created.token.token, url: $remote.url } },
		roles: [api indexer runner],
		runner: { id: $created.data.id, remote: default, token: $created.token.token },
	}
	let alice = tg --url $remote.url login --verbose --name alice | from json
	let local = server spawn --name $'local-($order)' --config {
		remotes: { default: { token: $alice.token, url: $remote.url } },
	}
	let start_watch = tg --url $runner.url checkpoint watch runner.process.start | from json | get watch
	let path = artifact {
		tangram.ts: '
			export default async function () {
				const file = await tg.file({ contents: "#!/bin/sh\nprintf hello > \"$TANGRAM_OUTPUT\"", executable: true });
				return tg.build(file).sandbox();
			}
		',
	}
	let build = job spawn {
		let job_id = job id
		let output = tg --url $local.url build --remote $path | complete
		$output | job send --tag $job_id 0
	}

	# Let the scheduled parent initialize before watching the shortcut.
	success (timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | complete)
	tg --url $remote.url --token $root_token index
	let sandbox_watch = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.connect | from json | get watch
	let process_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.index.started | from json | get watch
	let process_submitted = tg --url $remote.url --token $root_token checkpoint watch process.control.index.submitted | from json | get watch
	let sandbox_submitted = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.index.submitted | from json | get watch
	tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
	let sandbox = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.connect $sandbox_watch 0 | from json | get params.sandbox
	let process = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.index.started $process_watch 0 | from json | get params.process
	let start_write = if $order == sandbox {
		let params = { started_process: true } | to json --raw
		tg --url $remote.url --token $root_token checkpoint watch index.batch --params $params | from json | get watch
	} else { null }

	# Either stream can submit initialization while the other is still preparing.
	if $order == process {
		tg --url $remote.url --token $root_token checkpoint unwatch process.control.index.started $process_watch
		success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.index.submitted $process_submitted 0 | complete)
		tg --url $remote.url --token $root_token index
		let sandboxes = tg --url $remote.url --token $alice.token sandbox list | from json | get id
		assert ($sandbox in $sandboxes) "process initialization should index the sandbox with its owner"
		tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.connect $sandbox_watch
	} else {
		tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.connect $sandbox_watch
		success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.index.submitted $sandbox_submitted 0 | complete)
		tg --url $remote.url --token $root_token checkpoint unwatch process.control.index.started $process_watch
	}
	success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.index.submitted $process_submitted 0 | complete)
	success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.index.submitted $sandbox_submitted 0 | complete)
	tg --url $remote.url --token $root_token checkpoint unwatch process.control.index.submitted $process_submitted
	tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.index.submitted $sandbox_submitted
	if $start_write != null {
		success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait index.batch $start_write 0 | complete)
		let index = job spawn {
			let job_id = job id
			let output = tg --url $remote.url --token $root_token index | complete
			$output | job send --tag $job_id 0
		}
		assert equal (try { job recv --tag $index --timeout 1sec } catch { null }) null "tg index must wait for the submitted initialization"
		tg --url $remote.url --token $root_token checkpoint unwatch index.batch $start_write
		success (job recv --tag $index --timeout 30sec)
	}
	tg --url $remote.url --token $root_token index

	# Hold terminal writes after submission, without holding their control responses.
	let params = { finished_process: true } | to json --raw
	let finish_write = tg --url $remote.url --token $root_token checkpoint watch index.batch --params $params | from json | get watch
	let params = { destroyed_sandbox: true } | to json --raw
	let destroy_write = tg --url $remote.url --token $root_token checkpoint watch index.batch --params $params | from json | get watch
	let destroyed = tg --url $runner.url checkpoint watch runner.sandbox.destroyed | from json | get watch
	let started = timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 1 | from json
	assert equal $started.params.process $process
	let status = job spawn {
		let job_id = job id
		let output = tg --url $remote.url --token $root_token process status --no-timeout $process | complete
		$output | job send --tag $job_id 0
	}
	assert equal (try { job recv --tag $status --timeout 1sec } catch { null }) null "the status stream must remain open while the process is running"
	tg --url $runner.url checkpoint unwatch runner.process.start $start_watch
	success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait index.batch $finish_write 0 | complete)

	# Indexing must wait for the process write, but process control may already have closed.
	let index = job spawn {
		let job_id = job id
		let output = tg --url $remote.url --token $root_token index | complete
		$output | job send --tag $job_id 0
	}
	assert equal (try { job recv --tag $index --timeout 1sec } catch { null }) null "tg index must wait for the submitted process write"
	tg --url $remote.url --token $root_token checkpoint unwatch index.batch $finish_write

	# Allow process indexing to complete before waiting for sandbox teardown.
	success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait index.batch $destroy_write 0 | complete)
	success (timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.destroyed $destroyed 0 | complete)
	let data = timeout 10s tg --url $remote.url --token $root_token sandbox get $sandbox | from json
	assert equal $data.data.status destroyed
	let sandbox_index = job spawn {
		let job_id = job id
		let output = tg --url $remote.url --token $root_token index | complete
		$output | job send --tag $job_id 0
	}
	assert equal (try { job recv --tag $sandbox_index --timeout 1sec } catch { null }) null "tg index must wait for the submitted sandbox write"
	tg --url $remote.url --token $root_token checkpoint unwatch index.batch $destroy_write
	success (job recv --tag $index --timeout 30sec)
	success (job recv --tag $sandbox_index --timeout 30sec)
	tg --url $runner.url checkpoint unwatch runner.sandbox.destroyed $destroyed

	# Process control may close after Finish is acknowledged, so observe completion after indexing commits.
	let output = job recv --tag $status --timeout 10sec
	success $output "the status stream should finish after indexing commits"
	assert ($output.stdout | str contains finished)
	let data = tg --url $remote.url --token $root_token process get $process | from json
	assert equal $data.status finished
	assert equal $data.sandbox $sandbox
	let data = tg --url $remote.url --token $root_token sandbox get $sandbox | from json
	assert equal $data.data.status destroyed
	let output = job recv --tag $build --timeout 30sec
	success $output "the process should complete with its output grants"
	let read = tg --url $local.url read ($output.stdout | str trim) | complete
	success $read
	assert equal $read.stdout hello
}
