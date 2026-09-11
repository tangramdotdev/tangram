use ../../test.nu *

# Runner waits finish before indexing, but subsequent output and error authorization waits for the queued batch.

for location in [local remote] {
	let root_token = random chars
	let owner = server spawn --name $'owner-($location)' --config {
		advanced: { checkpoints: true, single_process: ($location == local) },
		authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
		roles: (if $location == local { [api indexer runner scheduler] } else { [api indexer scheduler] }),
	}
	let runner = if $location == remote {
		let created = tg --url $owner.url --token $root_token runner create | from json
		server spawn --name runner --config {
			advanced: { checkpoints: true, single_process: true },
			authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
			remotes: { default: { token: $root_token, url: $owner.url } },
			roles: [api indexer runner],
			runner: { id: $created.data.id, remote: default, token: $created.token.token },
		}
	} else {
		$owner
	}
	let reader = tg --url $runner.url login --verbose --name reader | from json
	let node_reader = tg --url $runner.url login --verbose --name node-reader | from json
	let socket = $runner.url | str replace 'http+unix://' '' | url decode
	for case in [{ field: output, control_first: false }, { field: error, control_first: false }, { field: output, control_first: true }] {
		let field = $case.field
		let finish_watch = tg --url $runner.url --token $root_token checkpoint watch runner.process.finish | from json | get watch
		let control_watch = tg --url $owner.url --token $root_token checkpoint watch process.control.finish | from json | get watch
		let source = if $field == output {
			'export default () => { console.log("runner log"); return tg.file("output"); };'
		} else {
			'export default () => { throw new Error("runner error"); };'
		}
		let source = $source + $"\n// ($location) ($case.control_first)"
		let path = artifact { tangram.ts: $source }
		let spawned = tg --url $owner.url --token $root_token build --detach --verbose $path | from json
		let process = $spawned.process | split row '?' | first
		timeout 30s tg --url $runner.url --token $root_token checkpoint wait runner.process.finish $finish_watch 0 | ignore
		tg --url $runner.url --token $root_token grant $reader.user.id process_node $process | ignore
		tg --url $runner.url --token $root_token grant $reader.user.id $'process_node_($field)' $process | ignore
		tg --url $runner.url --token $root_token grant $node_reader.user.id process_node $process | ignore

		# Attach the reader before completion, then hold the finished-process batch and the control finish handler.
		let params = { process: $process } | to json --raw
		let attach_watch = tg --url $runner.url --token $root_token checkpoint watch process.wait.attach --params $params | from json | get watch
		let query = { lease: $spawned.lease, location: $location } | url build-query
		let wait_job = job spawn {
			let job_id = job id
			let output = http post --raw --max-time 30sec --unix-socket $socket --headers { Accept: 'text/event-stream', Authorization: $'Bearer ($reader.token)' } $'http://localhost/processes/($process)/wait?($query)' ''
			$output | job send --tag $job_id 0
		}
		timeout 10s tg --url $runner.url --token $root_token checkpoint wait process.wait.attach $attach_watch 0 | ignore
		tg --url $runner.url --token $root_token checkpoint unwatch process.wait.attach $attach_watch
		let batch_watch = tg --url $runner.url --token $root_token checkpoint watch index.batch --params '{"finished_process":true}' | from json | get watch
		tg --url $runner.url --token $root_token checkpoint unwatch runner.process.finish $finish_watch
		timeout 30s tg --url $runner.url --token $root_token checkpoint wait index.batch $batch_watch 0 | ignore
		timeout 30s tg --url $owner.url --token $root_token checkpoint wait process.control.finish $control_watch 0 | ignore
		let output = try { job recv --tag $wait_job --timeout 10sec } catch {
			error make { msg: $'($location) ($field) wait did not return while the finished-process batch was held' }
		}
		let output = $output | lines | where { str starts-with 'data: ' } | last | str substring 6.. | from json
		let object = if $field == output { $output.output.value } else { $output.error }
		assert (not ($object | str contains 'tokens')) "waiting must not mint object capabilities"
		if $case.control_first {
			tg --url $owner.url --token $root_token checkpoint unwatch process.control.finish $control_watch
			wait_until {
				let data = tg --url $owner.url --token $root_token process get $process | from json
				$data.log? | is-not-empty
			} --timeout 10sec "the control finish handler should write and compact the log before the runner batch"
			tg --url $runner.url --token $root_token checkpoint unwatch index.batch $batch_watch
		}

		# An authorized read waits for the queued batch instead of failing before the control handler indexes completion.
		let read_job = job spawn {
			let job_id = job id
			let output = if $field == output {
				tg --url $runner.url --token $reader.token cat $object | complete
			} else {
				tg --url $runner.url --token $reader.token get $object | complete
			}
			$output | job send --tag $job_id 0
		}
		if not $case.control_first {
			let premature = try { job recv --tag $read_job --timeout 1sec } catch { null }
			assert equal $premature null "object authorization must wait for the queued finished-process batch"
			tg --url $runner.url --token $root_token checkpoint unwatch index.batch $batch_watch
		}
		let read = try { job recv --tag $read_job --timeout 10sec } catch {
			error make { msg: $'($location) ($field) read did not finish after the index batch was released' }
		}
		success $read "the field grant must authorize the object before the control finish handler runs"
		assert ($read.stdout | str contains (if $field == output { 'output' } else { 'runner error' }))
		failure (tg --url $runner.url --token $node_reader.token get $object | complete) "node permission must not grant access to the object"
		if not $case.control_first {
			tg --url $owner.url --token $root_token checkpoint unwatch process.control.finish $control_watch
		}
		if $field == output {
			tg --url $owner.url --token $root_token index
			let log = tg --url $owner.url --token $root_token process log $process | str trim
			assert equal $log 'runner log' "the finish handler must preserve the process log after early runner indexing"
		}
	}
}
