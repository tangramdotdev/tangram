use ../lib/test.nu *

# Runner waits retain output and error capabilities before the finished process reaches the index.

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
	for case in [{ field: output, both: false, inherited: false }, { field: error, both: false, inherited: false }, { field: error, both: true, inherited: false }, { field: output, both: false, inherited: true }] {
		let field = $case.field
		let finish_watch = tg --url $runner.url --token $root_token checkpoint watch runner.process.finish | from json | get watch
		let control_watch = tg --url $owner.url --token $root_token checkpoint watch process.control.finish | from json | get watch
		let source = if $case.inherited {
			'export default async () => {
				const directory = await tg.directory({ shared: tg.file("output inherited"), private: tg.file("private") });
				await directory.store();
				const file = await directory.get("shared");
				tg.assert(file.state.tokens.local?.some((token) => tg.Authorization.Token.grantsObjectSubtree(token, file.id)));
				console.log("runner log");
				return file;
			};'
		} else if $field == output {
			'export default () => { console.log("runner log"); return tg.file("output"); };'
		} else {
			'export default () => { throw new Error("runner error"); };'
		}
		let source = $source + $"\n// ($location) ($case.both)"
		let path = artifact { tangram.ts: $source }
		let spawned = tg --url $owner.url --token $root_token build --detach --verbose $path | from json
		let process = $spawned.process | split row '?' | first
		success (timeout 30s tg --url $runner.url --token $root_token checkpoint wait runner.process.finish $finish_watch 0 | complete) "the runner must reach the finish checkpoint"
		tg --url $runner.url --token $root_token grant $reader.user.id process_node $process | ignore
		tg --url $runner.url --token $root_token grant $reader.user.id $'process_node_($field)' $process | ignore
		if $case.both {
			tg --url $runner.url --token $root_token grant $reader.user.id process_node_output $process | ignore
		}
		tg --url $runner.url --token $root_token grant $node_reader.user.id process_node $process | ignore

		# Attach the reader before completion, then hold the finished-process batch.
		let params = { process: $process } | to json --raw
		let attach_watch = tg --url $runner.url --token $root_token checkpoint watch process.wait.attach --params $params | from json | get watch
		let query = { lease: $spawned.lease, location: $location } | url build-query
		let wait_job = job spawn {
			let job_id = job id
			let output = http post --raw --max-time 30sec --unix-socket $socket --headers { Accept: 'text/event-stream', Authorization: $'Bearer ($reader.token)' } $'http://localhost/processes/($process)/wait?($query)' ''
			$output | job send --tag $job_id 0
		}
		success (timeout 10s tg --url $runner.url --token $root_token checkpoint wait process.wait.attach $attach_watch 0 | complete) "the reader must attach before completion"
		let node_wait_job = job spawn {
			let job_id = job id
			let output = http post --raw --max-time 30sec --unix-socket $socket --headers { Accept: 'text/event-stream', Authorization: $'Bearer ($node_reader.token)' } $'http://localhost/processes/($process)/wait?($query)' ''
			$output | job send --tag $job_id 0
		}
		success (timeout 10s tg --url $runner.url --token $root_token checkpoint wait process.wait.attach $attach_watch 1 | complete) "the node reader must attach before completion"
		tg --url $runner.url --token $root_token checkpoint unwatch process.wait.attach $attach_watch
		let batch_params = '{"finished_process":true}'
		let batch_watch = tg --url $runner.url --token $root_token checkpoint watch index.batch --params $batch_params | from json | get watch
		tg --url $runner.url --token $root_token checkpoint unwatch runner.process.finish $finish_watch
		success (timeout 30s tg --url $runner.url --token $root_token checkpoint wait index.batch $batch_watch 0 | complete) "the finished-process batch must reach the checkpoint"
		let premature = try { job recv --tag $wait_job --timeout 1sec } catch { null }
		tg --url $runner.url --token $root_token checkpoint unwatch index.batch $batch_watch
		success (timeout 30s tg --url $owner.url --token $root_token checkpoint wait process.control.finish $control_watch 0 | complete) "the control finish handler must reach the checkpoint"
		let output = if $premature != null { $premature } else {
			try { job recv --tag $wait_job --timeout 10sec } catch {
				error make { msg: $'($location) ($field) wait did not return after the finished-process batch was indexed' }
			}
		}
		let output = $output | lines | where { str starts-with 'data: ' } | last | str substring 6.. | from json
		if $field == output {
			assert equal $output.exit 0 "the process must succeed before reading its output"
		} else {
			assert ($output.exit != 0) "the process must fail before reading its error"
		}
		let object = if $field == output { $output.output.value } else { $output.error }
		let object_id = $object | split row '?' | first
		let node_output = job recv --tag $node_wait_job --timeout 10sec
		let node_output = $node_output | lines | where { str starts-with 'data: ' } | last | str substring 6.. | from json
		let node_object = if $field == output { $node_output.output.value } else { $node_output.error }
		assert equal ($node_object | split row '?' | first) $object_id
		let node_params = $'http://localhost/($node_object)' | url parse | get params
		assert ($node_params | all {|param| $param.key !~ '^tokens' }) "a live node reader must not receive output or error capabilities"
		let params = $'http://localhost/($object)' | url parse | get params
		if $field == output and not $case.inherited {
			assert ($params | any {|param| $param.key == 'tokens[local][0]' }) "a live output reader must retain the output's authorization token"
		}
		for param in ($params | where {|param| $param.key =~ '^tokens\[' }) {
			let body = $param.value | split row '.' | get 1 | decode base64 | decode utf-8 | from json
			if not ($body.resource | str starts-with "syn_") {
				assert equal $body.resource $object_id "the live response must not expose an ancestor's authorization token"
			}
		}
		if $location == remote and ($field == output or $case.both) {
			assert ($params | any {|param| $param.key == 'tokens[remote][0]' }) $"the result sync token must be associated with its issuer: ($field) ($params | get key | to json --raw)"
		} else if $location == remote {
			assert ($params | where {|param| $param.key =~ '^tokens\[' } | all {|param|
				let body = $param.value | split row '.' | get 1 | decode base64 | decode utf-8 | from json
				not ($body.resource | str starts-with 'syn_')
			}) "error permission alone must not expose the shared sync for both error and output objects"
		}
		# An authorized read uses the runner's indexed grant before the control finish request completes.
		let read_job = job spawn {
			let job_id = job id
			let output = if $field == output {
				tg --url $runner.url --token $reader.token cat $object_id | complete
			} else {
				tg --url $runner.url --token $reader.token get $object_id | complete
			}
			$output | job send --tag $job_id 0
		}
		let read = try { job recv --tag $read_job --timeout 10sec } catch {
			error make { msg: $'($location) ($field) read did not finish after the index batch was released' }
		}
		success $read "the field grant must authorize the object before the control finish handler runs"
		assert ($read.stdout | str contains (if $field == output { 'output' } else { 'runner error' }))
		tg --url $owner.url --token $root_token checkpoint unwatch process.control.finish $control_watch
		failure (tg --url $runner.url --token $node_reader.token get $object_id | complete) "node permission must not grant access to the object"
		if $field == output {
			tg --url $owner.url --token $root_token index
			let log = tg --url $owner.url --token $root_token process log $process | str trim
			assert equal $log 'runner log' "the finish handler must preserve the process log after early runner indexing"
		}
	}
}
