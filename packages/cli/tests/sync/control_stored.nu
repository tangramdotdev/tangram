use ../../test.nu *

# Storing a node answers every retained request while an unrelated node keeps the sync open.
for kind in [object process] {
	let root_token = random chars
	let store = { object_concurrency: 8, object_max_batch: 1 }
	let remote = server spawn --name $'remote-($kind)' --config {
		advanced: { checkpoints: true },
		authentication: { root: { token: $root_token } },
		sync: {
			control: { index_timeout: 60, request_timeout: 120 },
			get: { store: { lmdb: $store, memory: $store, scylla: $store } },
		},
	}
	let local = server spawn --name $'local-($kind)' --config {
		advanced: { checkpoints: true },
		authentication: { root: { token: $root_token } },
		remotes: { default: { token: $root_token, url: $remote.url } },
	}
	let node = if $kind == object {
		tg --url $local.url --token $root_token put 'tg.blob("stored")' | str trim
	} else {
		let process = 'pcs_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0'
		let data = {
			children: [],
			command: 'cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0',
			created_at: 0,
			finished_at: 0,
			host: 'test',
			output: 5,
			sandbox: 'sbx_00041061050r3gg28a1c60t3gf20',
			status: finished,
		}
		tg --url $local.url --token $root_token process put $process ($data | to json)
		$process
	}
	let blocker = tg --url $local.url --token $root_token put 'tg.blob("later")' | str trim
	let socket = $remote.url | str replace 'http+unix://' '' | url decode
	let headers = { Accept: 'application/json', Authorization: $'Bearer ($root_token)' }

	# Hold the node before storage and leave a second object held until every read has completed.
	let checkpoint = if $kind == object { 'sync.get.store.object' } else { 'sync.put.store.process' }
	let url = if $kind == object { $remote.url } else { $local.url }
	let watch = tg --url $url --token $root_token checkpoint watch $checkpoint --params ({ id: $node } | to json --raw) | from json | get watch
	let blocker_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blocker } | to json --raw) | from json | get watch
	let ack_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.ack --params ({ node: $node } | to json --raw) | from json | get watch
	let blocker_ack_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.ack --params ({ node: $blocker } | to json --raw) | from json | get watch
	let push_log = $env.TMPDIR | path join $'push-($kind).log'
	let push = job spawn {
		let job_id = job id
		let output = tg --no-quiet --url $local.url --token $root_token push $node $blocker o+e>| tee { save --force $push_log } | complete
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $url --token $root_token checkpoint wait $checkpoint $watch 0 | ignore
	wait_until { (open --raw $push_log) =~ 'tokens\[remote\]\[sync\][^\r\n]*\r?\n' } 'the push should log the complete referent with the sync token'
	let referent = open --raw $push_log | lines | where {|line| $line =~ 'tokens\[remote\]\[sync\]' } | first | str trim
	let sync = $'http://localhost/($referent)' | url parse | get params | where key == 'tokens[remote][sync][0]' | first | get value
	let query = { 'tokens[local][sync][0]': $sync } | url build-query
	let endpoint = if $kind == object { 'objects' } else { 'processes' }
	let uri = $'http://localhost/($endpoint)/($node)?($query)'

	# Each read has its own client lease and must receive an acknowledgement before storage resumes.
	let reads = 0..3 | each {
		job spawn {
			let job_id = job id
			let output = http get --max-time 30sec --unix-socket $socket --headers $headers $uri
			$output | job send --tag $job_id 0
		}
	}
	let leases = 0..3 | each {|index|
		timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.control.ack $ack_watch $index | from json | get params.lease
	}
	assert equal ($leases | uniq | length) 4 'the reads should use separate client leases'
	let blocked_read = job spawn {
		let job_id = job id
		let output = http get --max-time 30sec --unix-socket $socket --headers $headers $'http://localhost/objects/($blocker)?($query)'
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.control.ack $blocker_ack_watch 0 | ignore
	tg --url $remote.url --token $root_token checkpoint unwatch sync.control.ack $ack_watch
	tg --url $remote.url --token $root_token checkpoint unwatch sync.control.ack $blocker_ack_watch
	tg --url $url --token $root_token checkpoint unwatch $checkpoint $watch

	# All waiting reads complete before the held object allows the sync to finish.
	for read in $reads {
		let output = job recv --tag $read --timeout 10sec
		if $kind == object {
			assert equal ($output.data.value.bytes | decode base64 | decode utf-8) 'stored'
		} else {
			assert equal $output.data.output 5
		}
	}
	timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $blocker_watch 0 | ignore
	let premature = try { job recv --tag $blocked_read --timeout 1sec } catch { null }
	assert equal $premature null 'the unrelated read must remain pending'
	let premature = try { job recv --tag $push --timeout 1sec } catch { null }
	assert equal $premature null 'the sync must still be open when the reads finish'

	# A late read succeeds without another storage event.
	let output = http get --max-time 10sec --unix-socket $socket --headers $headers $uri
	if $kind == object {
		assert equal ($output.data.value.bytes | decode base64 | decode utf-8) 'stored'
	} else {
		assert equal $output.data.output 5
	}
	tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $blocker_watch
	job recv --tag $blocked_read --timeout 10sec | ignore
	success (job recv --tag $push --timeout 10sec) 'the push should finish once the last object is released'
}
