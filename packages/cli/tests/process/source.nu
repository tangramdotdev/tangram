use ../lib/test.nu *

# Source selection distinguishes live completion from indexed completion locally and across remotes.
for remote_runner in [false true] {
	let local_owner = server spawn --name local-owner --config {
		advanced: { checkpoints: true },
		control: { read_timeout: 0.25 },
		roles: (if $remote_runner { [api indexer scheduler] } else { [api indexer runner scheduler] }),
	}
	let runner = if $remote_runner {
		let created = tg --url $local_owner.url runner create | from json
		server spawn --name runner --config {
			advanced: { checkpoints: true },
			remotes: { default: { url: $local_owner.url } },
			roles: [api indexer runner],
			runner: { id: $created.data.id, remote: default, token: $created.token.token },
		}
	} else {
		$local_owner
	}
	let local_client = server spawn --name local-client --config { control: { read_timeout: 0.25 }, remotes: { default: { url: $local_owner.url } } }
	let finish = tg --url $runner.url checkpoint watch runner.process.control.finish.request | from json | get watch
	let path = artifact { tangram.ts: 'export default () => { console.log("source log"); return "done"; };' }
	let process = tg --url $local_owner.url build --no-tokens --detach $path | referent node
	timeout 30s tg --url $runner.url checkpoint wait runner.process.control.finish.request $finish 0 | ignore

	# Runner reads observe completion while the authoritative index is started; the client caches the finished record.
	for url in [$local_owner.url $local_client.url] {
		let output = timeout 10s tg --url $url wait $process | from json
		assert equal $output.output done
		let output = timeout 10s tg --url $url wait --source=runner $process | from json
		assert equal $output.output done
		assert equal (tg --url $url get --source=runner $process | from json | get status) finished
		let indexed_status = if $url == $local_owner.url { 'started' } else { 'finished' }
		assert equal (tg --url $url get --source=index $process | from json | get status) $indexed_status
		assert equal (tg --url $url process status --source=runner $process | from json) [finished]
		assert equal (tg --url $url process status --source=index $process | from json) [$indexed_status]
		assert equal (tg --url $url process children --source=runner $process | from json) []
	}

	# An index wait must stay pending after reading the unfinished indexed data.
	let watch = tg --url $local_owner.url checkpoint watch process.get.index | from json | get watch
	let waiter = job spawn {
		let job_id = job id
		let output = timeout 30s tg --url $local_owner.url wait --source=index $process | complete
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $local_owner.url checkpoint wait process.get.index $watch 0 | ignore
	tg --url $local_owner.url checkpoint unwatch process.get.index $watch
	assert equal (try { job recv --tag $waiter --timeout 300ms } catch { null }) null
	tg --url $runner.url checkpoint unwatch runner.process.control.finish.request $finish
	let output = job recv --tag $waiter --timeout 30sec
	success $output
	assert equal ($output.stdout | from json | get output) done

	# Indexed completion makes the finished log available.
	tg --url $local_owner.url index
	assert ((tg --url $local_owner.url get --source=index $process | from json | get log?) | is-not-empty)
	assert equal (tg --url $local_client.url process children --source=index $process | from json) []
	assert equal (tg --url $local_client.url output --source=index $process | from json) done

	# A runner-only read must not fall back to the index after runner state is discarded.
	let runner = server restart $runner
	success (tg --url $local_owner.url get --source=index $process | complete)
	for operation in [get wait] {
		let output = timeout 10s tg --url $local_owner.url $operation --source=runner $process | complete
		failure $output
		assert ($output.exit_code != 124) "a missing runner must be reported without falling back to the index"
	}
}
