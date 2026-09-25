use ../lib/test.nu *

# Synthetic checksum results have no sandbox and must not appear in sandbox process lists.

for location in [local remote] {
	let root_token = random chars
	let owner = server spawn --name $'owner-($location)' --config {
		advanced: { single_process: false },
		authentication: { root: { token: $root_token } },
		roles: (if $location == local { [api indexer runner scheduler] } else { [api indexer scheduler] }),
		runner: { process_state_ttl: 0 },
	}
	let runner = if $location == remote {
		let created = tg --url $owner.url --token $root_token runner create | from json
		server spawn --name runner --config {
			remotes: { default: { token: $root_token, url: $owner.url } },
			roles: [api indexer runner],
			runner: { id: $created.data.id, process_state_ttl: 0, remote: default, token: $created.token.token },
		}
	} else {
		$owner
	}
	let path = artifact { tangram.ts: 'export default () => tg.file("done");' }
	let first = tg --url $owner.url --token $root_token build --detach --checksum 'sha256:0000000000000000000000000000000000000000000000000000000000000000' $path | str trim
	tg --url $owner.url --token $root_token wait $first | ignore
	tg --url $owner.url --token $root_token index | ignore

	# Reusing a checksum mismatch creates a new result without assigning a sandbox.
	let second = tg --url $owner.url --token $root_token build --detach --cached=true --checksum 'sha256:1111111111111111111111111111111111111111111111111111111111111111' $path | str trim
	tg --url $owner.url --token $root_token wait $second | ignore
	let first_data = tg --url $owner.url --token $root_token get $first | from json
	let second_data = tg --url $owner.url --token $root_token get $second | from json
	let sandbox = $first_data.sandbox
	assert ($first != $second)
	assert equal $second_data.sandbox? null
	assert equal $second_data.started_at? null
	assert equal $second_data.status finished
	tg --url $owner.url --token $root_token index | ignore

	tg --url $owner.url --token $root_token sandbox wait --source=index $sandbox | ignore

	let socket = $runner.url | str replace 'http+unix://' '' | url decode
	let headers = if $location == local { { Authorization: $'Bearer ($root_token)' } } else { {} }
	for source in [auto runner index] {
		let processes = http get --raw --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/sandboxes/($sandbox)/processes?source=($source)&location=($location)&size=1&timeout=0'
		let chunks = $processes | lines | where { str starts-with 'data: {' } | each { str replace 'data: ' '' | from json }
		assert equal ($chunks | get position) [0]
		assert equal ($chunks | get data | flatten) [$first]
		assert ($processes | str contains 'event: end')

		let tail = http get --raw --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/sandboxes/($sandbox)/processes?source=($source)&location=($location)&position=end.-1&timeout=0'
		assert ($tail | str contains $first)
		assert (not ($tail | str contains $second))
	}
}
