export def checkin-output [
	server: record
	path: string
	--no-solve
	--token: string
	--watch
] {
	mut options = { lock: null, root: true }
	if $no_solve {
		$options.solve = false
	}
	if $watch {
		$options.watch = true
	}
	let body = { options: $options, path: $path } | to json --raw
	mut headers = { Accept: 'text/event-stream', 'Content-Type': 'application/json' }
	if $token != null {
		$headers.Authorization = $'Bearer ($token)'
	}
	let socket = $server.url | str replace 'http+unix://' '' | url decode
	let response = (
		$body
		| into binary
		| http post
			--headers $headers
			--unix-socket $socket
			'http://localhost/checkin'
	)
	let event = (
		$response
		| into string
		| lines
		| where { |line| $line starts-with 'data: ' }
		| last
		| str substring 6..
		| from json
	)
	if $event.artifact? == null {
		error make { msg: 'the checkin failed' }
	}
	let uri = $"http://localhost/($event.artifact)" | url parse
	let artifact = $uri.path | str substring 1..

	{ artifact: $artifact, reference: $event.artifact }
}

# Return the access that a reference grants to a user without other permissions: `subtree` when the artifact's descendants are readable through it, `node` when only the artifact is, or `none`.
export def reference-access [reference: string, --token: string] {
	if (tg --token $token get --depth=inf $reference | complete).exit_code == 0 {
		return 'subtree'
	}
	if (tg --token $token get $reference | complete).exit_code == 0 {
		return 'node'
	}
	'none'
}
