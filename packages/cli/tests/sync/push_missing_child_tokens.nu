use ../lib/test.nu *

# A push with subtree authorization can use a missing child stored at the destination.

def test [hold_verification: bool ...args] {
	let remote = server spawn --name remote --config {
		advanced: { checkpoints: true },
	}
	let root_token = random chars
	let local = server spawn --name local --config {
		advanced: { checkpoints: true },
		authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
		verification: { permissions: { initial: false, final: false } },
	}

	# Hold the remote's index writes.
	let batch_watch = (
		tg --url $remote.url checkpoint watch index.batch
		| from json
		| get watch
	)

	# Put the child and the parent on the remote.
	let file = tg --url $remote.url put --no-tokens 'tg.file("hello")' | referent node
	let directory = tg --url $remote.url put --no-tokens 'tg.directory({ "hello.txt": tg.file("hello") })' | referent node
	tg --url $remote.url checkpoint wait index.batch $batch_watch 0 | ignore

	# Put the parent on the local server without the child.
	tg --url $remote.url get --bytes $directory | tg --url $local.url --token $root_token put --no-tokens --bytes --kind dir | referent node

	# Hold the destination's fallback before it awaits the queued index writes.
	let retry_watch = (
		tg --url $remote.url checkpoint watch sync.get.pending.index --params ({ id: $file } | to json)
		| from json
		| get watch
	)

	# Give Alice a token for the directory subtree.
	let alice = tg --url $local.url login --verbose --name alice | from json
	tg --url $local.url --token $alice.token remote put default $remote.url
	let socket = $local.url | str replace 'http+unix://' '' | url decode
	let token = (http get --headers { Accept: 'application/json', Authorization: $'Bearer ($root_token)' } --unix-socket $socket $'http://localhost/objects/($directory)' | get tokens.local.0)
	let referent = $'($directory)?tokens[local][0]=($token | url encode --all)'

	# Hold storage verification so destination availability must cancel it.
	let verification_watch = if $hold_verification {
		tg --url $local.url --token $root_token checkpoint watch verification.index --params ({ resource: $file, storage: true } | to json)
		| from json
		| get watch
	} else { null }

	# Push the parent.
	let push = job spawn {
		let job_id = job id
		let output = tg --url $local.url --token $alice.token push ...$args $referent | complete
		$output | job send --tag $job_id 0
	}

	if $hold_verification {
		tg --url $local.url --token $root_token checkpoint wait verification.index $verification_watch 0 | ignore
	}

	# Release the retry and the pending index writes once the destination starts waiting.
	tg --url $remote.url checkpoint wait sync.get.pending.index $retry_watch 0 | ignore
	tg --url $remote.url checkpoint unwatch sync.get.pending.index $retry_watch
	tg --url $remote.url checkpoint unwatch index.batch $batch_watch

	let output = job recv --tag $push --timeout 30sec
	success $output
	if $hold_verification {
		tg --url $local.url --token $root_token checkpoint unwatch verification.index $verification_watch
	}

	# The transfer must still fail when neither side has the child.
	let empty = server spawn --name empty
	tg --url $local.url --token $alice.token remote put empty $empty.url
	let output = timeout 10s tg --url $local.url --token $alice.token push --remote=empty ...$args $referent | complete
	failure $output 'the destination must not accept an incomplete directory'
	assert ($output.exit_code != 124) 'the missing child should fail without hanging'
}

test true "--eager"
test true "--lazy"
test false "--eager"
test false "--lazy"
