use ../../util/test.nu *

# A push succeeds after the remote's idle timeout closes the local server's pooled connection between pushes.

let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	http: { idle_timeout: 0.5 },
}
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Push an object.
let idle = checkpoint watch http.connection.idle --url $remote.url
let a = tg put --no-tokens 'tg.file("a")' | referent node
let output = tg push $a | complete
success $output
success (tg --url $remote.url get $a --local | complete) "the first object should be present on the remote"

# Wait for the remote's idle timeout to close the pooled connection.
checkpoint pass $idle
checkpoint unwatch $idle

# Push another object over the stale connection.
let b = tg put --no-tokens 'tg.file("b")' | referent node
let output = tg push $b | complete
success $output
success (tg --url $remote.url get $b --local | complete) "the second object should be present on the remote"
