use ../lib/test.nu *

# A push succeeds after the remote's idle timeout closes the local server's pooled connection between pushes.

let remote = server spawn --cloud --name remote --config {
	http: { idle_timeout: 0.5 }
}
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Push an object.
let a = tg put --no-tokens 'tg.file("a")' | referent node
let output = tg push $a | complete
success $output
success (tg --url $remote.url get $a --local | complete) "the first object should be present on the remote"

# Wait past the remote's idle timeout plus a grace period so the remote closes the pooled connection.
sleep 1sec

# Push another object over the stale connection.
let b = tg put --no-tokens 'tg.file("b")' | referent node
let output = tg push $b | complete
success $output
success (tg --url $remote.url get $b --local | complete) "the second object should be present on the remote"
