use ../../test.nu *

# A push succeeds after the remote's idle timeout closes the local server's pooled connection between pushes.

let remote = spawn --cloud --name remote --config {
	http: { idle_timeout: 3 }
}
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Push an object.
let a = tg put 'tg.file("a")' | str trim
let output = tg push $a | complete
success $output
wait_until { (tg --url $remote.url get $a --local | complete).exit_code == 0 } "the first object should be present on the remote"

# Wait past the remote's idle timeout plus a grace period so the remote closes the pooled connection.
sleep 5sec

# Push another object over the stale connection.
let b = tg put 'tg.file("b")' | str trim
let output = tg push $b | complete
success $output
wait_until { (tg --url $remote.url get $b --local | complete).exit_code == 0 } "the second object should be present on the remote"
