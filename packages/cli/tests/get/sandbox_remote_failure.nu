use ../../test.nu *

# Getting a sandbox fails when any queried remote fails, even if another remote has the sandbox.

let alpha = spawn --cloud --name alpha
let zeta = spawn --cloud --name zeta
let local = spawn --name local --config {
	remotes: {
		alpha: { url: $alpha.url }
		zeta: { url: $zeta.url }
	}
}

let sandbox = tg --url $alpha.url sandbox create --no-network | str trim

let pid = open ($zeta.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the zeta remote should stop"

let output = tg --url $local.url get $sandbox | complete
failure $output
