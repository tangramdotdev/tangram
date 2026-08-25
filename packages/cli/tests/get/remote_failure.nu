use ../../test.nu *

# Getting a group fails when any queried remote fails, even if the preferred remote has the group.

let alpha = server spawn --cloud --name alpha
let zeta = server spawn --cloud --name zeta
let local = server spawn --name local --config {
	remotes: {
		alpha: { url: $alpha.url }
		zeta: { url: $zeta.url }
	}
}

tg --url $alpha.url group create foo | ignore

let pid = open ($zeta.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the zeta remote should stop"

let output = tg --url $local.url get foo | complete
failure $output
