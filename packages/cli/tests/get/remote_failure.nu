use ../../test.nu *

# Getting a named item fails when any queried remote fails, even if the preferred remote has the item.

let alpha = spawn --cloud --name alpha
let zeta = spawn --cloud --name zeta
let local = spawn --name local --config {
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
