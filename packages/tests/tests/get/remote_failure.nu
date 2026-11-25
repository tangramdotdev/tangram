use ../../util/test.nu *

# Getting a group fails when any queried remote fails, even if the preferred remote has the group.

let remote_alpha = server spawn --cloud --name remote-alpha
let remote_zeta = server spawn --cloud --name remote-zeta
let local = server spawn --name local --config {
	remotes: {
		alpha: { url: $remote_alpha.url }
		zeta: { url: $remote_zeta.url }
	}
}

tg --url $remote_alpha.url group create foo | ignore

let pid = server_pid $remote_zeta.directory
kill --signal 2 $pid
wait_for_exit $pid "the zeta remote should stop"

let output = tg --url $local.url get foo | complete
failure $output
