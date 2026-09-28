use ../lib/test.nu *

# The tree stays on the exact remote selected by the initial get.

let remote_zeta = server spawn --cloud --name remote-zeta
let remote_alpha = server spawn --cloud --name remote-alpha --config {
	remotes: { zeta: { url: $remote_zeta.url } }
}

tg --url $remote_alpha.url group create foo
tg --url $remote_alpha.url push --remote=zeta foo
tg --url $remote_alpha.url group create foo/alpha
tg --url $remote_zeta.url group create foo/zeta

let local = server spawn --name local --config {
	remotes: {
		zeta: { url: $remote_zeta.url }
		alpha: { url: $remote_alpha.url }
	}
}

let output = tg --url $local.url tree 'foo?location=remote:alpha,remote:zeta' --depth 1
snapshot $output '
	foo
	└╴foo/alpha
'
