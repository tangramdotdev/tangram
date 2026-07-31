use ../../test.nu *

# The tree stays on the exact remote selected by the initial get.

let zeta = spawn --cloud --name zeta
let alpha = spawn --cloud --name alpha --config {
	remotes: { zeta: { url: $zeta.url } }
}

tg --url $alpha.url group create foo
tg --url $alpha.url push --remote=zeta foo
tg --url $alpha.url group create foo/alpha
tg --url $zeta.url group create foo/zeta

let local = spawn --name local --config {
	remotes: {
		zeta: { url: $zeta.url }
		alpha: { url: $alpha.url }
	}
}

let output = tg --url $local.url tree 'foo?location=remote:alpha,remote:zeta' --depth 1
snapshot $output '
	foo
	└╴foo/alpha
'
