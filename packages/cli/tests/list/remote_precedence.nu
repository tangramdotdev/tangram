use ../../test.nu *

# Remotes are queried concurrently and conflicting results prefer the alphabetically first remote.

let alpha = spawn --cloud --name alpha
let zeta = spawn --cloud --name zeta
let local = spawn --name local --config {
	remotes: {
		zeta: { url: $zeta.url }
		alpha: { url: $alpha.url }
	}
}

let alpha_group = tg --url $alpha.url group create foo | from json
tg --url $zeta.url group create foo | ignore

let group = tg --url $local.url get foo | from json
assert equal $group.id $alpha_group.id
assert equal $group.location "remote:alpha"
