use ../../test.nu *

# Remotes are queried concurrently and conflicting results prefer the alphabetically first remote.

let alpha = server spawn --cloud --name alpha
let zeta = server spawn --cloud --name zeta
let local = server spawn --name local --config {
	remotes: {
		zeta: { url: $zeta.url }
		alpha: { url: $alpha.url }
	}
}

let alpha_group = tg --url $alpha.url group create foo | from json
tg --url $zeta.url group create foo | ignore

let output = with-env { TANGRAM_QUIET: "false" } { tg --url $local.url get foo | complete }
success $output
let group = $output.stdout | from json
assert equal $group.id $alpha_group.id
assert (($group | get --optional location) == null) "get should not print a location to stdout"
assert ($output.stderr | str contains "location=") "the referent should include its location"
assert ($output.stderr | str contains "alpha") "the referent should identify the alpha remote"
