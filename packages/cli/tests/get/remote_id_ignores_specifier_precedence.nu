use ../../test.nu *

# Getting by ID finds the exact remote node even when a different local node has the same specifier.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let remote_group = tg --url $remote.url group create foo | from json
let local_group = tg --url $local.url group create foo | from json
assert not equal $remote_group.id $local_group.id

let output = with-env { TANGRAM_QUIET: "false" } {
	tg --url $local.url get $remote_group.id | complete
}
success $output
let group = $output.stdout | from json
assert equal $group.id $remote_group.id
assert (($group | get --optional location) == null) "get should not print a location to stdout"
assert ($output.stderr | str contains "location=remote") "the referent should include its remote location"
