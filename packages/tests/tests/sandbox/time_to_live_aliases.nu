use ../../util/test.nu *

# The canonical and shorthand lifetime flags produce the same canonical sandbox data.
let local = server spawn --config { sandbox: { ttl: 0 }, runner: { sandbox_state_ttl: 0 } }

for flag in [--time-to-live --ttl] {
	let id = tg sandbox create --no-tokens $flag 1h | referent node
	let data = tg sandbox get $id | from json | get data
	assert equal $data.time_to_live 3600
	assert ("ttl" not-in $data)
	tg sandbox destroy $id
}

for flag in [--no-time-to-live --no-ttl] {
	let id = tg sandbox create --no-tokens $flag | referent node
	let data = tg sandbox get $id | from json | get data
	assert ($data.time_to_live? | is-empty)
	assert ("ttl" not-in $data)
	tg sandbox destroy $id
}
