use ../../test.nu *

# Repeated object puts should not get slower as the same shared child gains more fresh parents.

let server = spawn --config { tokio_single_threaded: false, v8_thread_pool_size: 8 }

def wrapper [trial: int, index: int] {
	[
		'"w',
		($index | into string),
		'": tg.directory({ "shared": tg.file("shared"), "salt": tg.file("',
		($trial | into string),
		'-',
		($index | into string),
		'") })',
	] | str join
}

mut rows = []
for trial in 0..<20 {
	let entries = (0..<64 | each {|index| wrapper $trial $index } | str join ", ")
	let expr = (["tg.directory({ ", $entries, " })"] | str join)
	let t0 = (date now)
	let out = (tg put $expr | complete)
	let indexed = (tg index | complete)
	let ns = (((date now) - $t0) | into int)
	let exit = if $out.exit_code != 0 { $out.exit_code } else { $indexed.exit_code }
	$rows = ($rows | append { trial: $trial, secs: ($ns / 1_000_000_000 | math round -p 2), exit: $exit, ns: $ns })
}
print ($rows | select trial secs exit)

let failed = ($rows | where exit != 0)
assert (($failed | length) == 0) $"($failed | length) of 20 object puts failed"

let first = ($rows | first 5 | get ns | math avg)
let last = ($rows | last 5 | get ns | math avg)
let ratio = ($last / $first)
let ratio_rounded = ($ratio | math round -p 2)
print $"first=(($first / 1_000_000_000) | math round -p 2)s last=(($last / 1_000_000_000) | math round -p 2)s ratio=($ratio_rounded)x"
assert ($ratio < 2.5) $"object puts compounded ($ratio_rounded)x: grant propagation is super-linear for repeated grants on shared objects"
