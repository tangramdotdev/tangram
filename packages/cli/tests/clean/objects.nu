use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': '
		export let a = () => tg.directory({
			"b": tg.build(b),
			"c": tg.build(c),
			"d": tg.build(d),
			"e": tg.build(e),
		});
		export let b = () => tg.directory({
			"f": tg.build(f),
		});
		export let c = () => tg.directory({
			"g": tg.build(g),
		});
		export let d = () => tg.directory({
			"h": tg.build(h),
		});
		export let e = () => tg.file("e");
		export let f = () => tg.file("f");
		export let g = () => tg.file("g");
		export let h = () => tg.file("h");
	'
}

# Build all exports and get their object IDs.
let a_output = tg build ($path + '#a') | complete
assert equal $a_output.exit_code 0
let a_id = $a_output.stdout | str trim

let b_output = tg build ($path + '#b') | complete
assert equal $b_output.exit_code 0
let b_id = $b_output.stdout | str trim

let c_output = tg build ($path + '#c') | complete
assert equal $c_output.exit_code 0
let c_id = $c_output.stdout | str trim

let d_output = tg build ($path + '#d') | complete
assert equal $d_output.exit_code 0
let d_id = $d_output.stdout | str trim

let e_output = tg build ($path + '#e') | complete
assert equal $e_output.exit_code 0
let e_id = $e_output.stdout | str trim

let f_output = tg build ($path + '#f') | complete
assert equal $f_output.exit_code 0
let f_id = $f_output.stdout | str trim

let g_output = tg build ($path + '#g') | complete
assert equal $g_output.exit_code 0
let g_id = $g_output.stdout | str trim

let h_output = tg build ($path + '#h') | complete
assert equal $h_output.exit_code 0
let h_id = $h_output.stdout | str trim

# Tag c and h.
tg tag c $c_id
tg tag h $h_id

# Clean.
let clean_output = tg clean | complete
assert equal $clean_output.exit_code 0

# Verify which objects were cleaned.
let a_get = tg object get $a_id | complete
assert ($a_get.exit_code != 0)

let b_get = tg object get $b_id | complete
assert ($b_get.exit_code != 0)

let c_get = tg object get $c_id | complete
assert equal $c_get.exit_code 0

let d_get = tg object get $d_id | complete
assert ($d_get.exit_code != 0)

let e_get = tg object get $e_id | complete
assert ($e_get.exit_code != 0)

let f_get = tg object get $f_id | complete
assert ($f_get.exit_code != 0)

let g_get = tg object get $g_id | complete
assert equal $g_get.exit_code 0

let h_get = tg object get $h_id | complete
assert equal $h_get.exit_code 0
