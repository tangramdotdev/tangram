use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
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
success $a_output
let a_id = $a_output.stdout | str trim

let b_output = tg build ($path + '#b') | complete
success $b_output
let b_id = $b_output.stdout | str trim

let c_output = tg build ($path + '#c') | complete
success $c_output
let c_id = $c_output.stdout | str trim

let d_output = tg build ($path + '#d') | complete
success $d_output
let d_id = $d_output.stdout | str trim

let e_output = tg build ($path + '#e') | complete
success $e_output
let e_id = $e_output.stdout | str trim

let f_output = tg build ($path + '#f') | complete
success $f_output
let f_id = $f_output.stdout | str trim

let g_output = tg build ($path + '#g') | complete
success $g_output
let g_id = $g_output.stdout | str trim

let h_output = tg build ($path + '#h') | complete
success $h_output
let h_id = $h_output.stdout | str trim

# Tag c and h.
tg tag c $c_id
tg tag h $h_id

# Clean.
let clean_output = tg clean | complete
success $clean_output

# Verify which objects were cleaned.
let a_get = tg object get $a_id | complete
failure $a_get

let b_get = tg object get $b_id | complete
failure $b_get

let c_get = tg object get $c_id | complete
success $c_get

let d_get = tg object get $d_id | complete
failure $d_get

let e_get = tg object get $e_id | complete
failure $e_get

let f_get = tg object get $f_id | complete
failure $f_get

let g_get = tg object get $g_id | complete
success $g_get

let h_get = tg object get $h_id | complete
success $h_get
