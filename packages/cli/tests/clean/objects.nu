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
let a_id = run tg build ($path + '#a')

let b_id = run tg build ($path + '#b')

let c_id = run tg build ($path + '#c')

let d_id = run tg build ($path + '#d')

let e_id = run tg build ($path + '#e')

let f_id = run tg build ($path + '#f')

let g_id = run tg build ($path + '#g')

let h_id = run tg build ($path + '#h')

# Tag c and h.
run tg tag c $c_id
run tg tag h $h_id

# Clean.
run tg clean

# Verify which objects were cleaned.
let a_get = tg object get $a_id | complete
failure $a_get

let b_get = tg object get $b_id | complete
failure $b_get

run tg object get $c_id

let d_get = tg object get $d_id | complete
failure $d_get

let e_get = tg object get $e_id | complete
failure $e_get

let f_get = tg object get $f_id | complete
failure $f_get

run tg object get $g_id

run tg object get $h_id
