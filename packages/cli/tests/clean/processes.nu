use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export let a = async () => {
			await tg.build(c);
			await tg.build(b);
			return tg.file("a");
		};
		export let b = async () => {
			await tg.build(e);
			await tg.build(d);
			return tg.file("b");
		};
		export let c = () => tg.file("c");
		export let d = () => tg.file("d");
		export let e = () => tg.file("e");
	'
}

# Build all exports and get their process IDs.
let a_output = tg process spawn --sandbox ($path + '#a') | complete
success $a_output
let a_process = $a_output.stdout | str trim | from json | get process
let a_result = tg process output $a_process | complete
success $a_result

let b_output = tg process spawn --sandbox ($path + '#b') | complete
success $b_output
let b_process = $b_output.stdout | str trim | from json | get process

let c_output = tg process spawn --sandbox ($path + '#c') | complete
success $c_output
let c_process = $c_output.stdout | str trim | from json | get process

let d_output = tg process spawn --sandbox ($path + '#d') | complete
success $d_output
let d_process = $d_output.stdout | str trim | from json | get process

let e_output = tg process spawn --sandbox ($path + '#e') | complete
success $e_output
let e_process = $e_output.stdout | str trim | from json | get process

# Tag b and d.
tg tag b $b_process
tg tag d $d_process

# Clean.
let clean_output = tg clean | complete
success $clean_output

# Verify a was cleaned (should fail to get).
let a_get = tg process get $a_process | complete
failure $a_get

# Verify b is still there (should succeed).
let b_get = tg process get $b_process | complete
success $b_get

# Verify c was cleaned (should fail to get).
let c_get = tg process get $c_process | complete
failure $c_get

# Verify d is still there (should succeed).
let d_get = tg process get $d_process | complete
success $d_get

# Verify e is still there (should succeed).
let e_get = tg process get $e_process | complete
success $e_get
