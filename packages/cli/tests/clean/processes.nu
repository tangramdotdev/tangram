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
let a_process = run tg process spawn --sandbox ($path + '#a') | from json | get process
run tg process output $a_process

let b_process = run tg process spawn --sandbox ($path + '#b') | from json | get process

let c_process = run tg process spawn --sandbox ($path + '#c') | from json | get process

let d_process = run tg process spawn --sandbox ($path + '#d') | from json | get process

let e_process = run tg process spawn --sandbox ($path + '#e') | from json | get process

# Tag b and d.
tg tag b $b_process
tg tag d $d_process

# Clean.
run tg clean

# Verify a was cleaned (should fail to get).
let a_get = tg process get $a_process | complete
failure $a_get

# Verify b is still there (should succeed).
run tg process get $b_process

# Verify c was cleaned (should fail to get).
let c_get = tg process get $c_process | complete
failure $c_get

# Verify d is still there (should succeed).
run tg process get $d_process

# Verify e is still there (should succeed).
run tg process get $e_process
