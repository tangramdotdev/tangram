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
let a_process = tg process spawn --sandbox ($path + '#a') | str trim
tg process output $a_process

let b_process = tg process spawn --sandbox ($path + '#b') | str trim

let c_process = tg process spawn --sandbox ($path + '#c') | str trim

let d_process = tg process spawn --sandbox ($path + '#d') | str trim

let e_process = tg process spawn --sandbox ($path + '#e') | str trim

tg wait $a_process
tg wait $b_process
tg wait $c_process
tg wait $d_process
tg wait $e_process

# Tag b and d.
tg tag b $b_process
tg tag d $d_process

# Clean.
tg clean

# Verify a was cleaned (should fail to get).
let a_get = tg process get $a_process | complete
failure $a_get

# Verify b is still there (should succeed).
tg process get $b_process

# Verify c was cleaned (should fail to get).
let c_get = tg process get $c_process | complete
failure $c_get

# Verify d is still there (should succeed).
tg process get $d_process

# Verify e is still there (should succeed).
tg process get $e_process
