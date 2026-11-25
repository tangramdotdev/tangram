use ../../../util/test.nu *

# An executable in a directory finds its libraries relative to its own path while the same file is checked out on its own, which hard links the two files.

if $nu.os-info.name != 'macos' {
	skip_test 'this test requires macos'
}

let local = server spawn

# Build an executable that loads a library relative to its own path.
let path = artifact {
	'lib.c': 'int value(void) { return 0; }'
	'main.c': 'int value(void); int main(void) { return value(); }'
	tree: { bin: {}, lib: {} }
}
let library = $path | path join tree lib libvalue.dylib
let executable = $path | path join tree bin main
^cc -dynamiclib -install_name @rpath/libvalue.dylib -o $library ($path | path join lib.c)
^cc -Wl,-rpath,@loader_path/../lib -o $executable ($path | path join main.c) $library

# Check out the directory and the executable on its own.
let directory = tg checkin --no-tokens ($path | path join tree) | referent node
let file = tg checkin --no-tokens $executable | referent node
let directory_path = tg checkout $directory | str trim
let file_path = tg checkout $file | str trim
let executable = $directory_path | path join bin main
^$executable

# Run the executable while another process looks up the file that was checked out on its own.
let lookup = job spawn { ^sh -c 'while :; do stat -f %i "$1"; done' sh $file_path | ignore }
let outputs = 1..200 | each { ^$executable | complete }
job kill $lookup
let failures = $outputs | where exit_code != 0
assert ($failures | is-empty) $"the executable failed ($failures | length) of 200 times: ($failures | get -o 0.stderr)"
