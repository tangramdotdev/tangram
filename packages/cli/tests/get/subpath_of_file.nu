use ../../test.nu *

# A reference with a get option on a file fails because only directories support subpaths.

let server = spawn

let file = tg put 'tg.file("solo")' | str trim

let output = tg get $"($file)?get=foo" | complete
failure $output
assert ($output.stderr | str contains "unexpected reference get option") "the error should mention the get option"
