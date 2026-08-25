use ../../test.nu *

# Checking a directory without a root module fails.

let server = server spawn

let dir = mktemp --directory
"hello" | save ($dir | path join readme.txt)

let output = tg check $dir | complete
failure $output
snapshot --normalize --redact $dir $output.stderr '
	error an error occurred
	-> failed to find a root module
	   directory = dir_01f2z5nnejmwayg7y6rhmypyxkqb1pyxzytmjb0bhgrsagfbvz74hg

'
