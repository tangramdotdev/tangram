use ../../test.nu *

# Documenting a directory without a root module fails.

let server = spawn

let dir = mktemp --directory
"hello" | save ($dir | path join readme.txt)

let output = tg document $dir | complete
failure $output
snapshot ($output.stderr | redact $dir) '
	error an error occurred
	-> failed to find a root module
	   directory = dir_01me3xkeh1893mtxbmxsqhv8kxsds7y2hayhwy3ecqrfxvpzy2pz8g

'
