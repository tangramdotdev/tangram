use ../lib/test.nu *

# A reference with a get option that names an entry the directory does not contain fails.

let local = server spawn

let dir = tg put --no-tokens 'tg.directory({ "hello.txt": tg.file("hello") })' | referent node

let output = tg get $"($dir)?get=nope.txt" | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the reference
	   reference = dir_01jj08423k0g8xdtzxdw2ezy5fjd2nyss1y7qsp6bejvgdaht0y5g0?get=nope.txt

'
