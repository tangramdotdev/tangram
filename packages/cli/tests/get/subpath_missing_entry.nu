use ../../test.nu *

# A reference with a get option that names an entry the directory does not contain fails.

let server = spawn

let dir = tg put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim

let output = tg get $"($dir)?get=nope.txt" | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to get the reference
	   reference = dir_01wfkxw25hxs3w1m4q1b505j4qrywc6d343zjf6z6pvspex7fczstg?get=nope.txt
	-> failed to get the reference
	   reference = dir_01wfkxw25hxs3w1m4q1b505j4qrywc6d343zjf6z6pvspex7fczstg?get=nope.txt

'
