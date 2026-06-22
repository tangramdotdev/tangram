use ../../test.nu *

# A reference with a get option on a file fails because only directories support subpaths.

let server = spawn

let file = tg put 'tg.file("solo")' | str trim

let output = tg get $"($file)?get=foo" | complete
failure $output
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to get the reference
	   reference = fil_01hywsaeq340a2qtmce8y2cxddkj8bshswktye8j3mvj6bgnqc3hk0?get=foo
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to get the reference
	   reference = fil_01hywsaeq340a2qtmce8y2cxddkj8bshswktye8j3mvj6bgnqc3hk0?get=foo
	-> unexpected reference get option

'
