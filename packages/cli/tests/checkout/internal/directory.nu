use ../../../test.nu *

# Checking out a directory object with a single file entry writes the directory into the checkouts directory.

let server = server spawn

let artifact = '
	tg.directory({
		"hello.txt": "Hello, World!"
	})
'
let id = tg put $artifact

let output = tg checkout $id

snapshot --path $server.checkout_directory
