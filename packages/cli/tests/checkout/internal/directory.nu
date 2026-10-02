use ../../lib/test.nu *

# Checking out a directory object with a single file entry writes the directory into the checkouts directory.

let local = server spawn

let artifact = '
	tg.directory({
		"hello.txt": "Hello, World!"
	})
'
let id = tg put --no-tokens $artifact | referent node

let output = tg checkout $id

snapshot --path $local.checkout_directory
