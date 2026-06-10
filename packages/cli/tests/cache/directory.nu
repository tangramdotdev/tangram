use ../../test.nu *

# Caching a directory object with a single file entry writes the directory into the artifacts cache.

let server = spawn

let artifact = '
	tg.directory({
		"hello.txt": "Hello, World!"
	})
'
let id = tg put $artifact

let output = tg cache $id

snapshot --path ($server.directory | path join "artifacts")
