use ../../test.nu *

let server = spawn

let artifact = '
	tg.directory({
		"hello.txt": "Hello, World!"
	})
'
let id = tg put $artifact

let output = tg cache $id

snapshot --path ($server.directory | path join "artifacts")
