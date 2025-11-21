use ../../test.nu *

let server = spawn

# Create the artifact.
let artifact = '
	tg.file("Hello, World!")
'
let id = run tg put $artifact

# Cache.
run tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
