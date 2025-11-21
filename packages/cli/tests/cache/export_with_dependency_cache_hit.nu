use ../../test.nu *

# Test that when building on a clean server with a cache hit from remote,
# all dependency artifacts are pulled and accessible.

# Create a remote server.
let remote_server = spawn -n remote

# Create a local server.
let local_server = spawn -n local

# Add the remote to the local server.
let output = tg -u $local_server.url remote put default $remote_server.url | complete
success $output

# Create an export that returns a file with nested dependencies.
# Main file -> Dependency A -> Dependency B
let path = artifact {
	tangram.ts: '
		export default async () => {
			// Create the deepest dependency (Dependency B)
			let dependencyB = await tg.file("dependency B contents");

			// Create Dependency A that depends on Dependency B
			let dependencyA = await tg.file({
				contents: "dependency A contents",
				dependencies: {
					"depB": {
						item: dependencyB
					}
				}
			});

			// Create main file that depends on Dependency A
			return tg.file({
				contents: "main file contents",
				dependencies: {
					"depA": {
						item: dependencyA
					}
				}
			});
		}
	'
}

# Tag and push the artifact.
let packageTag = "package"
let output = tg -u $local_server.url tag $packageTag $path | complete
success $output
let output = tg -u $local_server.url push $packageTag | complete
success $output

# Build the module on the local server.
let processTag = "process"
let id = tg -u $local_server.url build $packageTag --tag $processTag | complete | get stdout | str trim

# Push the build result to the remote server.
let output = tg -u $local_server.url push $processTag | complete
success $output

# Create a clean server.
let clean_server = spawn -n clean

# Add the remote to the clean server.
let output = tg -u $clean_server.url remote put default $remote_server.url | complete
success $output

# Build on the clean server. This should get a cache hit from the remote.
let clean_id = tg -u $clean_server.url build $path | complete | get stdout | str trim

# The IDs should match (confirming cache hit).
if $id != $clean_id {
	error make { msg: "IDs do not match - cache hit did not occur" }
}

let output = tg -u $clean_server.url checkout $id | complete
success $output

# Snapshot the artifacts directory. If transitive dependencies are missing, they won't be in the snapshot.
snapshot --path ($clean_server.directory | path join 'artifacts')
