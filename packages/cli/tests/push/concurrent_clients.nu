use ../../test.nu *

let remote = spawn --cloud -n remote
let num_clients = 10

# Create client servers and build artifacts for each.
let clients = 0..<$num_clients | each { |i|
	# Create a client server.
	let client = spawn -n $"client_($i)"

	# Add the remote to this client.
	tg -u $client.url remote put default $remote.url

	# Create a unique artifact with a reasonably large object tree.
	# Each client gets a different structure to ensure different objects.
	let path = artifact {
		tangram.ts: r#'
			export default async (name: string) => {
				const createDir = async (depth: number, breadth: number): Promise<tg.Directory> => {
					let entries : { [key: string]: tg.Artifact } = {}
					if (depth == 0) {
						for (let i = 0; i < breadth; i++) {
							entries[`_${i}`] = await tg.file(`${Math.random()}`);
						}
					} else {
						for (let i = 0; i < breadth; i++) {
							entries[`_${i}`] = await createDir(depth - 1, breadth);
						}
					}
					return tg.directory({ entries });
				};
				return await createDir(4, 4);
			};
		'#
	}

	# Build the artifact on this client.
	let id = tg -u $client.url build $path $i
	tg -u $client.url index

	{ client: $client, id: $id, index: $i }
}

# Push concurrently from all clients using jobs.
# Each job will push its artifact to the shared remote.
let jobs = $clients | each { |entry|
	job spawn {
		tg -u $entry.client.url push $entry.id --eager | complete
	}
}

# Wait for all push jobs to complete.
loop {
	let is_done = job list | where id in $jobs | is-empty
	if $is_done { break; }
	sleep 1sec
}

# Index the remote after all pushes.
tg -u $remote.url index

# Verify all objects are on the remote.
for entry in $clients {
	# Check that the root directory is present on the remote.
	let client_data = tg -u $entry.client.url get $entry.id --pretty
	let remote_data = tg -u $remote.url get $entry.id --pretty
	assert equal $client_data $remote_data

	# Check metadata matches.
	let client_metadata = tg -u $entry.client.url object metadata $entry.id --pretty
	let remote_metadata = tg -u $remote.url object metadata $entry.id --pretty
	assert equal $client_metadata $remote_metadata
}
