use ../../../test.nu *

# Checking out a directory defined through a graph node writes the directory into the checkouts directory.

let server = spawn --config { write: { checkout_pointers: false } }

# Create the artifact.
let artifact = artifact {
	tangram.ts: '
		export default function () {
			let graph = tg.graph({
				nodes: [
					{
						kind: "directory",
						entries: {
							"hello.txt": tg.file("Hello, World!")
						}
					}
				]
			});
			return tg.directory({
				graph,
				index: 0,
				kind: "directory"
			});
		}
	'
}
let id = tg checkin --no-checkout-pointers $artifact
let id = tg build $id
rm --recursive --force $server.checkout_directory
mkdir $server.checkout_directory

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory
