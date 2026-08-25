use ../../../test.nu *

# Checking out a directory containing a symlink that points back into the directory, forming a cycle, writes the directory into the checkouts directory.

let server = server spawn --config { write: { checkout_pointers: false } }

# Create the artifact.
let artifact = artifact {
	tangram.ts: '
		export default function () {
			let graph = tg.graph({
				nodes: [
					{
						kind: "directory",
						entries: {
							link: { index: 1, kind: "symlink" }
						}
					},
					{
						kind: "symlink",
						artifact: { index: 0, kind: "directory" },
						path: "link"
					}
				]
			});
			return tg.symlink({
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
