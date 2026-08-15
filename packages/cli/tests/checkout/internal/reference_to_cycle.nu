use ../../../test.nu *

# Checking out a reference to a directory that points into a cycle writes the directory into the checkouts directory.

let tmp = mktemp --directory

let server = spawn --config { write: { checkout_pointers: false } }

let artifact = artifact {
	tangram.ts: '
		export default function () {
			let graph = tg.graph({
				nodes: [
					{ kind: "directory", entries: { "b": 1 } },
					{ kind: "directory", entries: { "c": 2 } },
					{ kind: "file", dependencies: { "a": 0 } },
				]
			})
			return tg.directory({ graph, index: 1, kind: "directory" });
		}
	'
}
let id = tg build --no-checkout-pointers $artifact
rm --recursive --force $server.checkout_directory
mkdir $server.checkout_directory

let output = tg checkout $id

snapshot --path $server.checkout_directory
