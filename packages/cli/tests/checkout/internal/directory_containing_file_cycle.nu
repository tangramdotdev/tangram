use ../../../test.nu *

# Checking out a directory containing a file that participates in a dependency cycle writes the directory into the checkouts directory.

let server = server spawn --config { write: { checkout_pointers: false } }

let path = artifact {
	tangram.ts: r#'
		export default function () {
			let graph = tg.graph({
				nodes: [
					{ kind: "file", dependencies: { "./bar.tg.ts": 1 } },
					{ kind: "file", dependencies: { "./foo.tg.ts": 0 } },
				]
			});
			let foo = tg.file({
				graph,
				index: 0,
				kind: "file",
			});
			return tg.directory({
				foo,
			});
		}
	'#
}
let id = tg build --no-checkout-pointers $path
rm --recursive --force $server.checkout_directory
mkdir $server.checkout_directory

# Check out.
tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory
