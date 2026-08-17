use ../../../test.nu *

# Checking out two files that depend on each other, forming a cycle, writes the files into the checkouts directory.

let server = spawn --config { write: { checkout_pointers: false } }

let path = artifact {
	tangram.ts: r#'
		export default function () {
			return tg.file({
				graph: tg.graph({
					nodes: [
						{ kind: "file", dependencies: { "./bar.tg.ts": 1 } },
						{ kind: "file", dependencies: { "./foo.tg.ts": 0 } },
					]
				}),
				index: 0,
				kind: "file",
			})
		}
	'#
}
let id = tg build --no-checkout-pointers $path
rm --recursive --force $server.checkout_directory
mkdir $server.checkout_directory

# Check out.
let output = tg checkout $id

# Snapshot.
snapshot --path $server.checkout_directory
