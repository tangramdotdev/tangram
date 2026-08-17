use ../../../test.nu *

# Checking out a directory containing a file that depends on its enclosing directory writes the directory into the checkouts directory.

let server = spawn --config { write: { checkout_pointers: false } }

let path = artifact {
	tangram.ts: r#'
		export default function () {
			return tg.directory({
				graph: tg.graph({
					nodes: [
						{ kind: "directory", entries: { "tangram.ts": 1 } },
						{ kind: "file", dependencies: { ".": 0 } },
					]
				}),
				index: 0,
				kind: "directory",
			})
		}
	'#
}
let id = tg build --no-checkout-pointers $path
rm --recursive --force $server.checkout_directory
mkdir $server.checkout_directory

# Check out.
tg checkout $id

snapshot --path $server.checkout_directory
