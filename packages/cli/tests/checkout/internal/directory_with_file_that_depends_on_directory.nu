use ../../lib/test.nu *

# Checking out a directory containing a file that depends on its enclosing directory writes the directory into the checkouts directory.

let local = server spawn --config { write: { checkout_pointers: false } }

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

# Check out.
tg checkout $id

snapshot --path --entries [
	dir_01m8h84t7pgq9dzea9zvwvtm7hr2wfkkf1nxbcpvyrwk3xa75r55c0
] $local.checkout_directory
