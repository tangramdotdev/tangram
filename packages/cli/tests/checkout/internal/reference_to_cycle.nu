use ../../lib/test.nu *

# Checking out a reference to a directory that points into a cycle writes the directory into the checkouts directory.

let tmp = mktemp --directory

let local = server spawn --config { write: { checkout_pointers: false } }

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

let output = tg checkout $id

snapshot --path --entries [
	dir_01jct4bpr3p35ty9mvvp95smddcfdmt7r8pcqhqmsf9w7v487syfa0
	dir_01yvhnebp66wtrxzvd3se85wdv7m3gb9gny5fmv5k0csppvffv7y6g
] $local.checkout_directory
