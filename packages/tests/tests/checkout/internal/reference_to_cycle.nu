use ../../../util/test.nu *

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
	dir_016grye6yqhgjr7dj2qqx1rwd4rkq4qtjfwfpa8cmmmc3219gtzdtg
	dir_01rh3fmkfgtg2a0k12ftq71epevr3xt39nr5fc8k614wbgd1rprwyg
] $local.checkout_directory
