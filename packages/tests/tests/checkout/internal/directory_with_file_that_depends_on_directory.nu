use ../../../lib/test.nu *

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
	dir_0174aq4yr28b8jspvnr74vze64av8snpdfcyred7ne1vw09khkpvd0
] $local.checkout_directory
