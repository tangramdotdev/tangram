use ../../lib/test.nu *

# Checking out a directory whose two entries reference each other within the same graph writes the directory into the checkouts directory.

let local = server spawn --config { write: { checkout_pointers: false } }

let path = artifact {
	tangram.ts: r#'
		export default function () {
			let baz = tg.file("hello, world!");
			let graph = tg.graph({
				nodes: [
					{ kind: "directory", entries: { "foo": 1 } },
					{ kind: "file", dependencies: {"../bar": 2, baz } },
					{ kind: "directory", entries: { "tangram.ts": 3 } },
					{ kind: "file", dependencies: { "../foo": 0 } },
				]
			});
			return tg.directory({
				foo: tg.directory({
					graph,
					index: 0,
					kind: "directory",
				}),
				bar: tg.directory({
					graph,
					index: 2,
					kind: "directory",
				})
			})
		}
	'#
}
let id = tg build --no-checkout-pointers $path
rm --recursive --force $local.checkout_directory
mkdir $local.checkout_directory

tg checkout $id | complete

snapshot --path $local.checkout_directory
