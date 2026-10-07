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

tg checkout $id | complete

snapshot --path --entries [
	dir_01at8fy016038h8rw7gp51kse6rrpqtq8typjdrsd3d1sptk10e58g
	dir_01gtwffee5npyzyv9v3190s1n700307tkgxgt1cqctnkwnan62webg
	dir_01jhgks6ajjxxa25gy3bfmvtwh4ksh9tby3ctq0cyt7519g2p8nh4g
	fil_0104e8knec3ab503j9d17p8wgn2xv9wwj16r06sprzcg2zcv9atz2g
] $local.checkout_directory
