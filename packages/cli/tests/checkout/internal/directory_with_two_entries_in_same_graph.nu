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
	dir_013yvx9jd5sqqj099rdnqaswsfyw9c8bzzcx1s8p71ct1a3pzcbv0g
	dir_01t55mmrcnzhtk9xtms2gjgghkv2dyk5f1r36wry8yzcxj0175x0j0
	dir_01xm13t32gc48wehgnqx9514fc68kewqy2g1482tp68jeracajq5n0
	fil_0104e8knec3ab503j9d17p8wgn2xv9wwj16r06sprzcg2zcv9atz2g
] $local.checkout_directory
