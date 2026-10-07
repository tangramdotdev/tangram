use ../../lib/test.nu *

# Checking out a directory containing a file that participates in a dependency cycle writes the directory into the checkouts directory.

let local = server spawn --config { write: { checkout_pointers: false } }

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

# Check out.
tg checkout $id

# Snapshot.
snapshot --path --entries [
	dir_01z2s0c1g21hp91nbn555bejwfq65g1yvv1pgxpm53v8v46c2gpf3g
	fil_015dcp0awy3fqpf5f48ghbfreg00mkp13qjz3w4md2yv2jnzvpkb9g
	fil_01pv9xz3njqn4dzv0w4611nnscyxk14q5cjbavfb2kq5xffzpxxww0
] $local.checkout_directory
