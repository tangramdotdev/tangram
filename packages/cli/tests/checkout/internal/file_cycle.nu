use ../../lib/test.nu *

# Checking out two files that depend on each other, forming a cycle, writes the files into the checkouts directory.

let local = server spawn --config { write: { checkout_pointers: false } }

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

# Check out.
let output = tg checkout $id

# Snapshot.
snapshot --path --entries [
	fil_015dcp0awy3fqpf5f48ghbfreg00mkp13qjz3w4md2yv2jnzvpkb9g
	fil_01pv9xz3njqn4dzv0w4611nnscyxk14q5cjbavfb2kq5xffzpxxww0
] $local.checkout_directory
