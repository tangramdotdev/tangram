use ../../../util/test.nu *

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
	dir_01rq4c2tee1e5e5netpb4k3bn0rgm86aamzw4frywbzetb2h606bb0
	fil_016a4v9jpdbdb83jye3zv4js53wsv3z21gjyvarka9marg2znhwh8g
	fil_01aht3sdwcavvmj7jr9wsrxxzbj93c3wvn21zy9kzm8b8r87wjp25g
] $local.checkout_directory
