use ../../../lib/test.nu *

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
	fil_016a4v9jpdbdb83jye3zv4js53wsv3z21gjyvarka9marg2znhwh8g
	fil_01aht3sdwcavvmj7jr9wsrxxzbj93c3wvn21zy9kzm8b8r87wjp25g
] $local.checkout_directory
