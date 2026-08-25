use ../../test.nu *

# The tree command renders groups, tags, and tag nodes as distinct graph nodes.

let server = server spawn

# Create and tag dependencies.
let foo_path = artifact {
	tangram.ts: '// tree/of/tags/foo'
}
tg tag -p tree/of/tags/foo $foo_path

let bar_path = artifact {
	tangram.ts: 'import * as foo from "tree/of/tags/foo"'
}
tg tag -p tree/of/tags/bar $bar_path

# Run the tree command.
let output = tg tree tree
snapshot $output '
	tree
	└╴tree/of
	  └╴tree/of/tags
	    ├╴tree/of/tags/bar
	    │ └╴target: dir_01a2v2xx6h1atd6bxncf5h08racw5rvdxr2r0dt855j9r0hc5k1290
	    │   └╴entries: map
	    │     └╴tangram.ts: fil_01z47zpv1n98qnjqv439p8s9kh6cbta4gz8p0d0jv2p5fj762kq25g
	    │       ├╴contents: blb_0171ytyz9bccy0k15hrep1as9ccybpy2ny0bjq2cmshydz9265zbz0
	    │       └╴dependencies: map
	    │         └╴tree/of/tags/foo: map
	    │           ├╴id: "dir_01bjvydgbza6t9bfv9pre2th2kvesar72b8ygds6h0rqpvxgndjfw0"
	    │           ├╴node: dir_01bjvydgbza6t9bfv9pre2th2kvesar72b8ygds6h0rqpvxgndjfw0
	    │           └╴tag: "tree/of/tags/foo"
	    └╴tree/of/tags/foo
	      └╴target: dir_01bjvydgbza6t9bfv9pre2th2kvesar72b8ygds6h0rqpvxgndjfw0
	        └╴entries: map
	          └╴tangram.ts: fil_01v6086g1en270899j96jw2t3brkdnwapkxasewzwhx3n0mnrkzsa0
	            └╴contents: blb_0194cvce5k7jhd4ywjr3b9k3ax0gqv8af172q1mk1y287bwz6gaarg
'
