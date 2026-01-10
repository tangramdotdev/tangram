use ../../test.nu *

let server = spawn

# Create and tag dependencies.
let foo_path = artifact {
	tangram.ts: '// tree/of/tags/foo'
}
tg tag tree/of/tags/foo $foo_path

let bar_path = artifact {
	tangram.ts: 'import * as foo from "tree/of/tags/foo"'
}
tg tag tree/of/tags/bar $bar_path

# Run tree command with tag kind.
let output = tg tree tree --kind=tag
snapshot $output '
	tree
	└╴tree/of
	  └╴tree/of/tags
	    ├╴tree/of/tags/bar: dir_011md771n3wpv4y4pzsanjj2qfypxtehxejf74kf2n7e400rn0h2kg
	    │ └╴entries: map
	    │   └╴tangram.ts: fil_014gcdqfw24bscb4553p10x69qkev5sqzvy5a2px7zthnbs15qfqng
	    │     ├╴contents: blb_0171ytyz9bccy0k15hrep1as9ccybpy2ny0bjq2cmshydz9265zbz0
	    │     └╴dependencies: map
	    │       └╴tree/of/tags/foo: map
	    │         ├╴id: "dir_014mfsjgqfsvtm7jjbryt0nsjbqtx5wy26dd9j49pg7sx76612xkvg"
	    │         ├╴item: dir_014mfsjgqfsvtm7jjbryt0nsjbqtx5wy26dd9j49pg7sx76612xkvg
	    │         └╴tag: "tree/of/tags/foo"
	    └╴tree/of/tags/foo: dir_014mfsjgqfsvtm7jjbryt0nsjbqtx5wy26dd9j49pg7sx76612xkvg
	      └╴entries: map
	        └╴tangram.ts: fil_01casadjaen6c5zjwghqhctht695vw52x41x5tfnj2dkv8b245ygbg
	          └╴contents: blb_0194cvce5k7jhd4ywjr3b9k3ax0gqv8af172q1mk1y287bwz6gaarg
'
