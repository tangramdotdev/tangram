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
	    ├╴tree/of/tags/bar: dir_01fpb7jsgxkj5cf755q2ya402cy6eg0ydvawhep133v0pdzf3sebj0
	    │ └╴entries: map
	    │   └╴tangram.ts: fil_01rnnxz52enh0fdjb7vf17m8krfry1arc280cebr617zpnaapyg9r0
	    │     ├╴contents: blb_0171ytyz9bccy0k15hrep1as9ccybpy2ny0bjq2cmshydz9265zbz0
	    │     └╴dependencies: map
	    │       └╴tree/of/tags/foo: map
	    │         ├╴id: "dir_01jdb1dnyvm0g4ks4y5cj3shqxq0wvscqxg4s84nqkjrm0vqbye7ng"
	    │         ├╴item: dir_01jdb1dnyvm0g4ks4y5cj3shqxq0wvscqxg4s84nqkjrm0vqbye7ng
	    │         └╴tag: "tree/of/tags/foo"
	    └╴tree/of/tags/foo: dir_01jdb1dnyvm0g4ks4y5cj3shqxq0wvscqxg4s84nqkjrm0vqbye7ng
	      └╴entries: map
	        └╴tangram.ts: fil_01casadjaen6c5zjwghqhctht695vw52x41x5tfnj2dkv8b245ygbg
	          └╴contents: blb_0194cvce5k7jhd4ywjr3b9k3ax0gqv8af172q1mk1y287bwz6gaarg
'
