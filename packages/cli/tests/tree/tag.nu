use ../../test.nu *

# The tree command renders groups, tags, and tag nodes as distinct graph nodes.

let server = spawn

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
	    │ └╴target: dir_01h1ega9xm0wfgk03jekq44c09qhc17h65we83wf047sxn7j946a8g
	    │   └╴entries: map
	    │     └╴tangram.ts: fil_01eppjjnw38e28041xyxzntn41k158ctazd31j9at3f0p0spqjta7g
	    │       ├╴contents: blb_0171ytyz9bccy0k15hrep1as9ccybpy2ny0bjq2cmshydz9265zbz0
	    │       └╴dependencies: map
	    │         └╴tree/of/tags/foo: map
	    │           ├╴id: "dir_017bc8mjf34bgdahf9aasyqra5f4ef35ekjnm19fejm94av8m25tbg"
	    │           ├╴node: dir_017bc8mjf34bgdahf9aasyqra5f4ef35ekjnm19fejm94av8m25tbg
	    │           └╴tag: "tree/of/tags/foo"
	    └╴tree/of/tags/foo
	      └╴target: dir_017bc8mjf34bgdahf9aasyqra5f4ef35ekjnm19fejm94av8m25tbg
	        └╴entries: map
	          └╴tangram.ts: fil_01casadjaen6c5zjwghqhctht695vw52x41x5tfnj2dkv8b245ygbg
	            └╴contents: blb_0194cvce5k7jhd4ywjr3b9k3ax0gqv8af172q1mk1y287bwz6gaarg
'
