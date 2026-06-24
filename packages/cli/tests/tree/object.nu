use ../../test.nu *

# The tree command renders the expected object tree for a tagged single-file package.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return 42; }'
}

# Run tree command.
tg tag root $path
let output = tg tree root
snapshot $output '
	root: dir_01t88ctpttxnfrmwfs3avyx4ps6bqjz8qbf2ax9zjnvwka27c52z30
	└╴entries: map
	  └╴tangram.ts: fil_01bmpbckej87pxfjz87zeaht4sjyx2jw4jh3yvdqnr57bzygvt791g
	    └╴contents: blb_01kmkgpm0e193fjq5zxr1359x94gejpmx8d3q3ytge2v6tf49g1tw0
'
