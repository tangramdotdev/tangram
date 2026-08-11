use ../../test.nu *

# The tree command renders a tag and its object as distinct graph nodes.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return 42; }'
}

# Run tree command.
tg tag root $path
let output = tg tree root
snapshot $output '
	root
	└╴target: dir_01a4svy1thqxp7dcmfc7jjvpkkkqz4tk3sm090e4xh7x7brmvnc7m0
	  └╴entries: map
	    └╴tangram.ts: fil_01m7bw9grpp30bmdtac61mgg2sp9kd2nqz6qpn6a8fhs594h9n7bbg
	      └╴contents: blb_01kmkgpm0e193fjq5zxr1359x94gejpmx8d3q3ytge2v6tf49g1tw0
'
