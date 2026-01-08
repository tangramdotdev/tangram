use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => 42;'
}

# Run tree command.
tg tag root $path
let output = tg tree root
snapshot $output '
	root: dir_01fpzn2q7gxx3ct70vbyb2ydcgfznkvac49xp78r7cfxeq69sb4jkg
	└╴entries: map
	  └╴tangram.ts: fil_01sa3pyv7baf50x2ymmvy7p41zqnmmv8gp1fq5z3mq60ps8vcfxa30
	    └╴contents: blb_01mdez7rn5622ncqxr3the1thtqwp9tv919f5xyaj021mbp0egfa40
'
