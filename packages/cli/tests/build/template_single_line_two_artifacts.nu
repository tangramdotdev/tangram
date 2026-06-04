use ../../test.nu *

# A single-line tg template literal interpolates two distinct artifact placeholders separated by a space and matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		import foo from "./foo.txt";
		import bar from "./bar.txt";
		export default () => tg`${foo} ${bar}`;
	'
	foo.txt: 'foo'
	bar.txt: 'bar'
}

let output = tg build $path
snapshot $output
