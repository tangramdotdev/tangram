use ../../test.nu *

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

let output = run tg build $path
snapshot $output
