use ../../../test.nu *

# A missing process output remains distinct from a process that outputs null.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			let missing = tg.Process.Wait.fromData({ exit: 0 });
			let null_ = tg.Process.Wait.fromData({ exit: 0, output: null });
			return [missing.output === undefined, null_.output === null];
		}
	'
}

let output = tg build $path
snapshot $output '[true,true]'
