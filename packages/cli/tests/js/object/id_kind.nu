use ../../../test.nu *

# tg.Object.Id.kind maps each object id prefix to its kind.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () =>
			["blb_0", "dir_0", "fil_0", "sym_0", "gph_0", "cmd_0", "err_0"].map(
				tg.Object.Id.kind,
			);
	'
}

let output = tg build $path
snapshot $output '["blob","directory","file","symlink","graph","command","error"]'
