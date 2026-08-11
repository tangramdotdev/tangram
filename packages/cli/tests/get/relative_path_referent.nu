use ../../test.nu *

# Getting a relative reference reports the resolved path relative to the working directory; an absolute reference reports it absolute.

let server = spawn

let pkg = artifact {
	tangram.ts: 'export default () => tg.file("rel");',
}
let parent = $pkg | path dirname
let name = $pkg | path basename

let output = with-env { TANGRAM_QUIET: "false" } { do --env { cd $parent; tg get $"./($name)" | complete } }
success $output
snapshot $output.stderr '
	info dir_01v52btwaj8ca7djfp5wewr1g0pvmfg1b5pb4ezm1b59zkbjj1jxc0?path=artifact

'

let output = with-env { TANGRAM_QUIET: "false" } { tg get $pkg | complete }
success $output
snapshot --normalize --redact $parent $output.stderr '
	info dir_01v52btwaj8ca7djfp5wewr1g0pvmfg1b5pb4ezm1b59zkbjj1jxc0?path=<redacted>/artifact

'
