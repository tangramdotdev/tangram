use ../../test.nu *

# Getting a relative path reports the referent's path relative to the working directory, while an absolute path reports it absolute.

let server = spawn

let pkg = artifact {
	tangram.ts: 'export default () => tg.file("rel");',
}
let parent = $pkg | path dirname
let name = $pkg | path basename

let output = with-env { TANGRAM_QUIET: "false" } { do --env { cd $parent; tg get $"./($name)" | complete } }
success $output
assert ($output.stderr | str contains $"?path=($name)") "the referent path should be relative to the working directory"

let output = with-env { TANGRAM_QUIET: "false" } { tg get $pkg | complete }
success $output
assert ($output.stderr | str contains $"?path=($pkg)") "the referent path should be absolute"
