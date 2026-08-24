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
assert equal ($output.stderr | lines | length) 1 "get should print one referent info message"
assert ($output.stderr | str contains "info dir_") "the referent should contain the directory id"
assert ($output.stderr | str contains "location=local") "the referent should contain the location"
assert ($output.stderr | str contains "path=artifact") "the referent should contain the relative path"
assert ($output.stderr | str contains "tokens[local]") "the referent should contain the tokens"

let output = with-env { TANGRAM_QUIET: "false" } { tg get $pkg | complete }
success $output
assert equal ($output.stderr | lines | length) 1 "get should print one referent info message"
assert ($output.stderr | str contains "info dir_") "the referent should contain the directory id"
assert ($output.stderr | str contains "location=local") "the referent should contain the location"
assert ($output.stderr | str contains $"path=($pkg)") "the referent should contain the absolute path"
assert ($output.stderr | str contains "tokens[local]") "the referent should contain the tokens"
