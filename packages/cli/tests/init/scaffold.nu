use ../../test.nu *

# Initializing the current directory writes the autobuild scaffold to tangram.ts without requiring a server.

let dir = mktemp --directory

let output = do { cd $dir; tg init } | complete
success $output

assert equal (ls $dir | get name | path basename) ["tangram.ts"] "only tangram.ts should be created"
snapshot (open ($dir | path join tangram.ts)) '
	import * as autobuild from "autobuild";
	import * as std from "std";
	import source from "." with { type: "directory" };
	export default function () { return autobuild.build({ env: env(), source }); }
	export function env() { return std.env(autobuild.env({ source })); }

'
