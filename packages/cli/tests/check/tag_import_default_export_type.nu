use ../../test.nu *

let server = spawn

let source = 'export default (): Promise<tg.Directory> => tg.directory();'

# Directory tag.
tg tag test-dir (artifact { tangram.ts: $source })

# File tag.
let temp = mktemp -d
$source | save ($temp | path join "dep.tg.ts")
tg tag test-file ($temp | path join "dep.tg.ts")

let test_directory = artifact { tangram.ts: 'import d from "test-dir"; const x: tg.Directory = await tg.build(d);' }
tg check $test_directory

let test_file = artifact { tangram.ts: 'import f from "test-file" with { type: "ts" }; const x: tg.Directory = await tg.build(f);' }
tg check $test_file
