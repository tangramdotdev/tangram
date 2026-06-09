use ../../test.nu *

# Outdated fails when the package has no lockfile.

let server = spawn

let path = artifact { tangram.ts: 'export default () => 42;' }

let output = do --env { cd $path; tg outdated . } | complete
failure $output
assert ($output.stderr | str contains "missing lockfile") "the error should mention the missing lockfile"
