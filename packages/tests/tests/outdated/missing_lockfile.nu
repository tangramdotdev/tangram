use ../../util/test.nu *

# Outdated reports no outdated dependencies when the package has no lockfile.

let local = server spawn

let path = artifact { tangram.ts: 'export default function () { return 42; }' }

let output = do --env { cd $path; tg outdated . } | complete
success $output
assert equal $output.stdout ""

let output = do --env { cd $path; tg outdated --json . } | complete
success $output
assert equal ($output.stdout | from json) []
