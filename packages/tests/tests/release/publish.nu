use ../../util/test.nu *
use ../../util/release.nu

# A release publishes by default, so a run without what publishing requires fails before it compiles or packages anything.
let local = server spawn --name local
let path = release fixture
cd $path

let output = tg run .#release | complete
failure $output
assert ($output.stderr | str contains 'publishing requires --version')
assert ($output.stderr | str contains '--stage compile --stage package')
assert not ('release' | path exists)

let output = tg run .#release -- --version 1.0.0 --no-sign | complete
failure $output
assert ($output.stderr | str contains 'publishing requires signed packaging')
assert not ('release' | path exists)
