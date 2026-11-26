use ../../util/test.nu *
use ../../util/release.nu

# The package stage without signing builds each Linux target's packages from the compiled artifact, and generates the unsigned repository metadata over exactly this release's packages, replacing the metadata and the signatures of an earlier packaging.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
let local = server spawn --name local
let path = release fixture
cd $path
let commit = ^git rev-parse HEAD | str trim

# Leave the metadata, a signature, and a package of an earlier packaging.
mkdir release/apt release/rpm/repodata
'stale' | save release/apt/InRelease
'stale' | save release/rpm/repodata/stale-primary.xml.zst
'stale' | save release/apt/tangram_0.0.1_amd64.deb

let output = tg run .#release -- --stage compile --stage package --no-sign --target x86_64-linux-gnu --target x86_64-linux-musl | complete
success $output

# Each package holds the tarball of the compiled executable.
let compiled = $"tarball of #!/bin/sh\necho ($commit)\n"
assert equal (open --raw release/tangram_x86_64-linux-gnu.tar.gz) $compiled
assert equal (open --raw release/apt/tangram_0.0.0_amd64.deb) $'x86_64-linux-gnu 0.0.0 from ($compiled)'
assert equal (open --raw release/rpm/tangram-0.0.0-1.x86_64.rpm) $'x86_64-linux-gnu 0.0.0 from ($compiled)'
assert equal (open --raw release/apk/x86_64/tangramdotdev-0.0.0-r0.apk) $'x86_64-linux-musl 0.0.0 from ($compiled)'

# The AUR recipe has the checksum of the GNU tarball.
let recipe = open --raw release/aur/PKGBUILD | from json
assert equal $recipe.archives.x86_64 (open --raw release/tangram_x86_64-linux-gnu.tar.gz | hash sha256)
assert equal (open --raw release/aur/.SRCINFO) 'srcinfo'

# The metadata covers only this release's packages, and the earlier metadata and signature are gone.
assert equal (open --raw release/apt/Packages) $'tangram_0.0.0_amd64.deb: x86_64-linux-gnu 0.0.0 from ($compiled)'
assert equal (open --raw release/rpm/repodata/repomd.xml) $'tangram-0.0.0-1.x86_64.rpm: x86_64-linux-gnu 0.0.0 from ($compiled)'
assert equal (open --raw release/apk/x86_64/APKINDEX.tar.gz) $'tangramdotdev-0.0.0-r0.apk: x86_64-linux-musl 0.0.0 from ($compiled)'
assert not ('release/apt/InRelease' | path exists)
assert equal (ls release/rpm/repodata | get name | path basename) ['repomd.xml']
