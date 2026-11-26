use ../../util/test.nu *
use ../../util/release.nu

# Two machines that publish disjoint targets of a release from the same commit each add their targets, and the second regenerates the indexes over the targets of both.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
let local = server spawn --name local
let state = release publisher
let path = release fixture
let other = mktemp -d | path join other
^git clone -q $path $other

# Publish one target from each checkout.
cd $path
let output = tg run .#release -- --version 1.0.0 --target x86_64-linux-gnu | complete
success $output
cd $other
let output = tg run .#release -- --version 1.0.0 --target aarch64-linux-gnu | complete
success $output

# The apt and rpm repositories cover the signed packages of both targets.
let commit = ^git rev-parse HEAD | str trim
let compiled = $"tarball of #!/bin/sh\necho ($commit)\n"
let bucket = $state | path join bucket/stable
let debs = $"tangram_1.0.0_amd64.deb: x86_64-linux-gnu 1.0.0 from ($compiled)\ntangram_1.0.0_arm64.deb: aarch64-linux-gnu 1.0.0 from ($compiled)"
assert equal (open --raw ($bucket | path join apt/Packages)) $debs
assert equal (open --raw ($bucket | path join apt/InRelease)) 'signed release'
let repomd = open --raw ($bucket | path join rpm/repodata/repomd.xml)
assert equal (open --raw ($bucket | path join rpm/repodata/repomd.xml.asc)) $'signature of ($repomd)'
let primary = $repomd | parse --regex 'href="(?<href>[^"]+)"' | get href | first
let rpms = $"tangram-1.0.0-1.aarch64.rpm: aarch64-linux-gnu 1.0.0 from ($compiled) signed\ntangram-1.0.0-1.x86_64.rpm: x86_64-linux-gnu 1.0.0 from ($compiled) signed"
assert equal (open --raw ($bucket | path join rpm $primary)) $rpms

# The image index and the AUR recipe cover both architectures.
let registry = open ($state | path join registry.json)
let images = ['ghcr.io/tangramdotdev/tangram:1.0.0-amd64' 'ghcr.io/tangramdotdev/tangram:1.0.0-arm64']
assert equal ($registry.indexes | get 'ghcr.io/tangramdotdev/tangram:1.0.0') $images
assert equal ($registry.indexes | get 'ghcr.io/tangramdotdev/tangram:latest') $images
let recipe = ^git -C ($state | path join aur.git) show master:PKGBUILD | from json
assert equal ($recipe.archives | columns | sort) ['aarch64' 'x86_64']
assert equal $recipe.archives.x86_64 (open --raw ($path | path join release/tangram_x86_64-linux-gnu.tar.gz) | hash sha256)

# The GitHub release is published with the tarballs and notes of both targets.
let github = open ($state | path join github.json) | get 'v1.0.0'
assert equal ($github.assets | columns | sort) ['tangram_aarch64-linux-gnu.tar.gz' 'tangram_x86_64-linux-gnu.tar.gz']
assert not $github.isDraft
assert ($github.notes | str contains '- aarch64-linux-gnu')
assert ($github.notes | str contains '- x86_64-linux-gnu')
