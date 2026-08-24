use ../../test.nu *

# A sandbox mount target cannot traverse a symlink in the guest root.

if $nu.os-info.name != 'linux' {
	return
}

let lower = mktemp --directory | str trim
let outside = mktemp --directory | str trim
let overlay = mktemp --directory | str trim
let source = mktemp --directory | str trim
mkdir ($overlay | path join upper)
mkdir ($overlay | path join work)
^ln -s $outside ($lower | path join escape)
let output = ^tangram sandbox container run --index 0 --unshare-all --uid 0 --gid 0 --chdir / --overlay-src $lower --overlay ($overlay | path join upper) ($overlay | path join work) / --bind $source /escape/created -- /bin/true | complete
failure $output
assert ($output.stderr | str contains 'mount targets may not traverse symbolic links')
assert (not (($outside | path join created) | path exists))
