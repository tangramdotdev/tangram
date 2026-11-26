use ./test.nu *

const repository = path self '../../../'

# Create a small release workspace with the repository's release package, whose sandboxed build makes an executable that prints the commit.
export def fixture [] {
	let path = artifact {
		'.gitignore': "/release\n"
		'Cargo.toml': '
			[workspace.package]
			version = "0.0.0"
		'
		'build.tg.ts': '
			export const release = (arg: { commit: string; target: string }) =>
				tg.directory({
					tangram: tg.file(`#!/bin/sh\necho ${arg.commit}\n`, { executable: true }),
					...(arg.target.endsWith("-darwin") ? { "libtangram_vfs_provider.a": tg.file("provider") } : {}),
				});
		'
		packages: {
			release: {
				'release.nu': (open --raw ($repository | path join packages/release/release.nu))
				'stage.nu': (open --raw ($repository | path join packages/release/stage.nu))
				'tangram.ts': (open --raw ($repository | path join packages/release/tangram.ts))
			}
		}
		'rust-toolchain.toml': (open --raw ($repository | path join rust-toolchain.toml))
		scripts: {
			'context.tg.ts': (open --raw ($repository | path join scripts/context.tg.ts))
			'task.tg.ts': (open --raw ($repository | path join scripts/task.tg.ts))
			'version.py': (open --raw ($repository | path join scripts/version.py))
		}
		'source.txt': 'source'
		'tangram.ts': '
			import release_ from "./packages/release";

			export const release = async (...args: Array<string>) => {
				await release_(...args);
			};
		'
	}
	^git -C $path init -q
	^git -C $path add .
	^git -C $path -c user.name=test -c user.email=test@example.com -c commit.gpgsign=false commit -qm fixture
	$path
}

# Get the release target of this machine.
export def host-target [] {
	let arch = ^uname -m | str trim | str replace 'arm64' 'aarch64'
	if $nu.os-info.name == 'macos' { $'($arch)-darwin' } else { $'($arch)-linux-gnu' }
}
