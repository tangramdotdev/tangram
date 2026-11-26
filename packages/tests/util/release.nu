use ./test.nu *

const repository = path self '../../../'

# Create a small release workspace with the repository's release orchestration, whose sandboxed compile makes an executable that prints the commit, and whose packaging builds and image stand in for the real ones with files that name their inputs.
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
				'build.tg.ts': '
					const text = async (release: tg.Directory) => (await release.get("tangram").then(tg.File.expect)).text;
					export const tarball = async (arg: { release: tg.Directory }) => tg.file(`tarball of ${await text(arg.release)}`);
					const package_ = async (name: string, arg: { release: tg.Directory; target: string; version: string }) =>
						tg.directory({ [name]: tg.file(`${arg.target} ${arg.version} from ${await (await tarball(arg)).text}`) });
					export const deb = (arg: { release: tg.Directory; target: string; version: string }) =>
						package_(`tangram_${arg.version}_${arg.target.startsWith("aarch64") ? "arm64" : "amd64"}.deb`, arg);
					export const rpm = (arg: { release: tg.Directory; target: string; version: string }) =>
						package_(`tangram-${arg.version}-1.${arg.target.split("-")[0]}.rpm`, arg);
					export const apk = (arg: { release: tg.Directory; target: string; version: string }) =>
						package_(`tangramdotdev-${arg.version}-r0.apk`, arg);
					export const aur = (arg: { archives: Record<string, string>; version: string }) =>
						tg.directory({ PKGBUILD: tg.file(JSON.stringify(arg)), ".SRCINFO": tg.file("srcinfo") });
					const list = async (directory: tg.Directory) => {
						const lines = [];
						for await (const [name, artifact] of directory) {
							lines.push(artifact instanceof tg.File ? `${name}: ${await artifact.text}` : `${name}/`);
						}
						return tg.file(lines.join("\n"));
					};
					export const repository = async (arg: { apk?: tg.Directory; apt?: tg.Directory; rpm?: tg.Directory }) => {
						const output: Record<string, tg.Unresolved<tg.Artifact>> = {};
						if (arg.apt !== undefined) {
							output.apt = tg.directory({ Packages: list(arg.apt), "Packages.gz": tg.file(""), Release: tg.file("") });
						}
						if (arg.rpm !== undefined) {
							output.rpm = tg.directory({ repodata: { "repomd.xml": list(arg.rpm) } });
						}
						if (arg.apk !== undefined) {
							const apk: Record<string, tg.Unresolved<tg.Artifact>> = {};
							for await (const [architecture, directory] of arg.apk) {
								apk[architecture] = tg.directory({ "APKINDEX.tar.gz": list(tg.Directory.expect(directory)) });
							}
							output.apk = tg.directory(apk);
						}
						return tg.directory(output);
					};
				'
				'image.nu': '
					def main [] {}
					def "main package" [--platform: string, --version: string] {
						mkdir release/image
						$"image ($platform)" | save -f $"release/image/tangram_($version).tar"
					}
				'
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
