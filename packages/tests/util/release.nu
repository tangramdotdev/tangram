use ./test.nu *

const repository = path self '../../../'
const services = path self release_services.py

# Create a small release workspace with the repository's release orchestration and publishing, whose sandboxed compile makes an executable that prints the commit, and whose packaging builds, smoke tests, image, and signing stand in for the real ones with files that name their inputs.
export def fixture [] {
	let path = artifact {
		'.gitignore': "/release\n__pycache__\n"
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
						tg.directory({
							PKGBUILD: tg.file(JSON.stringify(arg)),
							".SRCINFO": tg.file("srcinfo"),
							LICENSE: tg.file("license"),
							"config.json": tg.file("config"),
							"tangram.service": tg.file("service"),
							"tangram.sysusers": tg.file("sysusers"),
						});
					const listing = async (directory: tg.Directory) => {
						const lines = [];
						for await (const [name, artifact] of directory) {
							lines.push(artifact instanceof tg.File ? `${name}: ${await artifact.text}` : `${name}/`);
						}
						return lines.join("\n");
					};
					const list = async (directory: tg.Directory) => tg.file(await listing(directory));
					export const test = async (arg: { apk?: tg.Directory; apt?: tg.Directory; image?: tg.File; rpm?: tg.Directory; tarball: tg.File; target: string }) =>
						tg.file(`tested ${arg.target} from ${await arg.tarball.text}`);
					export const repository = async (arg: { apk?: tg.Directory; apt?: tg.Directory; rpm?: tg.Directory }) => {
						const output: Record<string, tg.Unresolved<tg.Artifact>> = {};
						if (arg.apt !== undefined) {
							output.apt = tg.directory({ Packages: list(arg.apt), "Packages.gz": list(arg.apt), Release: tg.file("release") });
						}
						if (arg.rpm !== undefined) {
							const primary = await listing(arg.rpm);
							const name = `${(await tg.checksum(primary, "sha256")).slice(7)}-primary.xml`;
							const repomd = `<repomd><data type="primary"><location href="repodata/${name}"/></data></repomd>`;
							output.rpm = tg.directory({ repodata: { "repomd.xml": tg.file(repomd), [name]: tg.file(primary) } });
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
						let architecture = $platform | split row "/" | last
						mkdir release/image
						$"ghcr.io/tangramdotdev/tangram:($version)-($architecture)" | save -f $"release/image/tangram_($version)_($architecture).tar"
					}
				'
				'publish.py': (open --raw ($repository | path join packages/release/publish.py))
				'release.nu': (open --raw ($repository | path join packages/release/release.nu))
				'sign.py': '
					import sys
					from pathlib import Path

					KEY_BUCKET = "keys-tangram-dev"


					def download_key(*args):
						raise RuntimeError("the stand-in signing has no keys")


					passphrase = signing_directory = download_key


					def sign(path):
						path.write_text(path.read_text() + " signed")


					def main(action, version, targets):
						release = Path("release")
						gnu = [arch for arch in ("aarch64", "x86_64") if f"{arch}-linux-gnu" in targets]
						musl = [arch for arch in ("aarch64", "x86_64") if f"{arch}-linux-musl" in targets]
						if action == "packages":
							for arch in gnu:
								sign(release / "rpm" / f"tangram-{version}-1.{arch}.rpm")
								(release / "rpm" / "RPM-GPG-KEY-tangram").write_text("rpm key")
							for arch in musl:
								sign(release / "apk" / arch / f"tangramdotdev-{version}-r0.apk")
								(release / "apk" / "tangram-apk.rsa.pub").write_text("apk key")
						elif action == "repositories":
							if gnu:
								apt = release / "apt"
								(apt / "InRelease").write_text("signed " + (apt / "Release").read_text())
								(apt / "tangram-apt.asc").write_text("apt key")
								repomd = release / "rpm" / "repodata" / "repomd.xml"
								(repomd.parent / "repomd.xml.asc").write_text("signature of " + repomd.read_text())
							for arch in musl:
								sign(release / "apk" / arch / "APKINDEX.tar.gz")


					if __name__ == "__main__":
						main(sys.argv[1], sys.argv[2], sys.argv[3:])
				'
				'sparkle.py': (open --raw ($repository | path join packages/release/sparkle.py))
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

# Stand in for the package bucket, served over HTTP, and for the GitHub releases, the container registry, and the AUR repository, which publishing a release writes to, and get the directory of their state.
export def --env publisher [] {
	# Put the cf, gh, and docker stand-ins on the path.
	let state = mktemp -d
	let bin = $state | path join bin
	mkdir $bin
	for command in [cf docker gh] {
		let path = $bin | path join $command
		$"#!/bin/sh\nexec python3 '($services)' '($state)' ($command) \"$@\"\n" | save $path
		^chmod 755 $path
	}
	$env.PATH = $env.PATH | prepend $bin

	# Serve the bucket, which the supervisor stops on cleanup.
	let supervisor = mktemp -d
	track_supervisor $supervisor
	^tangram_tests_supervisor start --directory $supervisor --name bucket --ready-fd 3 -- python3 $services $state serve o> /dev/null
	$env.TANGRAM_PACKAGE_HOST = $'http://127.0.0.1:(open --raw ($state | path join port) | str trim)'

	# Clone and push the AUR package with a local repository, and commit to it without signing.
	let aur = $state | path join aur.git
	^git init -q --bare --initial-branch=master $aur
	let entries = [
		[key value];
		[$'url.file://($aur).insteadOf' 'ssh://aur@aur.archlinux.org/tangramdotdev-bin.git']
		['user.name' 'test']
		['user.email' 'test@example.com']
		['commit.gpgsign' 'false']
	]
	let count = $env.GIT_CONFIG_COUNT? | default '0' | into int
	for entry in ($entries | enumerate) {
		load-env {
			$'GIT_CONFIG_KEY_($count + $entry.index)': $entry.item.key
			$'GIT_CONFIG_VALUE_($count + $entry.index)': $entry.item.value
		}
	}
	$env.GIT_CONFIG_COUNT = $'($count + ($entries | length))'
	$state
}

# Get what the release has published to the stand-ins: the checksum of every file in the bucket, the GitHub releases, the container registry, and the commits of the AUR repository.
export def published [state: path] {
	let bucket = $state | path join bucket
	let files = glob $'($bucket)/**/*' --no-dir | sort | each {|file| {($file | path relative-to $bucket): (open --raw $file | hash sha256)} } | reduce -f {} {|entry, files| $files | merge $entry }
	let aur = ^git -C ($state | path join aur.git) log --format=%H master | lines
	{aur: $aur, bucket: $files, github: (open ($state | path join github.json)), registry: (open ($state | path join registry.json))}
}
