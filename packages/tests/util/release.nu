use ./test.nu *

const repository = path self '../../../'

# Create a small release workspace with the repository's release scripts.
export def fixture [] {
	let path = artifact {
		'.gitignore': "/release\n/target\n"
		'Cargo.toml': '
			[workspace]
			members = ["cli", "provider"]
			resolver = "2"
			[workspace.package]
			version = "0.0.0"
		'
		'build.tg.ts': '
			export const release = (arg: { commit: string; host: string }) => tg.command({
				args: ["build", "--release", "--target", arg.host, "-p", "tangram_cli", ...(arg.host.endsWith("-darwin") ? ["-p", "tangram_vfs_provider"] : [])],
				env: { TANGRAM_CLI_COMMIT_HASH: arg.commit },
				executable: { path: "cargo" },
			});
			export const releaseBinary = (file: tg.File) => {
				tg.assert(file instanceof tg.File);
				return file;
			};
		'
		cli: {
			'Cargo.toml': '
				[package]
				name = "tangram_cli"
				version.workspace = true
				edition = "2021"
				[[bin]]
				name = "tangram"
				path = "main.rs"
			'
			'main.rs': 'fn main() { println!("{}", env!("TANGRAM_CLI_COMMIT_HASH")); }'
		}
		provider: {
			'Cargo.toml': '
				[package]
				name = "tangram_vfs_provider"
				version.workspace = true
				edition = "2021"
				[lib]
				path = "lib.rs"
				crate-type = ["staticlib"]
			'
			'lib.rs': '#[no_mangle] pub extern "C" fn provider() {}'
		}
		packages: {
			release: {
				'release.nu': (open --raw ($repository | path join packages/release/release.nu))
				'stage.nu': (open --raw ($repository | path join packages/release/stage.nu))
			}
		}
		scripts: {
			'version.py': (open --raw ($repository | path join scripts/version.py))
		}
		'tangram.ts': '
			export const release = async (...args: Array<string>) => {
				await tg.run`nu packages/release/release.nu "$@"`
					.executable("sh")
					.args(["--", ...args])
					.cwd(tg.process.cwd);
			};
		'
	}
	^cargo generate-lockfile --manifest-path ($path | path join Cargo.toml) --offline
	^git -C $path init -q
	^git -C $path add .
	^git -C $path -c user.name=test -c user.email=test@example.com -c commit.gpgsign=false commit -qm fixture
	$path
}
