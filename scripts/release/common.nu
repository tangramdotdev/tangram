const repository_path = path self '../../'

# The release targets, keyed by Rust target triple, with the host name used in the archive names.
export const targets = {
	'aarch64-apple-darwin': { arch: 'aarch64', host: 'aarch64-darwin', os: 'darwin' },
	'aarch64-unknown-linux-gnu': { arch: 'aarch64', host: 'aarch64-linux', os: 'linux' },
	'x86_64-apple-darwin': { arch: 'x86_64', host: 'x86_64-darwin', os: 'darwin' },
	'x86_64-unknown-linux-gnu': { arch: 'x86_64', host: 'x86_64-linux', os: 'linux' },
}

# The newest glibc that the Linux binaries may require, which is releaseGlibcVersion in build.tg.ts.
export const glibc_floor = [2 35]

# Get the path to the repository.
export def repository []: nothing -> path {
	$repository_path | path expand
}

# Get the workspace version.
export def version []: nothing -> string {
	open (repository | path join Cargo.toml) | get workspace.package.version
}

# Get the commit, or a hash of the edited contents of the checkout.
export def commit []: nothing -> string {
	^python3 (repository | path join scripts/version.py) | str trim
}

# Get the macOS deployment target from the Cargo configuration.
export def deployment_target []: nothing -> string {
	open (repository | path join .cargo/config.toml) | get env.MACOSX_DEPLOYMENT_TARGET
}

# Get the Rust target triple of this machine.
export def host_target []: nothing -> string {
	^rustc -vV | lines | parse 'host: {host}' | get host | first
}

# Get the operating system of this machine as it appears in the release targets.
export def host_os []: nothing -> string {
	match $nu.os-info.name {
		'macos' => 'darwin',
		'linux' => 'linux',
		$name => { error make { msg: $'unsupported operating system: ($name)' } },
	}
}

# Get the architecture of this machine as it appears in the release targets.
export def host_arch []: nothing -> string {
	match $nu.os-info.arch {
		'aarch64' | 'arm64' => 'aarch64',
		'x86_64' => 'x86_64',
		$arch => { error make { msg: $'unsupported architecture: ($arch)' } },
	}
}

# Print a step heading.
export def step [message: string] {
	print -e $"\n==> ($message)"
}
