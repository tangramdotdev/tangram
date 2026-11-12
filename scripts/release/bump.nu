#!/usr/bin/env nu

use ./common.nu *

# The files that declare the version, with a pattern that matches each declaration and captures the text around the version.
const declarations = [
	{ path: 'Cargo.toml', pattern: '(?m)^(version = ")[^"]*(")$' },
	{ path: 'packages/clients/javascript/package.json', pattern: '(?m)^(\t"version": ")[^"]*(")' },
	{ path: 'packages/clients/javascript/src/config.ts', pattern: '(?m)^(export const version = ")[^"]*(";)$' },
	{ path: 'packages/clients/python/pyproject.toml', pattern: '(?m)^(version = ")[^"]*(")$' },
	{ path: 'packages/clients/python/src/tangram/config.py', pattern: '(?m)^(version = ")[^"]*(")$' },
	{ path: 'packages/macos/project.yml', pattern: '(?m)^(\s*MARKETING_VERSION: )\S+()$' },
	{ path: 'packages/vscode/package.json', pattern: '(?m)^(\t"version": ")[^"]*(")' },
	{ path: 'packages/zed/extension.toml', pattern: '(?m)^(version = ")[^"]*(")$' },
]

# Set the version everywhere it is declared.
def main [
	version: string # The new version, such as 0.0.1.
] {
	if ($version | parse --regex '^\d+\.\d+\.\d+$' | is-empty) {
		error make { msg: $'the version must have the form <major>.<minor>.<patch>: ($version)' }
	}
	cd (repository)

	# Replace each declaration, requiring that each file declares the version exactly once.
	for declaration in $declarations {
		let contents = open --raw $declaration.path
		let count = ($contents | parse --regex $declaration.pattern | length)
		if $count != 1 {
			error make { msg: $'expected one version declaration in ($declaration.path), but found ($count)' }
		}
		$contents
		| str replace --regex $declaration.pattern $'${1}($version)${2}'
		| save --force --raw $declaration.path
		print -e $'set ($declaration.path)'
	}

	# Update the workspace packages in the lockfiles.
	^cargo update --workspace --offline
	^bun install --lockfile-only
	^uv lock

	print -e $'the version is now ($version)'
}
