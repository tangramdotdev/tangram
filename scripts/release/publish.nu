#!/usr/bin/env nu

use ./common.nu *

const repository_url = 'https://github.com/tangramdotdev/tangram'

# Write the checksums and the Homebrew formula for the archives in the release directory, then create a draft GitHub release.
def main [
	--dry-run # Write the checksums and the formula, but do not create the release.
	--target: string # The commit to tag. Defaults to the current commit.
] {
	let root = repository
	cd $root
	let version = version
	let tag = $'v($version)'
	let release_path = $root | path join release

	# Require every archive.
	let archives = $targets | values | each { |entry| $release_path | path join $'tangram_($entry.host).tar.gz' }
	let missing = $archives | where { |archive| not ($archive | path exists) }
	if ($missing | is-not-empty) {
		error make { msg: $'the release directory is missing archives: ($missing | path basename | str join ", ")' }
	}

	# Include the macOS app if it is present.
	let app = $release_path | path join Tangram.zip
	let assets = if ($app | path exists) { $archives | append $app } else { $archives }

	# Write the checksums.
	step 'Writing the checksums'
	let digests = $assets | each { |asset| { name: ($asset | path basename), digest: (open --raw $asset | hash sha256) } }
	let checksums_path = $release_path | path join SHASUMS256.txt
	$digests | each { |entry| $'($entry.digest)  ($entry.name)' } | str join "\n" | $in + "\n" | save --force $checksums_path
	print (open --raw $checksums_path)

	# Write the Homebrew formula.
	step 'Writing the Homebrew formula'
	let formula_path = $release_path | path join tangram.rb
	formula $version $digests | save --force $formula_path
	print -e $'wrote ($formula_path)'

	if $dry_run {
		print -e $'skipped creating the release ($tag)'
		return
	}

	# Create the draft release.
	step $'Creating the draft release ($tag)'
	let target = $target | default (^git rev-parse HEAD | str trim)
	let notes = [
		$'Install with `curl -fsSL https://tangram.dev/install.sh | sh`.'
		''
		'The Linux binaries require glibc 2.34 or later.'
	] | str join "\n"
	(
		^gh release create $tag
			--draft
			--generate-notes
			--notes $notes
			--target $target
			--title $version
			...$assets
			$checksums_path
	)
	print -e $'created the draft release ($tag); publish it to make it the latest release'
}

# Render the Homebrew formula.
def formula [version: string, digests: list<record>]: nothing -> string {
	let source = { |host|
		let digest = $digests | where name == $'tangram_($host).tar.gz' | get digest | first
		[
			$'      url "($repository_url)/releases/download/v#{version}/tangram_($host).tar.gz"'
			$'      sha256 "($digest)"'
		]
	}
	[
		'class Tangram < Formula'
		'  desc "Build system and package manager"'
		'  homepage "https://www.tangram.dev"'
		$'  version "($version)"'
		'  license "MIT"'
		''
		'  on_macos do'
		'    on_arm do'
		...(do $source aarch64-darwin)
		'    end'
		'    on_intel do'
		...(do $source x86_64-darwin)
		'    end'
		'  end'
		''
		'  on_linux do'
		'    on_arm do'
		...(do $source aarch64-linux)
		'    end'
		'    on_intel do'
		...(do $source x86_64-linux)
		'    end'
		'  end'
		''
		'  def install'
		'    bin.install "tangram"'
		'    bin.install_symlink "tangram" => "tg"'
		'    bin.install "tgx"'
		'  end'
		''
		'  test do'
		'    assert_match version.to_s, shell_output("#{bin}/tangram --version")'
		'  end'
		'end'
		''
	] | str join "\n"
}
