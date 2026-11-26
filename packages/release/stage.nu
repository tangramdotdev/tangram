# Describe the complete set of compiled files for one release target.
export def files [target: string] {
	let binary = {mode: '755', name: $'tangram_($target)'}
	if ($target ends-with '-darwin') {
		[$binary {mode: '644', name: $'libtangram_vfs_provider_($target).a'}]
	} else {
		[$binary]
	}
}

export def metadata [target: string] {
	$'tangram_($target).json'
}

# Record the source revision and the contents of every compiled file.
export def record [directory: path, target: string, commit: string] {
	for file in (files $target) {
		if ($directory | path join $file.name | path type) != file {
			error make {msg: $'the release target is missing ($file.name)'}
		}
	}
	let hashes = files $target | each {|file|
		{($file.name): (open --raw ($directory | path join $file.name) | hash sha256)}
	} | reduce -f {} {|entry, hashes| $hashes | merge $entry }
	{commit: $commit, files: $hashes, target: $target} | to json | save -f ($directory | path join (metadata $target))
}

# Reject stale source revisions, changed files, and incomplete helper/provider pairs.
export def validate [directory: path, target: string, commit: string] {
	let path = $directory | path join (metadata $target)
	if ($path | path type) != file {
		error make {msg: $'the release metadata is missing for ($target); compile the target again'}
	}
	let metadata = open $path
	if $metadata.target != $target or $metadata.commit != $commit {
		error make {msg: $'the staged release for ($target) does not match the current source revision; compile the target again'}
	}
	for file in (files $target) {
		let path = $directory | path join $file.name
		if ($path | path type) != file {
			error make {msg: $'the release target is missing ($file.name)'}
		}
		let expected = $metadata.files | get -o $file.name
		if $expected == null or (open --raw $path | hash sha256) != $expected {
			error make {msg: $'the release checksum does not match for ($file.name); compile or pull the target again'}
		}
	}
}

# Validate and copy the complete target before replacing any staged file, then install its metadata last.
export def install [directory: path, target: string, commit: string] {
	validate $directory $target $commit
	mkdir release
	let files = (files $target) | append {mode: '644', name: (metadata $target)}
	for file in $files {
		if ('release' | path join $file.name | path type) == dir {
			error make {msg: $'the release destination is a directory ("release" | path join $file.name)'}
		}
	}
	let temporary = ^mktemp -d release/.stage.XXXXXXXX | str trim
	try {
		for file in $files {
			^install -m $file.mode ($directory | path join $file.name) ($temporary | path join $file.name)
		}
		validate $temporary $target $commit
		for file in $files {
			mv -f ($temporary | path join $file.name) ('release' | path join $file.name)
		}
	} catch {|error|
		rm -rf $temporary
		error make $error.raw
	}
	rm -rf $temporary
}
