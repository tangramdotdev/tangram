import AppKit

@MainActor
enum CommandLineTool {
	private enum Scope {
		case system
		case user

		var directory: URL {
			URL(filePath: (displayPath as NSString).expandingTildeInPath, directoryHint: .isDirectory)
		}

		var displayPath: String {
			switch self {
			case .system:
				"/usr/local/bin"
			case .user:
				"~/.tangram/bin"
			}
		}
	}

	private static let app = URL(filePath: "/Applications/Tangram.app", directoryHint: .isDirectory)
	private static let executable = app.appending(path: "Contents/Resources/tg")
	private static let names = ["tg", "tangram"]
	private static let promptKey = "TangramCommandLineToolPrompted"

	static var canInstall: Bool {
		Bundle.main.bundleURL.resolvingSymlinksInPath().path() == app.path()
			&& FileManager.default.isExecutableFile(atPath: executable.path())
	}

	static func promptIfNeeded() {
		let defaults = UserDefaults.standard
		guard canInstall, !defaults.bool(forKey: promptKey), !hasExistingCommand else {
			return
		}
		offerInstallation()
	}

	static func offerInstallation() {
		guard canInstall else {
			return
		}
		UserDefaults.standard.set(true, forKey: promptKey)

		let alert = NSAlert()
		alert.messageText = "Install Tangram Command-Line Tools?"
		alert.informativeText = "Use tg and tangram in your shell. Install for your account in \(Scope.user.displayPath), or install for everyone in \(Scope.system.displayPath) with administrator approval."
		alert.addButton(withTitle: "Install for Me")
		alert.addButton(withTitle: "Install for Everyone")
		alert.addButton(withTitle: "Not Now")
		switch alert.runModal() {
		case .alertFirstButtonReturn:
			install(in: .user)
		case .alertSecondButtonReturn:
			install(in: .system)
		default:
			break
		}
	}

	private static var hasExistingCommand: Bool {
		let environment = ProcessInfo.processInfo.environment["PATH"] ?? ""
		let directories = environment.split(separator: ":").map { URL(filePath: String($0)) } + [
			Scope.system.directory,
			Scope.user.directory,
			URL(filePath: "/opt/homebrew/bin"),
		]
		return directories.contains { directory in
			names.contains { name in
				let path = directory.appending(path: name).path()
				var isDirectory: ObjCBool = false
				return FileManager.default.fileExists(atPath: path, isDirectory: &isDirectory)
					&& !isDirectory.boolValue && FileManager.default.isExecutableFile(atPath: path)
			}
		}
	}

	private static func install(in scope: Scope) {
		let files = FileManager.default
		let directory = scope.directory
		var missing: [URL] = []
		for name in names {
			let link = directory.appending(path: name)
			let target = try? files.destinationOfSymbolicLink(atPath: link.path())
			if target == executable.path() {
				continue
			}
			if target != nil || files.fileExists(atPath: link.path()) {
				showResult("A Command Already Exists", "Move \(link.path()) before installing the app’s command-line tools there.")
				return
			}
			missing.append(link)
		}
		guard !missing.isEmpty else {
			showSuccess(in: scope)
			return
		}

		switch scope {
		case .system:
			guard installWithAuthorization() else {
				return
			}
		case .user:
			do {
				try files.createDirectory(at: directory, withIntermediateDirectories: true)
				for link in missing {
					try files.createSymbolicLink(at: link, withDestinationURL: executable)
				}
			} catch {
				showResult("Couldn’t Install Command-Line Tools", error.localizedDescription)
				return
			}
		}
		showSuccess(in: scope)
	}

	private static func installWithAuthorization() -> Bool {
		// Use fixed paths and check both destinations again as root.
		let source = #"""
		do shell script "
		set -eu
		source=/Applications/Tangram.app/Contents/Resources/tg
		[ -x \"$source\" ] && [ ! -L /usr/local ] && [ ! -L /usr/local/bin ] || exit 1
		/bin/mkdir -p /usr/local/bin
		for name in tg tangram; do
			link=/usr/local/bin/$name
			if [ -e \"$link\" ] || [ -L \"$link\" ]; then
				[ -L \"$link\" ] && [ \"$(/usr/bin/readlink \"$link\")\" = \"$source\" ] || exit 1
			fi
		done
		for name in tg tangram; do
			link=/usr/local/bin/$name
			[ -L \"$link\" ] || /bin/ln -sh \"$source\" \"$link\"
		done
		" with administrator privileges
		"""#
		var error: NSDictionary?
		if NSAppleScript(source: source)?.executeAndReturnError(&error) != nil, error == nil {
			return true
		}
		if (error?[NSAppleScript.errorNumber] as? Int) != -128 {
			let message = error?[NSAppleScript.errorMessage] as? String
			showResult("Couldn’t Install Command-Line Tools", message ?? "macOS could not create the links in /usr/local/bin.")
		}
		return false
	}

	private static func showSuccess(in scope: Scope) {
		showResult("Tangram Command-Line Tools Installed", "The tg and tangram commands are linked in \(scope.displayPath). Make sure that directory is in your shell’s PATH.")
	}

	private static func showResult(_ title: String, _ message: String) {
		let alert = NSAlert()
		alert.messageText = title
		alert.informativeText = message
		alert.runModal()
	}
}
