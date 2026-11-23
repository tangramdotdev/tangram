import AppKit

let iconPath = CommandLine.arguments[1]
let filePath = CommandLine.arguments[2]
guard let icon = NSImage(contentsOfFile: iconPath),
      NSWorkspace.shared.setIcon(icon, forFile: filePath)
else {
	FileHandle.standardError.write(Data("could not set the DMG instruction icon\n".utf8))
	exit(EXIT_FAILURE)
}
