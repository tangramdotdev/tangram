import AppKit
import Darwin
import OSLog
import ServiceManagement
import SwiftUI

private enum AppError: LocalizedError {
	case failedToStopServer
	case invalidPidFile
	case invalidSocketPath

	var errorDescription: String? {
		switch self {
		case .failedToStopServer:
			"failed to stop the server"
		case .invalidPidFile:
			"invalid pid file"
		case .invalidSocketPath:
			"invalid socket path"
		}
	}
}

private enum VfsBrokerError: LocalizedError {
	case extensionApprovalCancelled
	case invalidRequest
	case invalidToken
	case extensionApprovalTimedOut(String)
	case mountFailed(String)

	var errorDescription: String? {
		switch self {
		case .extensionApprovalCancelled:
			"fskit extension approval was cancelled"
		case .invalidRequest:
			"invalid request"
		case .invalidToken:
			"invalid token"
		case let .extensionApprovalTimedOut(message):
			"timed out waiting for the fskit extension to be enabled: \(message)"
		case let .mountFailed(message):
			"failed to mount the fskit volume: \(message)"
		}
	}
}

private enum Tab: String, CaseIterable, Identifiable {
	case info = "Info"
	case logs = "Logs"

	var id: Self {
		self
	}
}

private enum Toolbar {
	static let identifier = NSToolbar.Identifier("TangramToolbar")
	static let tabs = NSToolbarItem.Identifier("Tabs")
}

private let logger = Logger(subsystem: "dev.tangram.Tangram", category: "App")

private struct VfsBrokerRequest: Decodable {
	let type: String
	let token: String
	let dataDirectory: String
	let mountPath: String

	private enum CodingKeys: String, CodingKey {
		case dataDirectory = "data_directory"
		case mountPath = "mount_path"
		case token
		case type
	}
}

private struct VfsBrokerResponse: Encodable {
	let type: String
	let message: String?

	static func ok() -> Self {
		Self(type: "ok", message: nil)
	}

	static func error(_ message: String) -> Self {
		Self(type: "error", message: message)
	}
}

private final class VfsBrokerAsyncResult: @unchecked Sendable {
	var result: Result<Void, Error> = .success(())
}

private final class VfsBroker: @unchecked Sendable {
	let socketPath: URL
	let token = UUID().uuidString

	private let queue = DispatchQueue(label: "dev.tangram.vfs-broker")
	private var listenSocket: Int32 = -1
	private var isRunning = false
	private var mountedPaths: Set<String> = []

	init(socketDirectory: URL) {
		socketPath = socketDirectory.appending(path: "app.sock")
	}

	func start() throws {
		stop()
		try? FileManager.default.removeItem(at: socketPath)

		let path = socketPath.path()
		guard path.utf8.count < MemoryLayout.size(ofValue: sockaddr_un().sun_path) else {
			throw AppError.invalidSocketPath
		}

		let fd = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
		guard fd >= 0 else {
			throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
		}

		var address = sockaddr_un()
		address.sun_family = sa_family_t(AF_UNIX)
		let pathCapacity = MemoryLayout.size(ofValue: address.sun_path)
		_ = withUnsafeMutablePointer(to: &address.sun_path) { pointer in
			pointer.withMemoryRebound(to: CChar.self, capacity: pathCapacity) { buffer in
				strncpy(buffer, path, pathCapacity - 1)
			}
		}

		let bindResult = withUnsafePointer(to: &address) { pointer in
			pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { pointer in
				Darwin.bind(fd, pointer, socklen_t(MemoryLayout<sockaddr_un>.size))
			}
		}
		guard bindResult == 0 else {
			let error = errno
			Darwin.close(fd)
			throw POSIXError(POSIXErrorCode(rawValue: error) ?? .EIO)
		}
		guard Darwin.listen(fd, 4) == 0 else {
			let error = errno
			Darwin.close(fd)
			throw POSIXError(POSIXErrorCode(rawValue: error) ?? .EIO)
		}

		listenSocket = fd
		isRunning = true
		queue.async { [weak self] in
			self?.acceptLoop()
		}
	}

	func stop() {
		isRunning = false
		if listenSocket >= 0 {
			Darwin.close(listenSocket)
			listenSocket = -1
		}
		for path in mountedPaths {
			try? unmount(URL(filePath: path, directoryHint: .isDirectory))
		}
		mountedPaths.removeAll()
		try? FileManager.default.removeItem(at: socketPath)
	}

	private func acceptLoop() {
		while isRunning {
			let fd = Darwin.accept(listenSocket, nil, nil)
			guard fd >= 0 else {
				if isRunning {
					continue
				}
				return
			}
			handleConnection(fd)
			Darwin.close(fd)
		}
	}

	private func handleConnection(_ fd: Int32) {
		do {
			let data = try readLine(from: fd)
			let request = try JSONDecoder().decode(VfsBrokerRequest.self, from: data)
			try handle(request)
			try write(VfsBrokerResponse.ok(), to: fd)
		} catch {
			try? write(VfsBrokerResponse.error(error.localizedDescription), to: fd)
		}
	}

	private func handle(_ request: VfsBrokerRequest) throws {
		guard request.token == token else {
			throw VfsBrokerError.invalidToken
		}
		guard request.type == "mount_vfs" else {
			throw VfsBrokerError.invalidRequest
		}
		try FileManager.default.createDirectory(
			at: URL(filePath: request.mountPath, directoryHint: .isDirectory),
			withIntermediateDirectories: true,
		)
		try mountWithApproval(dataDirectory: request.dataDirectory, at: request.mountPath)
	}

	private func waitForExtensionApproval() throws {
		let semaphore = DispatchSemaphore(value: 0)
		let box = VfsBrokerAsyncResult()
		Task { @MainActor in
			do {
				try await waitForExtensionApprovalAsync()
				box.result = .success(())
			} catch {
				box.result = .failure(error)
			}
			semaphore.signal()
		}
		semaphore.wait()
		try box.result.get()
	}

	@MainActor
	private func waitForExtensionApprovalAsync() async throws {
		let alert = NSAlert()
		alert.messageText = "Enable Tangram Filesystem"
		alert.informativeText = "Tangram needs Tangram Filesystem enabled before it can mount the artifact file system. System Settings will open the File System Extensions list. Enable Tangram Filesystem, then Tangram will continue automatically."
		alert.addButton(withTitle: "Open System Settings")
		alert.addButton(withTitle: "Cancel")
		guard alert.runModal() == .alertFirstButtonReturn else {
			throw VfsBrokerError.extensionApprovalCancelled
		}
		openFileSystemExtensionSettings()
	}

	@MainActor
	private func openFileSystemExtensionSettings() {
		let urls = [
			"x-apple.systempreferences:com.apple.LoginItems-Settings.extension",
			"x-apple.systempreferences:com.apple.LoginItems-Settings",
		]
		for string in urls {
			if let url = URL(string: string), NSWorkspace.shared.open(url) {
				return
			}
		}
	}

	private func mountWithApproval(dataDirectory: String, at mountPath: String) throws {
		let output = try mountOnce(dataDirectory: dataDirectory, at: mountPath)
		if output.status == 0 {
			return
		}
		guard output.message.contains(" is disabled") else {
			throw VfsBrokerError.mountFailed(output.message)
		}

		try waitForExtensionApproval()
		var lastMessage = output.message
		for _ in 0 ..< 300 {
			Thread.sleep(forTimeInterval: 1)
			let output = try mountOnce(dataDirectory: dataDirectory, at: mountPath)
			if output.status == 0 {
				return
			}
			lastMessage = output.message
		}
		throw VfsBrokerError.extensionApprovalTimedOut(lastMessage)
	}

	private func mountOnce(dataDirectory: String, at mountPath: String) throws -> (status: Int32, message: String) {
		let mountURL = URL(filePath: mountPath, directoryHint: .isDirectory).standardizedFileURL
		let path = mountURL.path()
		guard !mountedPaths.contains(path) else {
			return (0, "")
		}

		let output = try run(
			executable: URL(filePath: "/sbin/mount"),
			arguments: [
				"-F",
				"-r",
				"-t", "tangram",
				"-o", "nobrowse,nodev,nosuid,rdonly",
				URL(filePath: dataDirectory, directoryHint: .isDirectory).standardizedFileURL.path(),
				path,
			],
		)
		if output.status == 0 {
			mountedPaths.insert(path)
		}
		return output
	}

	private func unmount(_ mountURL: URL) throws {
		let output = try run(
			executable: URL(filePath: "/sbin/umount"),
			arguments: [mountURL.path()],
		)
		if output.status != 0 {
			throw VfsBrokerError.mountFailed(output.message)
		}
	}

	private func run(executable: URL, arguments: [String]) throws -> (status: Int32, message: String) {
		let process = Process()
		let pipe = Pipe()
		process.executableURL = executable
		process.arguments = arguments
		process.standardOutput = pipe
		process.standardError = pipe
		try process.run()
		let data = pipe.fileHandleForReading.readDataToEndOfFile()
		process.waitUntilExit()
		let message = String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
		return (process.terminationStatus, message)
	}

	private func readLine(from fd: Int32) throws -> Data {
		var data = Data()
		var byte: UInt8 = 0
		while true {
			let count = Darwin.read(fd, &byte, 1)
			if count == 1 {
				if byte == 0x0a {
					return data
				}
				data.append(byte)
			} else if count == 0 {
				return data
			} else if errno != EINTR {
				throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
			}
		}
	}

	private func write(_ response: VfsBrokerResponse, to fd: Int32) throws {
		var data = try JSONEncoder().encode(response)
		data.append(0x0a)
		try data.withUnsafeBytes { buffer in
			var written = 0
			while written < buffer.count {
				let count = Darwin.write(fd, buffer.baseAddress!.advanced(by: written), buffer.count - written)
				if count > 0 {
					written += count
				} else if errno != EINTR {
					throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
				}
			}
		}
	}
}

@MainActor
private final class AppModel: ObservableObject {
	@Published var details = ""
	@Published var isServerRunning = false
	@Published var launchAtLogin = false
	@Published var launchAtLoginError: String?
	@Published var log = ""
	@Published var selectedTab = Tab.info
	@Published var serverStatus = "Server: stopped"

	var restartServer: () -> Void = {}
	var setLaunchAtLogin: (Bool) -> Void = { _ in }
	var startServer: () -> Void = {}
	var stopServer: () -> Void = {}
}

@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate, NSWindowDelegate, NSToolbarDelegate {
	private let model = AppModel()
	private let statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
	private let serverMenuItem = NSMenuItem(title: "Server: stopped", action: nil, keyEquivalent: "")
	private let tabControl = NSSegmentedControl(
		labels: Tab.allCases.map(\.rawValue),
		trackingMode: .selectOne,
		target: nil,
		action: nil,
	)
	private var window: NSWindow?
	private var process: Process?
	private var logHandle: FileHandle?
	private var logPipe: Pipe?
	private var logReadOffset: UInt64 = 0
	private var logTailTimer: Timer?
	private var lastError: String?
	private var isStopping = false
	private var vfsBroker: VfsBroker?
	private let launchAtLoginPromptKey = "TangramLaunchAtLoginPrompted"

	private var homeDirectory: URL {
		FileManager.default.homeDirectoryForCurrentUser
	}

	private var realHomeDirectory: URL {
		guard let passwd = getpwuid(getuid()) else {
			return homeDirectory
		}
		return URL(filePath: String(cString: passwd.pointee.pw_dir), directoryHint: .isDirectory)
	}

	private var dataDirectory: URL {
		realHomeDirectory.appending(path: ".tangram", directoryHint: .isDirectory)
	}

	private var configFile: URL {
		homeDirectory.appending(path: ".config/tangram/config.json")
	}

	private var logFile: URL {
		dataDirectory.appending(path: "log")
	}

	private var pidFile: URL {
		dataDirectory.appending(path: "lock")
	}

	private var artifactsPath: URL {
		dataDirectory.appending(path: "artifacts", directoryHint: .isDirectory)
	}

	private var fskitExecutable: URL {
		Bundle.main.bundleURL
			.appending(path: "Contents/Extensions/TangramFSKit.appex/Contents/MacOS/TangramFSKit")
			.standardizedFileURL
	}

	private var tangramExecutable: URL {
		Bundle.main.bundleURL.appending(path: "Contents/Helpers/tangram").standardizedFileURL
	}

	func applicationDidFinishLaunching(_ notification: Notification) {
		installMenuBarItem()
		installMainMenu()
		installActions()
		startServer()
		showWindow(nil)
		refreshLaunchAtLoginStatus()
		promptForLaunchAtLoginIfNeeded()
	}

	func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
		showWindow(nil)
		return true
	}

	func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
		false
	}

	func applicationWillTerminate(_ notification: Notification) {
		stopTailingLog()
		stopServer()
	}

	func windowShouldClose(_ sender: NSWindow) -> Bool {
		hideWindow()
		return false
	}

	@objc func showWindow(_ sender: Any?) {
		NSApp.setActivationPolicy(.regular)
		let window = window ?? makeWindow()
		self.window = window
		updateStatus()
		window.makeKeyAndOrderFront(nil)
		NSApp.activate(ignoringOtherApps: true)
	}

	func hideWindow() {
		window?.orderOut(nil)
		NSApp.setActivationPolicy(.accessory)
	}

	private var serverStatus: String {
		if let process, process.isRunning {
			"running, pid \(process.processIdentifier)"
		} else if let lastError {
			"stopped, \(lastError)"
		} else {
			"stopped"
		}
	}

	private func installActions() {
		model.startServer = { [weak self] in
			self?.startServer()
		}
		model.stopServer = { [weak self] in
			self?.stopServer()
			self?.updateStatus()
		}
		model.restartServer = { [weak self] in
			self?.stopServer()
			self?.startServer()
		}
		model.setLaunchAtLogin = { [weak self] enabled in
			self?.setLaunchAtLogin(enabled)
		}
	}

	private func startServer() {
		guard process?.isRunning != true else {
			return
		}

		do {
			appendDiagnostic("starting the server")
			appendDiagnostic("home: \(homeDirectory.path())")
			appendDiagnostic("real home: \(realHomeDirectory.path())")
			appendDiagnostic("data: \(dataDirectory.path())")
			appendDiagnostic("config: \(configFile.path())")
			appendDiagnostic("log: \(logFile.path())")
			appendDiagnostic("helper: \(tangramExecutable.path())")
			appendDiagnostic("helper exists: \(FileManager.default.fileExists(atPath: tangramExecutable.path()))")
			try FileManager.default.createDirectory(at: dataDirectory, withIntermediateDirectories: true)
			appendDiagnostic("created the data directory")
			for directory in [
				homeDirectory.appending(path: "tmp", directoryHint: .isDirectory),
				homeDirectory.appending(path: ".cache", directoryHint: .isDirectory),
				homeDirectory.appending(path: ".local/share", directoryHint: .isDirectory),
			] {
				try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
			}
			try FileManager.default.createDirectory(
				at: configFile.deletingLastPathComponent(),
				withIntermediateDirectories: true,
			)
			if !FileManager.default.fileExists(atPath: configFile.path()) {
				try Data("{\"vfs\":{}}\n".utf8).write(to: configFile)
				appendDiagnostic("created the config file")
			}
			try stopExistingServer()
			cleanupExistingVfsState()
			appendDiagnostic("stopped any existing server and VFS")
			if !FileManager.default.fileExists(atPath: logFile.path()) {
				FileManager.default.createFile(atPath: logFile.path(), contents: nil)
				appendDiagnostic("created the server log")
			}
			let logHandle = try FileHandle(forWritingTo: logFile)
			try logHandle.seekToEnd()
			appendDiagnostic("opened the server log")

			let process = Process()
			let vfsBroker = VfsBroker(socketDirectory: homeDirectory)
			try vfsBroker.start()
			appendDiagnostic("started the VFS broker at \(vfsBroker.socketPath.path())")
			self.vfsBroker = vfsBroker
			process.executableURL = tangramExecutable
			process.arguments = serverArguments()
			appendDiagnostic("server arguments: \(process.arguments?.joined(separator: " ") ?? "")")
			process.currentDirectoryURL = homeDirectory
			var environment = ProcessInfo.processInfo.environment
			environment["HOME"] = homeDirectory.path()
			environment["PWD"] = homeDirectory.path()
			environment["TMPDIR"] = homeDirectory.appending(path: "tmp", directoryHint: .isDirectory).path()
			environment["XDG_CACHE_HOME"] = homeDirectory.appending(path: ".cache", directoryHint: .isDirectory).path()
			environment["XDG_CONFIG_HOME"] = homeDirectory.appending(path: ".config", directoryHint: .isDirectory).path()
			environment["XDG_DATA_HOME"] = homeDirectory.appending(path: ".local/share", directoryHint: .isDirectory).path()
			environment["TANGRAM_MACOS_APP_SOCKET"] = vfsBroker.socketPath.path()
			environment["TANGRAM_MACOS_APP_TOKEN"] = vfsBroker.token
			process.environment = environment
			process.standardInput = FileHandle.nullDevice
			let logPipe = Pipe()
			logPipe.fileHandleForReading.readabilityHandler = { [weak self, weak logHandle] handle in
				let data = handle.availableData
				guard !data.isEmpty else {
					return
				}
				try? logHandle?.write(contentsOf: data)
				let text = String(decoding: data, as: UTF8.self)
				Task { @MainActor in
					self?.appendDiagnostic(text.trimmingCharacters(in: .newlines))
				}
			}
			process.standardOutput = logPipe
			process.standardError = logPipe
			process.terminationHandler = { [weak self] process in
				Task { @MainActor in
					self?.serverDidExit(process)
				}
			}

			try process.run()
			appendDiagnostic("started the server process with pid \(process.processIdentifier)")
			self.process = process
			self.logHandle = logHandle
			self.logPipe = logPipe
			startTailingLog()
			lastError = nil
		} catch {
			self.process = nil
			self.logPipe = nil
			self.logHandle = nil
			self.vfsBroker?.stop()
			self.vfsBroker = nil
			cleanupExistingVfsState()
			appendDiagnostic("failed to start the server: \(error.localizedDescription)")
			lastError = error.localizedDescription
		}
		updateStatus()
	}

	private func stopServer() {
		guard let process else {
			cleanupExistingVfsState()
			updateStatus()
			return
		}

		isStopping = true
		do {
			try stopProcess(process.processIdentifier) {
				process.isRunning
			}
		} catch {
			lastError = error.localizedDescription
		}
		serverDidExit(process)
		isStopping = false
	}

	private func stopExistingServer() throws {
		guard let pid = try readServerPid(), pid != getpid(), processIsRunning(pid) else {
			return
		}
		try stopProcess(pid) {
			self.processIsRunning(pid)
		}
	}

	private func cleanupExistingVfsState() {
		unmountArtifactsIfNeeded()
		terminateLingeringFSKitExtensions()
	}

	private func unmountArtifactsIfNeeded() {
		let output = runForCleanup(executable: URL(filePath: "/sbin/umount"), arguments: [artifactsPath.path()])
		if output.status == 0 {
			appendDiagnostic("unmounted the artifacts VFS")
		}
	}

	private func terminateLingeringFSKitExtensions() {
		let output = runForCleanup(executable: URL(filePath: "/usr/bin/pkill"), arguments: ["-f", fskitExecutable.path()])
		if output.status == 0 {
			appendDiagnostic("stopped lingering FSKit extension instances")
		}
	}

	private func runForCleanup(executable: URL, arguments: [String]) -> (status: Int32, message: String) {
		let process = Process()
		let pipe = Pipe()
		process.executableURL = executable
		process.arguments = arguments
		process.standardOutput = pipe
		process.standardError = pipe
		do {
			try process.run()
		} catch {
			return (1, error.localizedDescription)
		}
		let data = pipe.fileHandleForReading.readDataToEndOfFile()
		process.waitUntilExit()
		let message = String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
		return (process.terminationStatus, message)
	}

	private func serverArguments() -> [String] {
		var arguments = ["-c", configFile.path()]
		arguments += ["-d", dataDirectory.path(), "serve"]
		return arguments
	}

	private func serverDidExit(_ exitedProcess: Process) {
		guard process === exitedProcess else {
			return
		}
		appendDiagnostic("server process exited with status \(exitedProcess.terminationStatus)")
		process = nil
		vfsBroker?.stop()
		vfsBroker = nil
		cleanupExistingVfsState()
		try? logHandle?.close()
		logPipe?.fileHandleForReading.readabilityHandler = nil
		logPipe = nil
		logHandle = nil
		if !isStopping, exitedProcess.terminationStatus != 0 {
			lastError = "server exited with status \(exitedProcess.terminationStatus)"
		}
		updateStatus()
	}

	private func startTailingLog() {
		stopTailingLog()
		logReadOffset = 0
		model.log = ""
		appendAvailableLogData()
		logTailTimer = Timer.scheduledTimer(withTimeInterval: 0.5, repeats: true) { [weak self] _ in
			Task { @MainActor in
				self?.appendAvailableLogData()
			}
		}
	}

	private func stopTailingLog() {
		logTailTimer?.invalidate()
		logTailTimer = nil
	}

	private func appendAvailableLogData() {
		do {
			let handle = try FileHandle(forReadingFrom: logFile)
			defer {
				try? handle.close()
			}
			let endOffset = try handle.seekToEnd()
			if endOffset < logReadOffset {
				logReadOffset = 0
				model.log = ""
			}
			guard endOffset > logReadOffset else {
				return
			}
			try handle.seek(toOffset: logReadOffset)
			let data = try handle.read(upToCount: Int(endOffset - logReadOffset)) ?? Data()
			logReadOffset = endOffset
			guard !data.isEmpty else {
				return
			}
			model.log += String(decoding: data, as: UTF8.self)
		} catch {
			if model.log.isEmpty {
				model.log = "failed to read the log: \(error.localizedDescription)"
			}
		}
	}

	private func appendDiagnostic(_ message: String) {
		let line = "[app] \(message)\n"
		logger.info("\(line.trimmingCharacters(in: .newlines), privacy: .public)")
		model.log += line
	}

	private func readServerPid() throws -> pid_t? {
		do {
			let contents = try String(contentsOf: pidFile, encoding: .utf8)
			let string = contents.trimmingCharacters(in: .whitespacesAndNewlines)
			guard !string.isEmpty else {
				return nil
			}
			guard let pid = pid_t(string), pid > 0 else {
				throw AppError.invalidPidFile
			}
			return pid
		} catch {
			if let error = error as? CocoaError, error.code == .fileReadNoSuchFile {
				return nil
			}
			throw error
		}
	}

	private func processIsRunning(_ pid: pid_t) -> Bool {
		if kill(pid, 0) == 0 {
			return true
		}
		return errno != ESRCH
	}

	private func stopProcess(_ pid: pid_t, isRunning: () -> Bool) throws {
		if isRunning() {
			try sendSignal(SIGINT, to: pid)
			waitBriefly(while: isRunning)
		}
		if isRunning() {
			try sendSignal(SIGTERM, to: pid)
			waitBriefly(while: isRunning)
		}
		if isRunning() {
			try sendSignal(SIGKILL, to: pid)
			waitBriefly(while: isRunning)
		}
		if isRunning() {
			throw AppError.failedToStopServer
		}
	}

	private func sendSignal(_ signal: Int32, to pid: pid_t) throws {
		if kill(pid, signal) == 0 {
			return
		}
		let error = errno
		if error == ESRCH {
			return
		}
		throw POSIXError(POSIXErrorCode(rawValue: error) ?? .EPERM)
	}

	private func waitBriefly(while isRunning: () -> Bool) {
		for _ in 0..<40 where isRunning() {
			usleep(50_000)
		}
	}

	private func refreshLaunchAtLoginStatus() {
		let status = SMAppService.mainApp.status
		model.launchAtLogin = matches(status, .enabled) || matches(status, .requiresApproval)
	}

	private func promptForLaunchAtLoginIfNeeded() {
		let defaults = UserDefaults.standard
		guard !defaults.bool(forKey: launchAtLoginPromptKey),
		      matches(SMAppService.mainApp.status, .notRegistered)
		else {
			return
		}
		defaults.set(true, forKey: launchAtLoginPromptKey)

		let alert = NSAlert()
		alert.messageText = "Open Tangram at Login?"
		alert.informativeText = "Tangram can start automatically when you log in."
		alert.addButton(withTitle: "Open at Login")
		alert.addButton(withTitle: "Not Now")
		if alert.runModal() == .alertFirstButtonReturn {
			setLaunchAtLogin(true)
		}
	}

	private func setLaunchAtLogin(_ enabled: Bool) {
		do {
			let status = SMAppService.mainApp.status
			if enabled {
				if !matches(status, .enabled), !matches(status, .requiresApproval) {
					try SMAppService.mainApp.register()
				}
			} else if !matches(status, .notRegistered) {
				try SMAppService.mainApp.unregister()
			}
			model.launchAtLoginError = nil
		} catch {
			model.launchAtLoginError = error.localizedDescription
		}
		refreshLaunchAtLoginStatus()
	}

	private func matches(_ status: SMAppService.Status, _ other: SMAppService.Status) -> Bool {
		status.rawValue == other.rawValue
	}

	private func updateStatus() {
		let status = serverStatus
		let isRunning = process?.isRunning == true
		serverMenuItem.title = "Server: \(status)"
		model.serverStatus = "Server: \(status)"
		model.isServerRunning = isRunning
		model.details = [
			"Data: \(dataDirectory.path())",
			"Config: \(configFile.path())",
			"Log: \(logFile.path())",
			"PID: \(pidFile.path())",
		].joined(separator: "\n")
	}

	private func makeWindow() -> NSWindow {
		let window = NSWindow(
			contentRect: NSRect(x: 0, y: 0, width: 720, height: 480),
			styleMask: [.titled, .closable, .miniaturizable, .resizable],
			backing: .buffered,
			defer: false,
		)
		window.title = "Tangram"
		window.titleVisibility = .hidden
		window.toolbarStyle = .unified
		window.isReleasedWhenClosed = false
		window.delegate = self
		window.center()
		installToolbar(for: window)
		window.contentView = NSHostingView(rootView: RootView(model: model))
		return window
	}

	private func installToolbar(for window: NSWindow) {
		let toolbar = NSToolbar(identifier: Toolbar.identifier)
		toolbar.delegate = self
		toolbar.displayMode = .iconOnly
		toolbar.allowsUserCustomization = false
		toolbar.autosavesConfiguration = false
		toolbar.centeredItemIdentifier = Toolbar.tabs
		window.toolbar = toolbar
	}

	private func configureTabControl() {
		tabControl.selectedSegment = Tab.allCases.firstIndex(of: model.selectedTab) ?? 0
		tabControl.target = self
		tabControl.action = #selector(tabDidChange(_:))
		tabControl.controlSize = .regular
		tabControl.segmentStyle = .texturedSquare
		tabControl.frame = NSRect(x: 0, y: 0, width: 220, height: 28)
	}

	@objc private func tabDidChange(_ sender: NSSegmentedControl) {
		guard Tab.allCases.indices.contains(sender.selectedSegment) else {
			return
		}
		model.selectedTab = Tab.allCases[sender.selectedSegment]
	}

	func toolbarDefaultItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
		[.flexibleSpace, Toolbar.tabs, .flexibleSpace]
	}

	func toolbarAllowedItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
		[.flexibleSpace, Toolbar.tabs]
	}

	func toolbar(
		_ toolbar: NSToolbar,
		itemForItemIdentifier itemIdentifier: NSToolbarItem.Identifier,
		willBeInsertedIntoToolbar flag: Bool,
	) -> NSToolbarItem? {
		guard itemIdentifier == Toolbar.tabs else {
			return nil
		}
		configureTabControl()
		let item = NSToolbarItem(itemIdentifier: itemIdentifier)
		item.label = "Tabs"
		item.paletteLabel = "Tabs"
		item.view = tabControl
		return item
	}

	private func installMenuBarItem() {
		statusItem.button?.image = menuBarImage()
		statusItem.button?.toolTip = "Tangram"

		let menu = NSMenu()
		menu.addItem(menuItem("Show Window", #selector(showWindow(_:))))
		menu.addItem(.separator())
		serverMenuItem.isEnabled = false
		menu.addItem(serverMenuItem)
		menu.addItem(.separator())
		menu.addItem(withTitle: "Quit Tangram", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
		statusItem.menu = menu
	}

	private func installMainMenu() {
		let mainMenu = NSMenu()
		let appItem = NSMenuItem()
		mainMenu.addItem(appItem)

		let appMenu = NSMenu()
		appItem.submenu = appMenu
		appMenu.addItem(menuItem("Show Window", #selector(showWindow(_:))))
		appMenu.addItem(.separator())
		appMenu.addItem(withTitle: "Quit Tangram", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
		NSApp.mainMenu = mainMenu
	}

	private func menuItem(_ title: String, _ action: Selector) -> NSMenuItem {
		let item = NSMenuItem(title: title, action: action, keyEquivalent: "")
		item.target = self
		return item
	}

	private func menuBarImage() -> NSImage? {
		if let image = NSImage(named: "MenuBarIcon") {
			image.size = NSSize(width: 18, height: 18)
			return image
		}
		let image = NSImage(systemSymbolName: "triangle.fill", accessibilityDescription: "Tangram")
		image?.isTemplate = true
		return image
	}
}

private struct RootView: View {
	@ObservedObject var model: AppModel

	var body: some View {
		Group {
			switch model.selectedTab {
			case .info:
				InfoView(model: model)
			case .logs:
				LogsView(log: model.log)
			}
		}
		.frame(maxWidth: .infinity, maxHeight: .infinity)
	}
}

private struct InfoView: View {
	@ObservedObject var model: AppModel

	var body: some View {
		VStack(alignment: .leading, spacing: 12) {
			Text(model.serverStatus)
				.font(.system(size: 15, weight: .bold))
			Text(model.details)
				.font(.system(size: 12, design: .monospaced))
				.textSelection(.enabled)
			HStack(spacing: 8) {
				Button("Start", action: model.startServer)
					.disabled(model.isServerRunning)
				Button("Stop", action: model.stopServer)
					.disabled(!model.isServerRunning)
				Button("Restart", action: model.restartServer)
					.disabled(!model.isServerRunning)
			}
			Toggle("Open at Login", isOn: Binding(
				get: { model.launchAtLogin },
				set: { enabled in model.setLaunchAtLogin(enabled) },
			))
			if let error = model.launchAtLoginError {
				Text(error)
					.font(.system(size: 12))
					.foregroundStyle(.red)
					.textSelection(.enabled)
			}
		}
		.frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
		.padding(20)
	}
}

private struct LogsView: View {
	let log: String

	var body: some View {
		LogTextView(text: log)
	}
}

private struct LogTextView: NSViewRepresentable {
	let text: String

	func makeCoordinator() -> Coordinator {
		Coordinator()
	}

	func makeNSView(context: Context) -> NSScrollView {
		let scrollView = NSScrollView()
		scrollView.hasVerticalScroller = true
		scrollView.hasHorizontalScroller = true
		scrollView.autohidesScrollers = false
		scrollView.borderType = .noBorder

		let textView = NSTextView()
		textView.isEditable = false
		textView.isSelectable = true
		textView.drawsBackground = true
		textView.backgroundColor = .textBackgroundColor
		textView.textColor = .labelColor
		textView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
		textView.textContainerInset = NSSize(width: 12, height: 12)
		textView.minSize = NSSize(width: 0, height: scrollView.contentSize.height)
		textView.maxSize = NSSize(
			width: CGFloat.greatestFiniteMagnitude,
			height: CGFloat.greatestFiniteMagnitude,
		)
		textView.isVerticallyResizable = true
		textView.isHorizontallyResizable = true
		textView.autoresizingMask = [.width, .height]
		textView.textContainer?.widthTracksTextView = false
		textView.textContainer?.containerSize = NSSize(
			width: CGFloat.greatestFiniteMagnitude,
			height: CGFloat.greatestFiniteMagnitude,
		)
		scrollView.documentView = textView
		context.coordinator.textView = textView
		return scrollView
	}

	func updateNSView(_ scrollView: NSScrollView, context: Context) {
		guard let textView = context.coordinator.textView else {
			return
		}
		if text.hasPrefix(context.coordinator.text) {
			let suffix = String(text.dropFirst(context.coordinator.text.count))
			if !suffix.isEmpty {
				append(suffix, to: textView)
			}
		} else if textView.string != text {
			textView.string = text
		}
		context.coordinator.text = text
		textView.scrollRangeToVisible(NSRange(location: textView.string.utf16.count, length: 0))
	}

	private func append(_ string: String, to textView: NSTextView) {
		let attributes: [NSAttributedString.Key: Any] = [
			.font: NSFont.monospacedSystemFont(ofSize: 12, weight: .regular),
			.foregroundColor: NSColor.labelColor,
		]
		textView.textStorage?.append(NSAttributedString(string: string, attributes: attributes))
	}

	final class Coordinator {
		var text = ""
		weak var textView: NSTextView?
	}
}
