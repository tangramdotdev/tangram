import AppKit
import Darwin
import ServiceManagement
import SwiftUI

private enum AppError: LocalizedError {
	case failedToStopServer
	case invalidPidFile

	var errorDescription: String? {
		switch self {
		case .failedToStopServer:
			"failed to stop the server"
		case .invalidPidFile:
			"invalid pid file"
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
	private var logReadOffset: UInt64 = 0
	private var logTailTimer: Timer?
	private var lastError: String?
	private var isStopping = false
	private let launchAtLoginPromptKey = "TangramLaunchAtLoginPrompted"

	private var homeDirectory: URL {
		FileManager.default.homeDirectoryForCurrentUser
	}

	private var dataDirectory: URL {
		homeDirectory.appending(path: ".tangram", directoryHint: .isDirectory)
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

	private var tangramExecutable: URL {
		Bundle.main.bundleURL.appending(path: "Contents/MacOS/tangram").standardizedFileURL
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
			try FileManager.default.createDirectory(at: dataDirectory, withIntermediateDirectories: true)
			try stopExistingServer()
			if !FileManager.default.fileExists(atPath: logFile.path()) {
				FileManager.default.createFile(atPath: logFile.path(), contents: nil)
			}
			let logHandle = try FileHandle(forWritingTo: logFile)
			try logHandle.seekToEnd()

			let process = Process()
			process.executableURL = tangramExecutable
			process.arguments = serverArguments()
			process.currentDirectoryURL = homeDirectory
			process.standardInput = FileHandle.nullDevice
			process.standardOutput = logHandle
			process.standardError = logHandle
			process.terminationHandler = { [weak self] process in
				Task { @MainActor in
					self?.serverDidExit(process)
				}
			}

			try process.run()
			self.process = process
			self.logHandle = logHandle
			startTailingLog()
			lastError = nil
		} catch {
			self.process = nil
			self.logHandle = nil
			lastError = error.localizedDescription
		}
		updateStatus()
	}

	private func stopServer() {
		guard let process else {
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

	private func serverArguments() -> [String] {
		var arguments: [String] = []
		if FileManager.default.fileExists(atPath: configFile.path()) {
			arguments += ["-c", configFile.path()]
		}
		arguments += ["-d", dataDirectory.path(), "serve"]
		return arguments
	}

	private func serverDidExit(_ exitedProcess: Process) {
		guard process === exitedProcess else {
			return
		}
		process = nil
		try? logHandle?.close()
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
				set: model.setLaunchAtLogin,
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
