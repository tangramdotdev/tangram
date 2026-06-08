import ExtensionFoundation
import FSKit
import Foundation

private let helloName = "hello.txt"
private let helloData = Data("Hello from Tangram FSKit.\n".utf8)

private func posixError(_ code: Int32) -> Error {
	fs_errorForPOSIXError(code)
}

private func resourceKey(_ resource: FSResource) -> String {
	if let resource = resource as? FSPathURLResource {
		return resource.url.standardizedFileURL.path()
	}
	return String(describing: resource)
}

fileprivate final class SecurityScopedDirectory {
	let url: URL
	private let didStartAccessing: Bool

	init(url: URL) throws {
		self.url = url.standardizedFileURL
		didStartAccessing = self.url.startAccessingSecurityScopedResource()
		guard FileManager.default.fileExists(atPath: self.url.path()) else {
			if didStartAccessing {
				self.url.stopAccessingSecurityScopedResource()
			}
			throw posixError(ENOENT)
		}
	}

	deinit {
		if didStartAccessing {
			url.stopAccessingSecurityScopedResource()
		}
	}
}

@main
struct TangramFSKitExtension: UnaryFileSystemExtension {
	let fileSystem = TangramFileSystem()
}

final class TangramFileSystem: FSUnaryFileSystem, FSUnaryFileSystemOperations {
	private let lock = NSLock()
	private var resourceIdentifiers: [String: UUID] = [:]

	func probeResource(
		resource: FSResource,
		replyHandler reply: @escaping (FSProbeResult?, Error?) -> Void,
	) {
		let containerID = FSContainerIdentifier(uuid: identifier(for: resource))
		reply(.usableButLimited(name: "Tangram", containerID: containerID), nil)
	}

	func loadResource(
		resource: FSResource,
		options: FSTaskOptions,
		replyHandler reply: @escaping (FSVolume?, Error?) -> Void,
	) {
		guard let resource = resource as? FSPathURLResource else {
			reply(nil, posixError(EINVAL))
			return
		}

		let sourceDirectory: SecurityScopedDirectory
		do {
			sourceDirectory = try SecurityScopedDirectory(url: resource.url)
		} catch {
			reply(nil, error)
			return
		}

		let volumeID = FSVolume.Identifier(uuid: identifier(for: resource))
		containerStatus = .ready
		reply(TangramVolume(sourceDirectory: sourceDirectory, volumeID: volumeID), nil)
	}

	func unloadResource(
		resource: FSResource,
		options: FSTaskOptions,
		replyHandler reply: @escaping (Error?) -> Void,
	) {
		containerStatus = .notReady(status: posixError(ENODEV))
		reply(nil)
	}

	private func identifier(for resource: FSResource) -> UUID {
		let key = resourceKey(resource)
		lock.lock()
		defer { lock.unlock() }
		if let identifier = resourceIdentifiers[key] {
			return identifier
		}
		let identifier = UUID()
		resourceIdentifiers[key] = identifier
		return identifier
	}
}

final class TangramItem: FSItem {
	let id: FSItem.Identifier
	let itemType: FSItem.ItemType
	let name: String

	init(id: FSItem.Identifier, itemType: FSItem.ItemType, name: String) {
		self.id = id
		self.itemType = itemType
		self.name = name
		super.init()
	}
}

final class TangramVolume: FSVolume, FSVolume.Operations, FSVolume.OpenCloseOperations, FSVolume.ReadWriteOperations {
	private let sourceDirectory: SecurityScopedDirectory
	private let root = TangramItem(id: .rootDirectory, itemType: .directory, name: "/")
	private let hello = TangramItem(id: FSItem.Identifier(rawValue: 3)!, itemType: .file, name: helloName)

	fileprivate init(sourceDirectory: SecurityScopedDirectory, volumeID: FSVolume.Identifier) {
		self.sourceDirectory = sourceDirectory
		super.init(volumeID: volumeID, volumeName: FSFileName(string: "Tangram"))
	}

	var supportedVolumeCapabilities: FSVolume.SupportedCapabilities {
		let capabilities = FSVolume.SupportedCapabilities()
		capabilities.supportsPersistentObjectIDs = true
		capabilities.supports64BitObjectIDs = true
		capabilities.doesNotSupportSettingFilePermissions = true
		capabilities.doesNotSupportImmutableFiles = true
		capabilities.caseFormat = .sensitive
		return capabilities
	}

	var volumeStatistics: FSStatFSResult {
		let result = FSStatFSResult(fileSystemTypeName: "tangram")
		result.blockSize = 4096
		result.ioSize = 4096
		result.totalBytes = UInt64(helloData.count)
		result.usedBytes = UInt64(helloData.count)
		result.totalFiles = 2
		return result
	}

	var maximumLinkCount: Int {
		1
	}

	var maximumNameLength: Int {
		255
	}

	var restrictsOwnershipChanges: Bool {
		true
	}

	var truncatesLongNames: Bool {
		false
	}

	var maximumFileSize: UInt64 {
		UInt64(helloData.count)
	}

	func mount(options: FSTaskOptions, replyHandler reply: @escaping (Error?) -> Void) {
		reply(nil)
	}

	func unmount(replyHandler reply: @escaping () -> Void) {
		reply()
	}

	func synchronize(flags: FSSyncFlags, replyHandler reply: @escaping (Error?) -> Void) {
		reply(nil)
	}

	func activate(options: FSTaskOptions, replyHandler reply: @escaping (FSItem?, Error?) -> Void) {
		reply(root, nil)
	}

	func deactivate(options: FSDeactivateOptions, replyHandler reply: @escaping (Error?) -> Void) {
		reply(nil)
	}

	func getAttributes(
		_ desiredAttributes: FSItem.GetAttributesRequest,
		of item: FSItem,
		replyHandler reply: @escaping (FSItem.Attributes?, Error?) -> Void,
	) {
		guard let item = item as? TangramItem else {
			reply(nil, posixError(EINVAL))
			return
		}
		reply(attributes(for: item), nil)
	}

	func setAttributes(
		_ newAttributes: FSItem.SetAttributesRequest,
		on item: FSItem,
		replyHandler reply: @escaping (FSItem.Attributes?, Error?) -> Void,
	) {
		reply(nil, posixError(EROFS))
	}

	func lookupItem(
		named name: FSFileName,
		inDirectory directory: FSItem,
		replyHandler reply: @escaping (FSItem?, FSFileName?, Error?) -> Void,
	) {
		guard (directory as? TangramItem)?.id == root.id else {
			reply(nil, nil, posixError(ENOTDIR))
			return
		}
		guard name.string == helloName else {
			reply(nil, nil, posixError(ENOENT))
			return
		}
		reply(hello, FSFileName(string: helloName), nil)
	}

	func reclaimItem(_ item: FSItem, replyHandler reply: @escaping (Error?) -> Void) {
		reply(nil)
	}

	func readSymbolicLink(_ item: FSItem, replyHandler reply: @escaping (FSFileName?, Error?) -> Void) {
		reply(nil, posixError(EINVAL))
	}

	func createItem(
		named name: FSFileName,
		type: FSItem.ItemType,
		inDirectory directory: FSItem,
		attributes newAttributes: FSItem.SetAttributesRequest,
		replyHandler reply: @escaping (FSItem?, FSFileName?, Error?) -> Void,
	) {
		reply(nil, nil, posixError(EROFS))
	}

	func createSymbolicLink(
		named name: FSFileName,
		inDirectory directory: FSItem,
		attributes newAttributes: FSItem.SetAttributesRequest,
		linkContents contents: FSFileName,
		replyHandler reply: @escaping (FSItem?, FSFileName?, Error?) -> Void,
	) {
		reply(nil, nil, posixError(EROFS))
	}

	func createLink(
		to item: FSItem,
		named name: FSFileName,
		inDirectory directory: FSItem,
		replyHandler reply: @escaping (FSFileName?, Error?) -> Void,
	) {
		reply(nil, posixError(EROFS))
	}

	func removeItem(
		_ item: FSItem,
		named name: FSFileName,
		fromDirectory directory: FSItem,
		replyHandler reply: @escaping (Error?) -> Void,
	) {
		reply(posixError(EROFS))
	}

	func renameItem(
		_ item: FSItem,
		inDirectory sourceDirectory: FSItem,
		named sourceName: FSFileName,
		to destinationName: FSFileName,
		inDirectory destinationDirectory: FSItem,
		overItem: FSItem?,
		replyHandler reply: @escaping (FSFileName?, Error?) -> Void,
	) {
		reply(nil, posixError(EROFS))
	}

	func enumerateDirectory(
		_ directory: FSItem,
		startingAt cookie: FSDirectoryCookie,
		verifier: FSDirectoryVerifier,
		attributes desiredAttributes: FSItem.GetAttributesRequest?,
		packer: FSDirectoryEntryPacker,
		replyHandler reply: @escaping (FSDirectoryVerifier, Error?) -> Void,
	) {
		guard (directory as? TangramItem)?.id == root.id else {
			reply(verifier, posixError(ENOTDIR))
			return
		}

		if cookie.rawValue == FSDirectoryCookie.initial.rawValue {
			_ = packer.packEntry(
				name: FSFileName(string: helloName),
				itemType: .file,
				itemID: hello.id,
				nextCookie: FSDirectoryCookie(rawValue: 1),
				attributes: desiredAttributes.map { _ in attributes(for: hello) },
			)
		}
		reply(FSDirectoryVerifier(rawValue: 1), nil)
	}

	func openItem(_ item: FSItem, modes: FSVolume.OpenModes, replyHandler reply: @escaping (Error?) -> Void) {
		if modes.contains(.write) {
			reply(posixError(EROFS))
		} else {
			reply(nil)
		}
	}

	func closeItem(_ item: FSItem, modes: FSVolume.OpenModes, replyHandler reply: @escaping (Error?) -> Void) {
		reply(nil)
	}

	func read(
		from item: FSItem,
		at offset: off_t,
		length: Int,
		into buffer: FSMutableFileDataBuffer,
		replyHandler reply: @escaping (Int, Error?) -> Void,
	) {
		guard (item as? TangramItem)?.id == hello.id else {
			reply(0, posixError(EISDIR))
			return
		}
		guard offset >= 0 else {
			reply(0, posixError(EINVAL))
			return
		}
		let start = Int(offset)
		guard start < helloData.count else {
			reply(0, nil)
			return
		}
		let count = min(length, helloData.count - start)
		let slice = helloData[start ..< start + count]
		buffer.withUnsafeMutableBytes { rawBuffer in
			_ = slice.copyBytes(to: rawBuffer)
		}
		reply(count, nil)
	}

	func write(
		contents: Data,
		to item: FSItem,
		at offset: off_t,
		replyHandler reply: @escaping (Int, Error?) -> Void,
	) {
		reply(0, posixError(EROFS))
	}

	private func attributes(for item: TangramItem) -> FSItem.Attributes {
		let attributes = FSItem.Attributes()
		attributes.type = item.itemType
		attributes.mode = item.itemType == .directory ? 0o555 : 0o444
		attributes.linkCount = item.itemType == .directory ? 2 : 1
		attributes.uid = getuid()
		attributes.gid = getgid()
		attributes.fileID = item.id
		attributes.parentID = item.id == root.id ? root.id : root.id
		attributes.size = item.itemType == .file ? UInt64(helloData.count) : 0
		attributes.allocSize = attributes.size
		let now = timespec(tv_sec: 0, tv_nsec: 0)
		attributes.accessTime = now
		attributes.modifyTime = now
		attributes.changeTime = now
		attributes.birthTime = now
		return attributes
	}

}
