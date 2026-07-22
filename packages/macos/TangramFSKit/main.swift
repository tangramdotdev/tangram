import ExtensionFoundation
import FSKit
import Foundation
import os

private let logger = Logger(subsystem: "tangram.fskit", category: "extension")

private let appGroupIdentifier: String = {
	guard
		let identifier = Bundle.main.object(forInfoDictionaryKey: "TangramAppGroupIdentifier") as? String,
		!identifier.isEmpty
	else {
		fatalError("missing the TangramAppGroupIdentifier Info.plist key")
	}
	return identifier
}()

private func posixError(_ code: Int32) -> Error {
	fs_errorForPOSIXError(code)
}

private func resourceKey(_ resource: FSResource) -> String {
	if let resource = resource as? FSPathURLResource {
		return resource.url.standardizedFileURL.path()
	}
	return String(describing: resource)
}

private func mountOptions(_ options: FSTaskOptions) -> [String: String] {
	var result: [String: String] = [:]
	var iterator = options.taskOptions.makeIterator()
	while let option = iterator.next() {
		guard option == "-o", let value = iterator.next() else {
			continue
		}
		for entry in value.split(separator: ",") {
			let parts = entry.split(separator: "=", maxSplits: 1)
			guard parts.count == 2 else {
				continue
			}
			result[String(parts[0])] = String(parts[1])
		}
	}
	return result
}

private let nodeTTLSeconds: UInt64 = 60

private let nodeEvictionIntervalSeconds: UInt64 = 30

private final class Completion {
	let handler: (TgResponse?, Int32) -> Void

	init(_ handler: @escaping (TgResponse?, Int32) -> Void) {
		self.handler = handler
	}
}

private let tangramCallback: TgBatchCallback = { userData, batch in
	let completion = Unmanaged<Completion>.fromOpaque(userData!).takeRetainedValue()
	var response: TgResponse?
	let status = tg_response_batch_get(batch, 0, &response)
	if status < 0 {
		logger.error("operation failed: errno=\(-status, privacy: .public)")
	}
	completion.handler(status < 0 ? nil : response, status)
	_ = tg_response_batch_free(batch)
}

// Submits an asynchronous operation and routes its completion to the handler.
private func submit(
	_ handler: @escaping (TgResponse?, Int32) -> Void,
	_ call: (TgBatchCallback?, UnsafeMutableRawPointer) -> Int32,
) {
	let completion = Completion(handler)
	let context = Unmanaged.passRetained(completion).toOpaque()
	let status = call(tangramCallback, context)
	if status != 0 {
		Unmanaged<Completion>.fromOpaque(context).release()
		handler(nil, status)
	}
}

fileprivate final class SecurityScopedDirectory {
	let url: URL
	private let didStartAccessing: Bool

	init(url: URL) throws {
		self.url = url.standardizedFileURL
		didStartAccessing = self.url.startAccessingSecurityScopedResource()
		// Log whether the sandbox granted the scope, which determines whether the
		// fast path can open the object store.
		logger.info(
			"security scoped access to \(self.url.path(percentEncoded: false), privacy: .public): \(self.didStartAccessing, privacy: .public)",
		)
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

		// The provider is created in the volume's activate, because FSKit delivers the
		// mount's -o options there and not here.
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

private let rootNodeID: UInt64 = 1

final class TangramItem: FSItem {
	let nodeID: UInt64
	let parentID: UInt64
	var itemType: FSItem.ItemType
	let name: String
	var handle: UInt64?

	init(nodeID: UInt64, parentID: UInt64, itemType: FSItem.ItemType, name: String) {
		self.nodeID = nodeID
		self.parentID = parentID
		self.itemType = itemType
		self.name = name
		super.init()
	}
}

final class TangramVolume: FSVolume, FSVolume.Operations, FSVolume.OpenCloseOperations, FSVolume.ReadWriteOperations, FSVolume.XattrOperations {
	private var provider: TgVfsProvider?
	private let sourceDirectory: SecurityScopedDirectory
	private let root: TangramItem

	fileprivate init(sourceDirectory: SecurityScopedDirectory, volumeID: FSVolume.Identifier) {
		self.sourceDirectory = sourceDirectory
		root = TangramItem(nodeID: rootNodeID, parentID: rootNodeID, itemType: .directory, name: "/")
		super.init(volumeID: volumeID, volumeName: FSFileName(string: "Tangram"))
	}

	deinit {
		if let provider {
			_ = tg_provider_drop(provider)
		}
	}

	// Routes an operation to the provider, which exists only once the volume is
	// activated.
	private func withProvider(_ body: (TgVfsProvider) -> Int32) -> Int32 {
		guard let provider else {
			return -EIO
		}
		return body(provider)
	}

	private func nodeID(of item: FSItem) -> UInt64? {
		(item as? TangramItem)?.nodeID
	}

	private func itemType(for kind: TgAttrsKind) -> FSItem.ItemType {
		switch kind {
		case TgAttrsKindDirectory: .directory
		case TgAttrsKindSymlink: .symlink
		default: .file
		}
	}

	private func attributes(nodeID: UInt64, parentID: UInt64, from attrs: TgAttrs) -> FSItem.Attributes {
		let result = FSItem.Attributes()
		result.type = itemType(for: attrs.kind)
		result.mode = attrs.kind == TgAttrsKindDirectory ? 0o555 : (attrs.executable ? 0o555 : 0o444)
		result.linkCount = attrs.kind == TgAttrsKindDirectory ? 2 : 1
		result.uid = attrs.uid
		result.gid = attrs.gid
		result.fileID = FSItem.Identifier(rawValue: nodeID)!
		result.parentID = FSItem.Identifier(rawValue: parentID)!
		result.size = attrs.size
		result.allocSize = attrs.size
		let time = timespec(tv_sec: Int(attrs.mtime.secs), tv_nsec: Int(attrs.mtime.nanos))
		result.accessTime = time
		result.modifyTime = time
		result.changeTime = time
		result.birthTime = time
		return result
	}

	private func attributes(nodeID: UInt64, parentID: UInt64, from response: TgResponse) -> FSItem.Attributes? {
		var attrs = TgAttrs()
		guard tg_response_attrs(response, &attrs) == 0 else {
			return nil
		}
		return attributes(nodeID: nodeID, parentID: parentID, from: attrs)
	}

	var supportedVolumeCapabilities: FSVolume.SupportedCapabilities {
		let capabilities = FSVolume.SupportedCapabilities()
		capabilities.supportsPersistentObjectIDs = true
		capabilities.supports64BitObjectIDs = true
		capabilities.supportsSymbolicLinks = true
		capabilities.doesNotSupportSettingFilePermissions = true
		capabilities.doesNotSupportImmutableFiles = true
		capabilities.caseFormat = .sensitive
		return capabilities
	}

	var volumeStatistics: FSStatFSResult {
		let result = FSStatFSResult(fileSystemTypeName: "tangram")
		result.blockSize = 4096
		result.ioSize = 4096
		return result
	}

	var maximumLinkCount: Int { 1 }
	var maximumNameLength: Int { 255 }
	var restrictsOwnershipChanges: Bool { true }
	var truncatesLongNames: Bool { false }

	func mount(options: FSTaskOptions, replyHandler reply: @escaping (Error?) -> Void) {
		logger.info("mount task options: \(options.taskOptions, privacy: .public)")
		reply(nil)
	}

	func unmount(replyHandler reply: @escaping () -> Void) {
		reply()
	}

	func synchronize(flags: FSSyncFlags, replyHandler reply: @escaping (Error?) -> Void) {
		reply(nil)
	}

	func activate(options: FSTaskOptions, replyHandler reply: @escaping (FSItem?, Error?) -> Void) {
		// The resource is the server's data directory. Pass it to the provider so
		// that the fast path can read the object store and the cache directory
		// directly instead of sending every request over the socket. The security
		// scoped resource is held open by the volume for as long as the provider
		// lives.
		let dataDirectory = sourceDirectory.url.path(percentEncoded: false)

		// The server sends the object store's map size and path as mount options,
		// because lmdb requires a reader to open the environment with a map size at
		// least as large as the writer's. A zero map size and an empty path select
		// the defaults, which match the server's.
		let options = mountOptions(options)
		let objectStoreMapSize = options["object_store_map_size"].flatMap(UInt64.init) ?? 0
		let objectStorePath = options["object_store_path"] ?? ""

		// The object store's lock semaphores must be named so that the sandboxed
		// extension and the server can both open them. When the server does not
		// send a prefix, default to the app group identifier, which the sandbox
		// permits both processes to open. LMDB appends an 'r' or 'w' character.
		let objectStorePosixSemPrefix = options["object_store_posix_sem_prefix"] ?? appGroupIdentifier

		// The server sends the principal the mount serves and the grant tokens the
		// mount holds, which the provider uses to authorize access to each artifact.
		// An empty principal leaves the mount unenforced.
		let principal = options["principal"] ?? ""
		let tokens = options["tokens"] ?? ""

		// The client connects to the server over the unix socket the server sends as
		// a mount option, which lets concurrent servers each serve on their own
		// socket. It falls back to the socket in the shared app group container, the
		// only path the sandbox permits the extension to open.
		let socket: String
		if let socketOption = options["socket"] {
			socket = socketOption
		} else {
			guard let container = FileManager.default.containerURL(forSecurityApplicationGroupIdentifier: appGroupIdentifier) else {
				logger.error("failed to resolve the app group container")
				reply(nil, posixError(EIO))
				return
			}
			socket = container.appending(path: "socket").path(percentEncoded: false)
		}

		var provider: TgVfsProvider?
		logger.info(
			"creating provider for socket=\(socket, privacy: .public) data=\(dataDirectory, privacy: .public) object_store_map_size=\(objectStoreMapSize, privacy: .public) object_store_path=\(objectStorePath, privacy: .public) object_store_posix_sem_prefix=\(objectStorePosixSemPrefix, privacy: .public)",
		)
		let status = socket.withCString { socket in
			dataDirectory.withCString { dataDirectory in
				objectStorePath.withCString { objectStorePath in
					objectStorePosixSemPrefix.withCString { objectStorePosixSemPrefix in
						principal.withCString { principal in
							tokens.withCString { tokens in
								var config = TgConfig(
									data_directory: dataDirectory,
									node_eviction_interval_secs: nodeEvictionIntervalSeconds,
									node_ttl_secs: nodeTTLSeconds,
									object_store_map_size: objectStoreMapSize,
									object_store_path: objectStorePath,
									object_store_posix_sem_prefix: objectStorePosixSemPrefix,
									principal: principal,
									tokens: tokens,
								)
								return tg_provider_new(socket, &config, &provider)
							}
						}
					}
				}
			}
		}
		guard status == 0, let provider else {
			logger.error("tg_provider_new failed: status=\(status, privacy: .public)")
			reply(nil, posixError(status < 0 ? -status : EIO))
			return
		}
		logger.info("provider created")

		self.provider = provider
		reply(root, nil)
	}

	func deactivate(options: FSDeactivateOptions, replyHandler reply: @escaping (Error?) -> Void) {
		if let provider {
			_ = tg_provider_drop(provider)
			self.provider = nil
		}
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
		submit({ response, status in
			guard let response,
				let attributes = self.attributes(nodeID: item.nodeID, parentID: item.parentID, from: response)
			else {
				reply(nil, posixError(status < 0 ? -status : EIO))
				return
			}
			reply(attributes, nil)
		}, { callback, context in
			self.withProvider { tg_provider_getattr_async($0, item.nodeID, callback, context) }
		})
	}

	func setAttributes(
		_ newAttributes: FSItem.SetAttributesRequest,
		on item: FSItem,
		replyHandler reply: @escaping (FSItem.Attributes?, Error?) -> Void,
	) {
		reply(nil, posixError(EROFS))
	}

	func getXattr(
		named name: FSFileName,
		of item: FSItem,
		replyHandler reply: @escaping (Data?, Error?) -> Void,
	) {
		guard let nodeID = nodeID(of: item) else {
			reply(nil, posixError(EINVAL))
			return
		}
		let data = name.data
		submit({ response, status in
			var present = false
			var pointer: UnsafePointer<UInt8>?
			var length = 0
			guard let response, tg_response_xattr(response, &present, &pointer, &length) == 0 else {
				reply(nil, posixError(status < 0 ? -status : EIO))
				return
			}
			guard present, let pointer else {
				reply(nil, posixError(ENOATTR))
				return
			}
			reply(Data(bytes: pointer, count: length), nil)
		}, { callback, context in
			data.withUnsafeBytes { rawBuffer in
				let buffer = rawBuffer.bindMemory(to: UInt8.self)
				return self.withProvider {
					tg_provider_getxattr_async($0, nodeID, buffer.baseAddress, buffer.count, callback, context)
				}
			}
		})
	}

	func setXattr(
		named name: FSFileName,
		to value: Data?,
		on item: FSItem,
		policy: FSVolume.SetXattrPolicy,
		replyHandler reply: @escaping (Error?) -> Void,
	) {
		reply(posixError(EROFS))
	}

	func listXattrs(
		of item: FSItem,
		replyHandler reply: @escaping ([FSFileName]?, Error?) -> Void,
	) {
		guard let nodeID = nodeID(of: item) else {
			reply(nil, posixError(EINVAL))
			return
		}
		submit({ response, status in
			var count = 0
			guard let response, tg_response_xattr_names_len(response, &count) == 0 else {
				reply(nil, posixError(status < 0 ? -status : EIO))
				return
			}
			var names = [FSFileName]()
			names.reserveCapacity(count)
			for index in 0 ..< count {
				var pointer: UnsafePointer<UInt8>?
				var length = 0
				guard tg_response_xattr_names_get(response, index, &pointer, &length) == 0,
					let pointer
				else {
					reply(nil, posixError(EIO))
					return
				}
				let data = Data(bytes: pointer, count: length)
				names.append(FSFileName(data: data))
			}
			reply(names, nil)
		}, { callback, context in
			self.withProvider { tg_provider_listxattrs_async($0, nodeID, callback, context) }
		})
	}

	func lookupItem(
		named name: FSFileName,
		inDirectory directory: FSItem,
		replyHandler reply: @escaping (FSItem?, FSFileName?, Error?) -> Void,
	) {
		guard let parent = nodeID(of: directory), let component = name.string else {
			reply(nil, nil, posixError(EINVAL))
			return
		}
		let bytes = Array(component.utf8)
		submit({ response, status in
			var present = false
			var id: UInt64 = 0
			guard let response, tg_response_lookup(response, &present, &id) == 0, present else {
				reply(nil, nil, posixError(status < 0 ? -status : ENOENT))
				return
			}
			// Resolve the type with a follow-up getattr so the item is complete.
			submit({ response, status in
				var attrs = TgAttrs()
				guard let response, tg_response_attrs(response, &attrs) == 0 else {
					reply(nil, nil, posixError(status < 0 ? -status : EIO))
					return
				}
				// Take a lookup reference for the vended item, balanced by the forget in reclaimItem.
				_ = self.withProvider { tg_provider_remember_sync($0, id) }
				let type = self.itemType(for: attrs.kind)
				let item = TangramItem(nodeID: id, parentID: parent, itemType: type, name: component)
				reply(item, name, nil)
			}, { callback, context in
				self.withProvider { tg_provider_getattr_async($0, id, callback, context) }
			})
		}, { callback, context in
			bytes.withUnsafeBufferPointer { buffer in
				self.withProvider { tg_provider_lookup_async($0, parent, buffer.baseAddress, buffer.count, callback, context) }
			}
		})
	}

	func reclaimItem(_ item: FSItem, replyHandler reply: @escaping (Error?) -> Void) {
		if let nodeID = nodeID(of: item) {
			_ = self.withProvider { tg_provider_forget_sync($0, nodeID, 1) }
		}
		reply(nil)
	}

	func readSymbolicLink(_ item: FSItem, replyHandler reply: @escaping (FSFileName?, Error?) -> Void) {
		guard let nodeID = nodeID(of: item) else {
			reply(nil, posixError(EINVAL))
			return
		}
		submit({ response, status in
			var pointer: UnsafePointer<UInt8>?
			var length = 0
			guard let response, tg_response_bytes(response, &pointer, &length) == 0, let pointer else {
				reply(nil, posixError(status < 0 ? -status : EIO))
				return
			}
			let data = Data(bytes: pointer, count: length)
			reply(FSFileName(data: data), nil)
		}, { callback, context in
			self.withProvider { tg_provider_readlink_async($0, nodeID, callback, context) }
		})
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
		guard let nodeID = nodeID(of: directory) else {
			reply(verifier, posixError(ENOTDIR))
			return
		}
		let offset = UInt64(cookie.rawValue)
		submit({ response, status in
			guard let response else {
				reply(verifier, posixError(status < 0 ? -status : EIO))
				return
			}
			var count = 0
			guard tg_response_readdirplus_len(response, &count) == 0 else {
				reply(verifier, posixError(EIO))
				return
			}
			for index in 0 ..< count {
				var entry = TgDirEntryPlus()
				guard tg_response_readdirplus_get(response, index, &entry) == 0 else { continue }
				let data = Data(bytes: entry.name, count: entry.name_len)
				let name = FSFileName(data: data)
				// The entries are children of the directory being enumerated.
				let entryAttributes = desiredAttributes != nil
					? self.attributes(nodeID: entry.id, parentID: nodeID, from: entry.attrs)
					: nil
				let packed = packer.packEntry(
					name: name,
					itemType: self.itemType(for: entry.attrs.kind),
					itemID: FSItem.Identifier(rawValue: entry.id)!,
					nextCookie: FSDirectoryCookie(rawValue: cookie.rawValue + UInt64(index) + 1),
					attributes: entryAttributes,
				)
				// Stop when the kernel's buffer is full so unfit entries are re-requested from their cookie.
				if !packed {
					break
				}
			}
			reply(verifier, nil)
		}, { callback, context in
			self.withProvider { tg_provider_readdirplus_async($0, nodeID, offset, 64 * 1024, callback, context) }
		})
	}

	func openItem(_ item: FSItem, modes: FSVolume.OpenModes, replyHandler reply: @escaping (Error?) -> Void) {
		if modes.contains(.write) {
			reply(posixError(EROFS))
			return
		}
		guard let item = item as? TangramItem else {
			reply(posixError(EINVAL))
			return
		}
		if item.itemType != .file {
			reply(nil)
			return
		}
		submit({ response, status in
			var handle: UInt64 = 0
			guard let response, tg_response_open(response, &handle, nil) == 0 else {
				reply(posixError(status < 0 ? -status : EIO))
				return
			}
			item.handle = handle
			reply(nil)
		}, { callback, context in
			self.withProvider { tg_provider_open_async($0, item.nodeID, callback, context) }
		})
	}

	func closeItem(_ item: FSItem, modes: FSVolume.OpenModes, replyHandler reply: @escaping (Error?) -> Void) {
		if let item = item as? TangramItem, let handle = item.handle {
			_ = self.withProvider { tg_provider_close_sync($0, handle) }
			item.handle = nil
		}
		reply(nil)
	}

	func read(
		from item: FSItem,
		at offset: off_t,
		length: Int,
		into buffer: FSMutableFileDataBuffer,
		replyHandler reply: @escaping (Int, Error?) -> Void,
	) {
		guard let item = item as? TangramItem, let handle = item.handle else {
			reply(0, posixError(EBADF))
			return
		}
		guard offset >= 0 else {
			reply(0, posixError(EINVAL))
			return
		}
		submit({ response, status in
			var pointer: UnsafePointer<UInt8>?
			var count = 0
			guard let response, tg_response_bytes(response, &pointer, &count) == 0, let pointer else {
				reply(0, posixError(status < 0 ? -status : EIO))
				return
			}
			let copied = buffer.withUnsafeMutableBytes { raw -> Int in
				let count = min(count, raw.count)
				raw.baseAddress?.copyMemory(from: pointer, byteCount: count)
				return count
			}
			reply(copied, nil)
		}, { callback, context in
			self.withProvider { tg_provider_read_async($0, handle, UInt64(offset), UInt64(length), callback, context) }
		})
	}

	func write(
		contents: Data,
		to item: FSItem,
		at offset: off_t,
		replyHandler reply: @escaping (Int, Error?) -> Void,
	) {
		reply(0, posixError(EROFS))
	}
}
