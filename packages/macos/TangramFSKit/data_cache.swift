#if TANGRAM_FSKIT_DATA_CACHE

import FSKit

@available(macOS 27.0, *)
extension TangramVolume: FSVolume.DataCacheHandler {
	var isDataCacheInhibited: Bool { false }

	func open(
		_ item: FSItem,
		modes: FSVolume.OpenModes,
		cacheMode: FSVolume.DataCacheMode,
		context: FSContext,
		replyHandler reply: @escaping @Sendable (FSOpenItemResult?, (any Error)?) -> Void,
	) {
		let coherency = Self.coherency(for: cacheMode)
		openItem(item, modes: modes) { error in
			if let error {
				reply(nil, error)
				return
			}
			reply(FSOpenItemResult(grantedCoherency: coherency), nil)
		}
	}

	func close(_ item: FSItem, context: FSContext, replyHandler reply: @escaping @Sendable () -> Void) {
		closeItem(item, modes: []) { _ in reply() }
	}

	func upgrade(
		_ item: FSItem,
		cacheMode: FSVolume.DataCacheMode,
		context: FSContext,
		replyHandler reply: @escaping @Sendable (FSUpgradeItemResult?, (any Error)?) -> Void,
	) {
		reply(FSUpgradeItemResult(grantedCoherency: Self.coherency(for: cacheMode)), nil)
	}

	private static func coherency(for cacheMode: FSVolume.DataCacheMode) -> FSVolume.KernelCacheCoherencyType {
		switch cacheMode {
		case .none:
			.noCache
		default:
			.readCache
		}
	}
}

#endif
