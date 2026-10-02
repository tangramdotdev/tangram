import * as tg from "../index.ts";

const ERROR_NAME = "user.tangram.error";
const OUTCOME_NAME = "user.tangram.outcome";
const OUTPUT_NAME = "user.tangram.output";

export let readError = (path: string): Promise<Uint8Array | null> =>
	readProcessAttribute(path, ERROR_NAME);

export let readOutcome = (path: string): Promise<Uint8Array | null> =>
	readProcessAttribute(path, OUTCOME_NAME);

export let readOutput = (path: string): Promise<Uint8Array | null> =>
	readProcessAttribute(path, OUTPUT_NAME);

/** An empty process attribute marks the file contents as its serialized value. */
let readProcessAttribute = async (
	path: string,
	name: string,
): Promise<Uint8Array | null> => {
	let value = await readSharded(path, name);
	return value !== null && value.length === 0
		? await tg.host.readFile(path)
		: value;
};

/** Read a Tangram attribute stored as a single value or numbered shards. */
let readSharded = async (
	path: string,
	name: string,
): Promise<Uint8Array | null> => {
	let value = await tg.host.getxattr(path, name);
	let prefix = `${name}.`;
	let names = await tg.host.listxattr(path);
	let indices = new Map<number, string>();
	for (let name of names) {
		if (!name.startsWith(prefix)) {
			continue;
		}
		let suffix = name.slice(prefix.length);
		let index = Number(suffix);
		if (!Number.isSafeInteger(index) || index < 0 || suffix !== String(index)) {
			throw new Error("invalid xattr shard name");
		}
		indices.set(index, name);
	}
	if (indices.size === 0) {
		return value;
	}
	if (value !== null) {
		throw new Error("found both unsharded and sharded xattrs");
	}

	// Read the numbered shards in order before decoding the value.
	let shards: Array<Uint8Array> = [];
	let size = 0;
	let entries = [...indices.entries()].sort(([a], [b]) => a - b);
	for (let [expected, [index, name]] of entries.entries()) {
		if (index !== expected) {
			throw new Error("found a gap in the xattr shards");
		}
		let shard = await tg.host.getxattr(path, name);
		if (shard === null) {
			throw new Error("an xattr shard disappeared");
		}
		shards.push(shard);
		size += shard.length;
	}
	let output = new Uint8Array(size);
	let offset = 0;
	for (let shard of shards) {
		output.set(shard, offset);
		offset += shard.length;
	}

	return output;
};
