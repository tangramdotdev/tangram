import * as tg from "../index.ts";

const ERROR_NAME = "user.tangram.error";
const OUTPUT_NAME = "user.tangram.output";

export let readError = (path: string): Promise<Uint8Array | null> =>
	readSharded(path, ERROR_NAME);

export let readOutput = (path: string): Promise<Uint8Array | null> =>
	readSharded(path, OUTPUT_NAME);

/** Read a Tangram attribute stored as a single value or numbered shards. */
let readSharded = async (
	path: string,
	name: string,
): Promise<Uint8Array | null> => {
	let value = await tg.host.getxattr(path, name);
	if (value !== null) {
		return value;
	}

	// Read the numbered shards in order before decoding the value.
	let shards: Array<Uint8Array> = [];
	let size = 0;
	for (let index = 0; ; index++) {
		let shard = await tg.host.getxattr(path, `${name}.${index}`);
		if (shard === null) {
			break;
		}
		shards.push(shard);
		size += shard.length;
	}
	if (shards.length === 0) {
		return null;
	}
	let output = new Uint8Array(size);
	let offset = 0;
	for (let shard of shards) {
		output.set(shard, offset);
		offset += shard.length;
	}

	return output;
};
