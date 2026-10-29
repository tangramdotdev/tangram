// Encode an authorization token with a zero signature, which no server accepts, for tests of how clients merge tokens.
export let token = (
	resource: string,
	expiresAt: number | bigint,
	permissions: Array<string>,
	secretId = 0,
): string => {
	let mask = permissions.reduce(
		(mask, permission) => mask | (1n << BigInt(permissionIds[permission]!)),
		0n,
	);
	let id = [
		0,
		0,
		idKinds[resource.slice(0, 3)]!,
		Number(resource[5]),
		...decodeBase32(resource.slice(6)),
	];
	let bytes = [
		0,
		...uvarint(BigInt(expiresAt)),
		...uvarint(mask),
		...id,
		...uvarint(BigInt(secretId)),
		...new Array(16).fill(0),
	];
	return encodeBase32(bytes);
};

const symbols = "0123456789abcdefghjkmnpqrstvwxyz";

let decodeBase32 = (value: string): Array<number> => {
	let bytes: Array<number> = [];
	let buffer = 0;
	let bits = 0;
	for (let character of value) {
		buffer = ((buffer << 5) | symbols.indexOf(character)) & 0xfff;
		bits += 5;
		if (bits >= 8) {
			bits -= 8;
			bytes.push((buffer >> bits) & 0xff);
		}
	}
	return bytes;
};

let encodeBase32 = (bytes: Array<number>): string => {
	let output = "";
	let buffer = 0;
	let bits = 0;
	for (let byte of bytes) {
		buffer = ((buffer << 8) | byte) & 0xfff;
		bits += 8;
		while (bits >= 5) {
			bits -= 5;
			output += symbols[(buffer >> bits) & 31];
		}
	}
	if (bits > 0) {
		output += symbols[(buffer << (5 - bits)) & 31];
	}
	return output;
};

let uvarint = (value: bigint): Array<number> => {
	let bytes: Array<number> = [];
	while (value > 0x7fn) {
		bytes.push(Number(value & 0x7fn) | 0x80);
		value >>= 7n;
	}
	bytes.push(Number(value));
	return bytes;
};

const idKinds: Record<string, number> = {
	blb: 0,
	cmd: 5,
	dir: 1,
	fil: 2,
	pcs: 8,
	syn: 17,
};

const permissionIds: Record<string, number> = {
	object_node: 0,
	object_subtree: 1,
	process_node: 0,
	process_node_command_objects: 1,
	process_node_error_objects: 2,
	process_node_log_objects: 3,
	process_node_output_objects: 4,
	process_parent: 5,
	process_subtree: 6,
	process_subtree_command_objects: 7,
	process_subtree_error_objects: 8,
	process_subtree_log_objects: 9,
	process_subtree_output_objects: 10,
	sync_read: 0,
};
