import * as tg from "./index.ts";

export let checksum = (
	input: string | Uint8Array | tg.Blob | tg.Artifact,
	algorithm: Checksum.Algorithm,
): Promise<Checksum> => {
	return Checksum.new(input, algorithm);
};

export type Checksum = `${tg.Checksum.Algorithm}${":" | "-"}${string}`;

export declare namespace Checksum {
	let new_: (
		input: string | Uint8Array | tg.Blob | tg.Artifact,
		algorithm: Checksum.Algorithm,
	) => Promise<Checksum>;
	export { new_ as new };
}

export namespace Checksum {
	export type Algorithm = "blake3" | "sha256" | "sha512";

	export let new_ = async (
		input: string | Uint8Array | tg.Blob | tg.Artifact,
		algorithm: Checksum.Algorithm,
	): Promise<Checksum> => {
		if (typeof input === "string" || input instanceof Uint8Array) {
			return syscall("checksum", input, algorithm);
		} else if (input instanceof tg.Blob) {
			let value = await tg.build({
				args: [input, algorithm],
				executable: "checksum",
				host: "builtin",
			});
			return value as tg.Checksum;
		} else if (tg.Artifact.is(input)) {
			let value = await tg.build({
				args: [input, algorithm],
				executable: "checksum",
				host: "builtin",
			});
			return value as tg.Checksum;
		} else {
			return tg.unreachable();
		}
	};
	Checksum.new = new_;

	export let algorithm = (checksum: Checksum): Algorithm => {
		if (checksum.includes(":")) {
			return checksum.split(":")[0]! as Algorithm;
		} else if (checksum.includes("-")) {
			return checksum.split("-")[0]! as Algorithm;
		} else {
			throw new Error("invalid checksum");
		}
	};

	export let is = (value: unknown): value is Checksum => {
		if (typeof value !== "string") {
			return false;
		}
		const pattern = /^(blake3|sha256|sha512)([-:])[a-zA-Z0-9+/]+=*$/;
		return pattern.test(value);
	};

	export let expect = (value: unknown): Checksum => {
		tg.assert(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Checksum => {
		tg.assert(is(value));
	};
}
