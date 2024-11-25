import * as tg from "./index.ts";

export let checksum = (
	input: string | Uint8Array | tg.Blob | tg.Artifact,
	algorithm: Checksum.Algorithm,
): Promise<Checksum> => {
	return Checksum.new(input, algorithm);
};

export type Checksum = string;

export declare namespace Checksum {
	let new_: (
		input: string | Uint8Array | tg.Blob | tg.Artifact,
		algorithm: Checksum.Algorithm,
	) => Promise<Checksum>;
	export { new_ as new };
}

export namespace Checksum {
	export type Algorithm = "blake3" | "sha256" | "sha512" | "unsafe";

	export let new_ = async (
		input: string | Uint8Array | tg.Blob | tg.Artifact,
		algorithm: Checksum.Algorithm,
	): Promise<Checksum> => {
		if (typeof input === "string" || input instanceof Uint8Array) {
			return syscall("checksum", input, algorithm);
		} else if (tg.Blob.is(input)) {
			return await tg.Blob.checksum(input, algorithm);
		} else if (tg.Artifact.is(input)) {
			return await tg.Artifact.checksum(input, algorithm);
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
		} else if (checksum === "unsafe") {
			return "unsafe";
		} else {
			throw new Error("invalid checksum");
		}
	};
}
