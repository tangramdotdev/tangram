import { Artifact } from "./artifact.ts";
import { unreachable } from "./assert";
import { Blob } from "./blob.ts";

export let checksum = (
	input: string | Uint8Array | Blob | Artifact,
	algorithm: Checksum.Algorithm,
): Promise<Checksum> => {
	return Checksum.new(input, algorithm);
};

export type Checksum = string;

export declare namespace Checksum {
	let new_: (
		input: string | Uint8Array | Blob | Artifact,
		algorithm: Checksum.Algorithm,
	) => Promise<Checksum>;
	export { new_ as new };
}

export namespace Checksum {
	export type Algorithm = "blake3" | "sha256" | "sha512" | "unsafe";

	export let new_ = async (
		input: string | Uint8Array | Blob | Artifact,
		algorithm: Checksum.Algorithm,
	): Promise<Checksum> => {
		if (typeof input === "string" || input instanceof Uint8Array) {
			return syscall("checksum", input, algorithm);
		} else if (Blob.is(input)) {
			return await Blob.checksum(input, algorithm);
		} else if (Artifact.is(input)) {
			return await Artifact.checksum(input, algorithm);
		} else {
			return unreachable();
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
