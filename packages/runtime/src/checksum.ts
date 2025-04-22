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
	export type Algorithm = "none" | "unsafe" | "blake3" | "sha256" | "sha512";

	export let new_ = async (
		input: string | Uint8Array | tg.Blob | tg.Artifact,
		algorithm: Checksum.Algorithm,
	): Promise<Checksum> => {
		if (typeof input === "string" || input instanceof Uint8Array) {
			return syscall("checksum", input, algorithm);
		} else if (tg.Blob.is(input)) {
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
		if (checksum === "none") {
			return "none";
		} else if (checksum === "unsafe") {
			return "unsafe";
		} else if (checksum.includes(":")) {
			return checksum.split(":")[0]! as Algorithm;
		} else if (checksum.includes("-")) {
			return checksum.split("-")[0]! as Algorithm;
		} else {
			throw new Error("invalid checksum");
		}
	};
}
