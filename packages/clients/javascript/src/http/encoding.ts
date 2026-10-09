export function requireJson(contentType: string | undefined) {
	let type = contentType?.split(";", 1)[0]?.trim().toLowerCase();
	if (type?.startsWith("application/vnd.tangram.") && !type.endsWith("+json")) {
		throw new Error(
			"the client does not support Tangram body prefix serialization",
		);
	}
}
