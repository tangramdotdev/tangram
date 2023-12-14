export let assert: (
	condition: unknown,
	message?: string,
) => asserts condition = (condition, message) => {
	if (!condition) {
		throw new Error(message ?? "Failed assertion.");
	}
};

export let unimplemented = (message?: string): never => {
	throw new Error(message ?? "Reached unimplemented code.");
};

export let unreachable = (message?: string): never => {
	throw new Error(message ?? "Reached unreachable code.");
};

export let todo = (): never => {
	throw new Error("Reached todo.");
};
