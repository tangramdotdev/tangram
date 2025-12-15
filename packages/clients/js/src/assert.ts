export let assert: (
	condition: unknown,
	message?: string,
) => asserts condition = (condition, message) => {
	if (!condition) {
		throw new Error(message ?? "failed assertion");
	}
};

export let unimplemented = (message?: string): never => {
	throw new Error(message ?? "reached unimplemented code");
};

export let unreachable = (message?: string): never => {
	throw new Error(message ?? "reached unreachable code");
};

export let todo = (): never => {
	throw new Error("reached todo");
};
