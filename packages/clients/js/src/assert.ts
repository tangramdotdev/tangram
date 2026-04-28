/** Assert that a condition is truthy. If not, throw an error with an optional message. */
export let assert: (
	condition: unknown,
	message?: string,
) => asserts condition = (condition, message) => {
	if (!condition) {
		throw new Error(message ?? "failed assertion");
	}
};

/** Throw an error indicating that unimplemented code has been reached. */
export let unimplemented = (message?: string): never => {
	throw new Error(message ?? "reached unimplemented code");
};

/** Throw an error indicating that unreachable code has been reached. */
export let unreachable = (message?: string): never => {
	throw new Error(message ?? "reached unreachable code");
};

export let todo = (): never => {
	throw new Error("reached todo");
};
