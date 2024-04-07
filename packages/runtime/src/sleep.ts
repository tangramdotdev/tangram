export let sleep = async (duration: number) => {
	return await syscall("sleep", duration);
};
