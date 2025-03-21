use tangram_sandbox as sandbox;

#[tokio::main]
async fn main() {
	let mut command = sandbox::Command::new("/bin/dash");
	command
		.network(false)
		.cwd("/")
		.args(std::env::args_os().skip(1))
		.envs(std::env::vars_os())
		.mount(sandbox::BindMount {
			source: "/".into(),
			target: "/".into(),
			readonly: false,
		});
	#[cfg(target_os = "linux")]
	{
		#[cfg(target_os = "linux")]
		tokio::fs::create_dir_all("/tmp/sandbox").await.ok();
		command.chroot("/tmp/sandbox");
	}
	command.spawn().await.unwrap().wait().await.unwrap();
}
