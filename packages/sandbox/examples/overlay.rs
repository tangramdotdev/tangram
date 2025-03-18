use tangram_sandbox as sandbox;
#[tokio::main]
async fn main() {
	tokio::fs::create_dir_all("/tmp/overlay/lower").await.ok();
	tokio::fs::create_dir_all("/tmp/overlay/upper").await.ok();
	tokio::fs::create_dir_all("/tmp/overlay/merged").await.ok();
	tokio::fs::create_dir_all("/tmp/overlay/root").await.ok();
	tokio::fs::create_dir_all("/tmp/overlay/work").await.ok();

	let status = sandbox::Command::new("/bin/dash")
		.network(false)
		.chroot("/tmp/overlay/root")
		.args(std::env::args_os().skip(1))
		.envs(std::env::vars_os())
		.mount(sandbox::BindMount {
			source: "/".into(),
			target: "/".into(),
			readonly: false,
		})
		.cwd("/")
		.mount(sandbox::Overlay {
			lowerdirs: vec!["/tmp/overlay/lower".into()],
			upperdir: "/tmp/overlay/upper".into(),
			workdir: "/tmp/overlay/work".into(),
			merged: "/merged".into(),
			readonly: false,
		})
		.spawn()
		.await
		.unwrap()
		.wait()
		.await
		.unwrap();
	eprintln!("{status:?}");
}

