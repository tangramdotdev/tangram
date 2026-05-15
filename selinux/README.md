# Tangram SELinux policy

On SELinux-enforcing distributions, install this module so pasta can attach to Tangram's sandbox netns:

```sh
sudo semodule -i selinux/tangram.pp
```

Uninstall:

```sh
sudo semodule -r tangram
```

Rebuild `tangram.pp` from `tangram.te` (requires `checkpolicy` and `policycoreutils-devel`):

```sh
checkmodule -M -m -o tangram.mod tangram.te && semodule_package -o tangram.pp -m tangram.mod && rm tangram.mod
```
