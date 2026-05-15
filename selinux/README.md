# Tangram SELinux policy

On SELinux-enforcing distributions, install this module so pasta can attach to Tangram's sandbox netns:

```sh
sudo semodule -i selinux/tangram.pp
```

Uninstall:

```sh
sudo semodule -r tangram
```
