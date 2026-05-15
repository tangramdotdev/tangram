# Tangram SELinux policy

On SELinux-enforcing Linux distributions (Fedora, RHEL, CentOS Stream, Rocky, Alma), Tangram's network sandbox uses [pasta](https://passt.top/) to set up an isolated user-mode network. The stock `selinux-policy-targeted` package does not grant pasta the access it needs to attach to Tangram's sandbox helper, because the helper does not run as a "container" — it is the runtime that mediates between the host filesystem and the sandbox payload. This directory ships a minimal SELinux policy module that grants pasta the one missing permission.

Without this module loaded, tests like `sandbox_ports.nu`, `builtin_download_checksum.nu`, and `host_command_hello_world.nu` fail with `pasta wrote an invalid pid file`, preceded by `pasta.avx2: netns dir open: Permission denied, exiting` in the server log.

## Installation

```sh
sudo semodule -i selinux/tangram.pp
```

Verify it loaded:

```sh
sudo semodule -l | grep tangram
# tangram
```

## Uninstallation

```sh
sudo semodule -r tangram
```

## What it grants

```
allow pasta_t unconfined_t:dir open;
```

That is, pasta is permitted to open `/proc/<pid>/ns` directories belonging to processes in the `unconfined_t` domain — which is the domain Tangram's sandbox helper inherits when launched by an interactive user. This is the only rule in the module.

## Security note

This rule is broader than ideal: it lets pasta inspect *any* `unconfined_t` process's namespaces, not only Tangram's. In practice pasta is invoked solely by container runtimes, so the broader access is not exploitable on its own, but a tighter version would require defining a Tangram-specific SELinux type and re-labeling Tangram's binary at install time — a tradeoff against installer complexity. If your site needs the tighter version, see "Custom domain" below.

## Rebuilding the module

```sh
cd selinux
checkmodule -M -m -o tangram.mod tangram.te
semodule_package -o tangram.pp -m tangram.mod
rm tangram.mod
```

Both `checkmodule` and `semodule_package` are provided by the `policycoreutils-devel` and `checkpolicy` packages on Fedora-family distributions.

## Custom domain

A site running custom SELinux policy may prefer to define a dedicated `tangram_sandbox_t` domain rather than widen `unconfined_t`. A minimal sketch:

```te
policy_module(tangram_sandbox, 1.0)

require {
    type pasta_t;
    type unconfined_t;
}

type tangram_sandbox_t;
type tangram_sandbox_exec_t;
domain_type(tangram_sandbox_t)
unconfined_domain(tangram_sandbox_t)

allow pasta_t tangram_sandbox_t:dir { open read getattr search };
allow pasta_t tangram_sandbox_t:file { open read getattr };
allow unconfined_t tangram_sandbox_t:process transition;
type_transition unconfined_t tangram_sandbox_exec_t:process tangram_sandbox_t;
```

Then label the tangram binary as `tangram_sandbox_exec_t` via `chcon` or `semanage fcontext`. This restricts pasta's access to Tangram processes only, at the cost of an additional file-label step.

## Why not transition to `container_t`?

The container-selinux package already grants `pasta_t` access to `container_t`, so the obvious approach is to have Tangram's helper transition to `container_t` before exec. This does not work because `container_t` is designed for *contained workloads* and forbids access to `user_home_t`, `user_tmp_t`, and other host filesystem labels — but Tangram's helper is the container runtime, not the workload, and must read/write host paths to mediate I/O between the user's filesystem and the sandbox payload.
