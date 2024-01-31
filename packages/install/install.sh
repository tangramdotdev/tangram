#!/bin/bash

# Set the version.
version="0.0.0"

# Exit if any command fails.
set -e
# Exit if any command in a pipeline fails.
set -o pipefail
# Exit if any unset variables are read.
set -u

# Define some helper functions to produce colorized output.
reset="\033[0m"
green="\033[0;32m"
yellow="\033[0;33m"
red="\033[0;31m"
success() {
	echo -e "${reset}${green}$*${reset}"
}
warning() {
	echo -e "${reset}${yellow}Warning: ${reset} $*"
}
error() {
	echo -e "${reset}${red}Error: ${reset} $*"
	exit 1
}

# Define some paths.
tangram_path="${HOME}/.tangram"
tarball_path="${tangram_path}/tangram.tar.gz"

# Define the shell configuration paths.
bash_profile_path="${HOME}/.bash_profile"
bashrc_path="${HOME}/.bashrc"
zshenv_path="${HOME}/.zshenv"
zshrc_path="${HOME}/.zshrc"

# Detect the host.
case "$(uname -m) $(uname -s)" in
	"x86_64 Linux")
		host="amd64_linux"
		;;
	"x86_64 Darwin")
		host="amd64_macos"
		;;
	"aarch64 Linux" | "arm64 Linux")
		host="arm64_linux"
		;;
	"aarch64 Darwin" | "arm64 Darwin")
		host="arm64_macos"
		;;
	*)
		error "Tangram does not support your combination of architecture and operating system: ""$(uname -m)" "$(uname -s)""."
		;;
esac

# Detect the user's shell.
case "$(basename "${SHELL}")" in
	"bash")
		shell="bash"
		;;
	"zsh")
		shell="zsh"
		;;
	*)
		shell=""
		;;
esac

# Define the URL.
url=https://github.com/tangramdotdev/tangram/releases/download/v${version}/tangram_${host}.tar.gz

# Create the Tangram path if it does not exist.
if [ ! -d "${tangram_path}" ]; then
	mkdir -p "${tangram_path}"
fi

# Download the tarball to the tarball path.
curl --fail --progress-bar --output "${tarball_path}" "${url}"

# Untar the tarball into the Tangram path.
tar -C "${tangram_path}" -xf "${tarball_path}"

# Remove the tarball.
rm "${tarball_path}"

# Install the shell integration.
case "${shell}" in
	"bash")
		# bash_profile
		if [ -z "${TANGRAM_BASH_PROFILE_INSTALLED:-}" ]; then
			cat <<- EOF >> "${bash_profile_path}"

				# tangram
				[ -f \${HOME}/.tangram/shell/bash/bash_profile ] && source \${HOME}/.tangram/shell/bash/bash_profile
			EOF
			success "A command was written to \"${bash_profile_path}\"."
		fi

		# bashrc
		if [ -z "${TANGRAM_BASHRC_INSTALLED:-}" ]; then
			cat <<- EOF >> "${bashrc_path}"

				# tangram
				[ -f \${HOME}/.tangram/shell/bash/bashrc ] && source \${HOME}/.tangram/shell/bash/bashrc
			EOF
			success "A command was written to \"${bashrc_path}\"."
		fi
		;;

	"zsh")
		# zshenv
		if [ -z "${TANGRAM_ZSHENV_INSTALLED:-}" ]; then
			cat >> "${zshenv_path}" <<- EOF

				# tangram
				[ -f \${HOME}/.tangram/shell/zsh/zshenv ] && source \${HOME}/.tangram/shell/zsh/zshenv
			EOF
			success "A command was written to \"${zshenv_path}\"."
		fi

		# zshrc
		if [ -z "${TANGRAM_ZSHRC_INSTALLED:-}" ]; then
			cat <<- EOF >> "${zshrc_path}"

				# tangram
				[ -f \${HOME}/.tangram/shell/zsh/zshrc ] && source \${HOME}/.tangram/shell/zsh/zshrc
			EOF
			success "A command was written to \"${zshrc_path}\"."
		fi
		;;

	*)
		warning "The Tangram shell integration was not installed. You will need to add \"${tangram_path}\" to \$PATH manually, and features like autoenvs that require shell integration will not work as expected."
		;;
esac

# Print a success message and some help to get started.
success "Tangram was successfully installed at \"${tangram_path}\"."
case ${shell} in
	"bash" | "zsh")
		echo "To get started, reload your shell and run \`tg --help\`."
		;;
	*)
		echo "To get started, add \"${tangram_path}/bin\" to \$PATH and run \`tg --help\`."
		;;
esac
