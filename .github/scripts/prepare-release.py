"""Prepare a local release commit; the workflow pushes only after all checks pass."""

import os
from pathlib import Path
import re
import subprocess
import tomllib


VERSION = re.compile(r"v(?P<major>0|[1-9][0-9]*)\.(?P<minor>0|[1-9][0-9]*)\.(?P<patch>0|[1-9][0-9]*)")


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def package():
    return tomllib.loads(Path("Cargo.toml").read_text())["package"]


def next_tag(manifest):
    prefix = f"v{manifest['version'].rsplit('.', 1)[0]}."
    patches = [
        int(match["patch"])
        for tag in git("tag", "--list", f"{prefix}*").splitlines()
        if (match := VERSION.fullmatch(tag)) and tag.startswith(prefix)
    ]
    return f"{prefix}{max(patches, default=0) + 1}"


def main():
    branch = os.environ["DEFAULT_BRANCH"]
    retry = os.environ.get("RETRY_TAG", "")
    git("fetch", "origin", branch, "--tags")
    if retry:
        if not VERSION.fullmatch(retry):
            raise SystemExit("retry_tag must be a semantic tag such as v0.1.1")
        exists = subprocess.run(["git", "show-ref", "--verify", "--quiet", f"refs/tags/{retry}"])
        if exists.returncode:
            raise SystemExit("Tag does not exist. Leave retry_tag empty to create a new release.")
        git("merge-base", "--is-ancestor", retry, f"origin/{branch}")
        git("checkout", "--detach", retry)
        if package()["version"] != retry[1:]:
            raise SystemExit("Tag and Cargo version do not match")
        tag = retry
    else:
        if git("rev-parse", "HEAD") != git("rev-parse", f"origin/{branch}"):
            raise SystemExit("Default branch has advanced; start a new Publish run")
        tag = next_tag(package())

    manifest = package()
    if not (manifest.get("license") or manifest.get("license-file")):
        raise SystemExit("Set the project license or license-file before publishing")
    if not manifest.get("description"):
        raise SystemExit("Set package.description before publishing")

    if not retry:
        path = Path("Cargo.toml")
        content, count = re.subn(
            r'(?m)^version = "[^"]+"$', f'version = "{tag[1:]}"', path.read_text(), count=1
        )
        if count != 1:
            raise SystemExit("Could not update package version")
        path.write_text(content)
        subprocess.run(["cargo", "check", "--all-features"], check=True)
        git("config", "user.name", "github-actions[bot]")
        git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
        git("add", "Cargo.toml", "Cargo.lock")
        git("commit", "-m", f"chore: release {tag}")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"tag={tag}\n")
    print(f"Prepared {tag}")


if __name__ == "__main__":
    main()
