#!/usr/bin/env bash
# Build + test the plugin in a JDK 21 Gradle container; the zip lands in dist/.
set -euo pipefail
cd "$(dirname "$0")"
[ -d plugin/es-libs ] || scripts/extract-es-jars.sh
rm -rf plugin/build/distributions
docker run --rm -u "$(id -u):$(id -g)" -e GRADLE_USER_HOME=/cache \
  -v "$PWD":/work -v s2s-gradle-cache:/cache -w /work/plugin \
  gradle:8-jdk21 gradle --no-daemon -q "${@:-test}" bundlePlugin
mkdir -p dist && rm -f dist/*.zip
cp plugin/build/distributions/*.zip dist/
ls -l dist/*.zip
