#!/usr/bin/env bash
#
# Tests scripts/check-workflows.sh. The Test workflow runs it after the
# check; run it by hand with `bash scripts/test_check_workflows.sh` on a
# checkout with LF line endings. It needs jq, which GitHub's runners have.
#
# The check runs on copies of .github/workflows: once as they are, where it
# must pass, and once for each change below, where it must fail with that
# change's problem alone. Each change would let a release skip something it
# relies on. Then the filter release.yml's checks job picks the tagged
# commit's Test run with runs on sample lists of runs.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
DIR=.github/workflows
BUILD="${DIR}/build.yml"
RELEASE="${DIR}/release.yml"
TEST="${DIR}/test.yml"
PASSED=0
FAILED=0
STALE=""

fail() {
    echo "::error::$*"
    FAILED=$((FAILED + 1))
}

if ! command -v jq > /dev/null; then
    echo "::error::jq is needed to run release.yml's filter" >&2
    exit 1
fi

WORK=$(mktemp -d)
trap 'rm -rf "${WORK}"' EXIT
COPY="${WORK}/copy"

# A fresh copy of the check and the workflows, for the next case.
fresh() {
    STALE=""
    rm -rf "${COPY}"
    mkdir -p "${COPY}/scripts" "${COPY}/${DIR}"
    cp scripts/check-workflows.sh "${COPY}/scripts/"
    cp -R "${DIR}/." "${COPY}/${DIR}/"
}

# Replaces the one place OLD appears in a file of the copy with NEW. If it
# isn't there exactly once, the case is out of date: it fails, unrun.
edit() {
    local file="${COPY}/$1" old=$2 new=$3 text rest
    text=$(cat "${file}"; printf x)
    text=${text%x}
    rest=${text#*"${old}"}
    if [[ "${rest}" == "${text}" || "${rest}" == *"${old}"* ]]; then
        STALE="$1 doesn't hold exactly one '${old}'"
        return
    fi
    printf '%s' "${text%%"${old}"*}${new}${rest}" > "${file}"
}

# Runs the check on the copy. It must pass when WANT is "pass", and
# otherwise fail with one problem, whose message contains WANT.
expect() {
    local name=$1 want=$2 out rc=0
    if [[ -n "${STALE}" ]]; then
        fail "${name}: this case is out of date: ${STALE}"
        return
    fi
    out=$(bash "${COPY}/scripts/check-workflows.sh" 2>&1) || rc=$?
    if [[ "${want}" == pass ]]; then
        if ((rc != 0)); then
            fail "${name}: the check failed: ${out}"
            return
        fi
    elif ((rc == 0)); then
        fail "${name}: the check passed; it must fail with: ${want}"
        return
    elif [[ $(grep -c '^::error::' <<<"${out}") -ne 1 ]] || ! grep -qF -- "${want}" <<<"${out}"; then
        fail "${name}: the check must fail with this problem alone: ${want}; it said: ${out}"
        return
    fi
    PASSED=$((PASSED + 1))
}

fresh
expect "the workflows as they are" pass

# The workflows and their actions.
fresh
printf 'name: CI\non: push\n' > "${COPY}/${DIR}/ci.yml"
expect "a fourth workflow" "holds: build.yml ci.yml release.yml test.yml;"

fresh
edit "${TEST}" 'Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6 # v2.9.2' 'Swatinem/rust-cache@v2'
expect "an action used by its tag" "'Swatinem/rust-cache@v2' is not pinned to a full commit SHA"

fresh
edit "${BUILD}" 'docker/login-action@c94ce9fb468520275223c153574b00df6fe4bcc9' 'docker/login-action@c94ce9fb'
expect "an action used by a short SHA" "'docker/login-action@c94ce9fb' is not pinned to a full commit SHA"

# release.yml's trigger and permissions.
TAGS="    tags: ['v*\\+ds.*']"
fresh
edit "${RELEASE}" "${TAGS}" "${TAGS}"$'\n'"    branches: [main]"
expect "a release on pushes to main" "release.yml must run only for v*+ds.* tags"

fresh
edit "${RELEASE}" "${TAGS}" "    tags: ['v*']"
expect "a release for any v* tag" "release.yml must run only for v*+ds.* tags"

fresh
edit "${RELEASE}" "${TAGS}" "${TAGS}"$'\n'"  workflow_dispatch:"
expect "a release by hand" "release.yml must run only for v*+ds.* tags"

fresh
edit "${RELEASE}" $'\npermissions: {}\n' $'\npermissions: write-all\n'
expect "default permissions" "release.yml must set permissions: {} at the top"

# The build job's key check.
fresh
edit "${RELEASE}" 'if ! cmp -s "$RUNNER_TEMP/embedded.pub" sign/release-signing-key.pub; then' 'if false; then'
expect "no key check" "no longer compares the key compiled into the node"

# The checks job.
fresh
edit "${RELEASE}" 'actions/workflows/test.yml/runs' 'actions/workflows/build.yml/runs'
expect "no Test run looked for" "missing: actions/workflows/test.yml/runs"

fresh
edit "${RELEASE}" $'if [[ "$conclusion" != "success" ]]; then\n            echo "::error::Test run' \
    $'if false; then\n            echo "::error::Test run'
expect "a failed Test run accepted" "must fail unless both runs succeeded"

# The Test run the checks job takes, and the names of runs by hand.
fresh
edit "${RELEASE}" '(.event == "workflow_dispatch" and .display_title == "Test by hand")' '.event == "workflow_dispatch"'
expect "any run by hand taken" "must take the Test run from the push to main, or one by hand without the listings cross-test"

fresh
edit "${RELEASE}" '(.event == "push" and .head_branch == "main")' '.event == "push"'
expect "a push to any branch taken" "must take the Test run from the push to main, or one by hand without the listings cross-test"

fresh
edit "${TEST}" "$(grep '^run-name:' "${TEST}")"$'\n' ''
expect "runs by hand not named" "test.yml must name a run by hand for whether it runs the listings cross-test"

fresh
edit "${TEST}" "'Test by hand, with the listings cross-test' || 'Test by hand'" \
    "'Test by hand' || 'Test by hand, with the listings cross-test'"
expect "the two names swapped" "test.yml must name a run by hand for whether it runs the listings cross-test"

CROSS_IF="    if: \${{ github.event_name == 'schedule' || inputs.listings_cross_test }}"
fresh
edit "${TEST}" "${CROSS_IF}" "    if: \${{ github.event_name != 'push' }}"
expect "the cross-test in every run by hand" "listings-cross-test job must run only weekly or when a run by hand asks for it"

fresh
edit "${TEST}" "${CROSS_IF}"$'\n' ''
expect "the cross-test in every run" "listings-cross-test job must run only weekly or when a run by hand asks for it"

# The Test run a release waits for runs every test.
fresh
edit "${TEST}" $'      - name: Vendored ant-protocol\n        if: ${{ !cancelled() }}\n        run: cargo test --locked --manifest-path vendor/ant-protocol/Cargo.toml\n' ''
expect "the vendored ant-protocol's tests left out" \
    "test job no longer runs every test (missing: run: cargo test --locked --manifest-path vendor/ant-protocol/Cargo.toml)"

fresh
edit "${TEST}" '        run: python3 scripts/test_mixed_versions_verdicts.py' '        run: "true"'
expect "the harness's verdict tests left out" \
    "test job no longer runs every test (missing: run: python3 scripts/test_mixed_versions_verdicts.py)"

fresh
edit "${TEST}" '        run: cargo test --locked -p ant-listings' '        run: "true"'
expect "the vendored ant-listings' tests left out" \
    "test job no longer runs every test (missing: run: cargo test --locked -p ant-listings)"

fresh
edit "${TEST}" 'for target in tests/*.rs tests/*/mod.rs; do' 'for target in tests/fork_pins.rs; do'
expect "the integration tests listed by hand" \
    "test job no longer runs every test (missing: for target in tests/*.rs tests/*/mod.rs; do)"

fresh
edit "${TEST}" 'run: cargo test --locked --features test-utils --lib --bins' 'run: cargo test --locked --lib'
expect "the library tested without test-utils, and its binaries left out" \
    "test job no longer runs every test (missing: run: cargo test --locked --features test-utils --lib --bins)"

# Publishing and :latest wait for the checks.
fresh
edit "${RELEASE}" $'    environment: release\n' ''
expect "publish outside the release environment" "publish job must run in the release environment"

fresh
edit "${RELEASE}" '    needs: [build, checks]' '    needs: build'
expect "publish without the checks" "publish job must need checks"

fresh
edit "${RELEASE}" '    needs: [build, checks, publish]' '    needs: [build, checks]'
expect "latest before the publish" "latest job must need publish"

fresh
edit "${RELEASE}" '    needs: [build, checks, publish]' '    needs: [build, publish]'
expect "latest without the checks" "latest job must need checks"

# No condition, and nothing continuing on error.
fresh
edit "${RELEASE}" $'    needs: [build, checks]\n' $'    needs: [build, checks]\n    if: ${{ always() }}\n'
expect "publish after a failed need" "release.yml must have no if:"

fresh
edit "${RELEASE}" $'    needs: [build, checks, publish]\n' $'    needs: [build, checks, publish]\n    if: ${{ !cancelled() }}\n'
expect "latest after a failed need" "release.yml must have no if:"

WAIT_TEST="      - name: Wait for the Test workflow's run of this commit"
fresh
edit "${RELEASE}" "${WAIT_TEST}"$'\n' "${WAIT_TEST}"$'\n        if: false\n'
expect "the Test run's check skipped" "release.yml must have no if:"

fresh
edit "${RELEASE}" $'      - name: Check tag, version and release key\n' \
    $'      - name: Check tag, version and release key\n        if: false\n'
expect "the tag and key check skipped" "release.yml must have no if:"

fresh
edit "${RELEASE}" '      - uses: actions/download-artifact@' \
    $'      - if: always()\n        run: true\n\n      - uses: actions/download-artifact@'
expect "a step of publish after a failure" "release.yml must have no if:"

fresh
edit "${RELEASE}" $'  checks:\n    runs-on: ubuntu-24.04\n' $'  checks:\n    runs-on: ubuntu-24.04\n    continue-on-error: true\n'
expect "the checks job continuing on error" "release.yml must not continue on error"

fresh
edit "${RELEASE}" "${WAIT_TEST}"$'\n' "${WAIT_TEST}"$'\n        continue-on-error: true\n'
expect "the Test run's check continuing on error" "release.yml must not continue on error"

fresh
edit "${RELEASE}" $'  checks:\n    runs-on: ubuntu-24.04\n' \
    $'  checks:\n    runs-on: ubuntu-24.04\n    # No if: or continue-on-error: true here.\n'
expect "a comment naming both" pass

# Only release.yml's latest job writes :latest.
fresh
edit "${BUILD}" $'        run: docker push "$IMAGE:${{ github.sha }}"\n' \
    $'        run: docker push "$IMAGE:${{ github.sha }}" && docker push "$IMAGE:latest"\n'
expect ":latest pushed by Build" "only release.yml's latest job may write the :latest tag"

fresh
edit "${RELEASE}" $'      - name: Install ant-keygen 0.1.0\n' \
    $'      - name: Move latest\n        run: docker buildx imagetools create --tag "$IMAGE:latest" x\n\n      - name: Install ant-keygen 0.1.0\n'
expect ":latest moved by publish" "only release.yml's latest job may write the :latest tag"

# The image's node manager: built with --locked from the pinned commit,
# checked, and taken by the image with its marker.
fresh
edit "${BUILD}" 'run: cargo build --release --locked -p ant-cli --bin darwin-nodes' \
    'run: cargo build --release -p ant-cli --bin darwin-nodes'
expect "the manager built without its lockfile" "build.yml's manager job must build darwin-nodes with --locked"

fresh
edit "${BUILD}" '{ read -r repository; read -r revision; } < docker/manager-revision' \
    'repository=https://github.com/Project-DSpace/darwin-client.git revision=main'
expect "the manager built from a branch" "build.yml's manager job must build darwin-nodes with --locked"

fresh
edit "${BUILD}" 'for text in WithAutonomi logs.autonomi.com /api/v1/reset /api/v1/logs/forward; do' \
    'for text in /api/v1/reset; do'
expect "the manager's binary checked for less" "build.yml's manager job must build darwin-nodes with --locked"

fresh
edit "${BUILD}" 'if [[ ! "$commit" =~ ^[0-9a-f]{7,40}$ || "$REVISION" != "$commit"* ]]; then' \
    'if [[ "$version" != *"commit ${REVISION:0:7}" ]]; then'
expect "the manager's commit matched on exactly 7 characters" \
    "build.yml's manager job must build darwin-nodes with --locked"

fresh
edit "${BUILD}" 'commit=${version##*commit }' 'commit=${REVISION:0:7}'
expect "the manager's commit taken from the pin, not the binary" \
    "build.yml's manager job must build darwin-nodes with --locked"

fresh
edit "${BUILD}" 'if ! git -C darwin-client merge-base --is-ancestor "$revision" refs/remotes/origin/main; then' \
    'if false; then'
expect "the manager built from a commit not checked to be on main" \
    "build.yml's manager job must check that the commit docker/manager-revision pins is on darwin-client's main"

fresh
edit "${BUILD}" 'git -C darwin-client fetch -q --filter=blob:none origin +refs/heads/main:refs/remotes/origin/main' \
    'git -C darwin-client fetch -q --depth 1 origin "$revision"'
expect "the pin checked without main's history" \
    "build.yml's manager job must check that the commit docker/manager-revision pins is on darwin-client's main"

fresh
edit "${BUILD}" '    needs: [linux-x64, manager]' '    needs: linux-x64'
expect "the image without the manager's build" "build.yml's image job must need the manager job"

fresh
edit "${BUILD}" $'          build-args: IMAGE_VERSION=${{ github.sha }}\n' ''
expect "the image without its marker" "build.yml's image job must pass the image marker"

# The node, for the image and for releases: built with no feature, and
# checked for test seams after it is built and before it is packaged.
SEAM_STEP=$'      - name: Check the test seams are left out\n        run: |\n          if [[ ! -s target/release/ant-node ]]; then\n            echo "::error::there is no target/release/ant-node to check"\n            exit 1\n          fi\n          if grep -aqF ANT_TEST_SEAM_ target/release/ant-node; then\n            echo "::error::target/release/ant-node holds a test seam (ANT_TEST_SEAM_*): it was built with test-utils"\n            exit 1\n          fi\n\n'
SEAMS_UNCHECKED="must check the node it built for test seams once, after the build and before the Package step"
fresh
edit "${RELEASE}" "${SEAM_STEP}" ''
expect "the release's node not checked for test seams" "release.yml's build job ${SEAMS_UNCHECKED}"

fresh
edit "${BUILD}" '          if grep -aqF ANT_TEST_SEAM_ target/release/ant-node; then' '          if false; then'
expect "the image's node not checked for test seams" "build.yml's linux-x64 job ${SEAMS_UNCHECKED}"

fresh
edit "${RELEASE}" '          if [[ ! -s target/release/ant-node ]]; then' '          if false; then'
expect "the release's seam check passing with no binary" "release.yml's build job ${SEAMS_UNCHECKED}"

fresh
edit "${RELEASE}" "${SEAM_STEP}" ''
edit "${RELEASE}" '      - uses: actions/upload-artifact@' "${SEAM_STEP}"'      - uses: actions/upload-artifact@'
expect "the release's seam check after its packaging" "release.yml's build job ${SEAMS_UNCHECKED}"

fresh
edit "${RELEASE}" "${SEAM_STEP}" ''
edit "${RELEASE}" $'      - name: Build\n' "${SEAM_STEP}"$'      - name: Build\n'
expect "the release's seam check before its build" "release.yml's build job ${SEAMS_UNCHECKED}"

fresh
edit "${RELEASE}" 'run: cargo build --release --locked --bin ant-node' \
    'run: cargo build --release --locked --features test-utils --bin ant-node'
expect "the release's node built with test-utils" \
    "release.yml's build job must build the node once, with exactly 'run: cargo build --release --locked --bin ant-node'"

fresh
edit "${BUILD}" 'run: cargo build --release --locked --bin ant-node' \
    'run: cargo build --release --locked --bin ant-node --features test-utils'
expect "the image's node built with test-utils" \
    "build.yml's linux-x64 job must build the node once, with exactly 'run: cargo build --release --locked --bin ant-node'"

# The filter release.yml's checks job gives gh api --jq to pick the tagged
# commit's Test run from the runs of test.yml.
filter=$(awk -v q="'" '
    index($0, "actions/workflows/test.yml/runs") { found = 1 }
    found && index($0, "--jq " q) {
        rest = substr($0, index($0, "--jq " q) + 6)
        if (index(rest, q)) { print substr(rest, 1, index(rest, q) - 1) }
        exit
    }
' "${RELEASE}")

# A run as the API lists it: run number, event, branch and name. Its id is
# the run number plus 39000000000.
run() {
    printf '{"id": %s, "run_number": %s, "event": "%s", "head_branch": "%s", "display_title": "%s"}\n' \
        "$((39000000000 + $1))" "$1" "$2" "$3" "$4"
}

# The filter must take the run numbered WANT ("" for none) from RUNS.
takes() {
    local name=$1 want=$2 runs=$3 got
    if [[ -z "${filter}" ]]; then
        fail "release.yml's filter, ${name}: this case is out of date: no --jq filter follows actions/workflows/test.yml/runs in ${RELEASE}"
        return
    fi
    if [[ -n "${want}" ]]; then
        want=$((39000000000 + want))
    fi
    if ! got=$(jq -s '{workflow_runs: .}' <<<"${runs}" | jq -r "${filter}"); then
        fail "release.yml's filter, ${name}: jq failed"
        return
    fi
    if [[ "${got}" != "${want}" ]]; then
        fail "release.yml's filter, ${name}: took '${got}', not '${want}'"
        return
    fi
    PASSED=$((PASSED + 1))
}

push=$(run 14 push main "Version 0.23.0+ds.13")
hand=$(run 15 workflow_dispatch v0.23.0+ds.13 "Test by hand")
cross=$(run 16 workflow_dispatch main "Test by hand, with the listings cross-test")
weekly=$(run 17 schedule main "Test")
unnamed=$(run 18 workflow_dispatch main "Test")
elsewhere=$(run 19 push ds13 "Version 0.23.0+ds.13")

takes "a later run by hand with the cross-test leaves the push run" 14 "${push}"$'\n'"${cross}"
takes "a later run by hand without it is taken" 15 "${push}"$'\n'"${hand}"
takes "a later cross-test run leaves a run by hand without it" 15 "${push}"$'\n'"${hand}"$'\n'"${cross}"
takes "the order runs are listed in doesn't matter" 15 "${cross}"$'\n'"${hand}"$'\n'"${push}"
takes "a weekly run, which has the cross-test, is not taken" 14 "${push}"$'\n'"${weekly}"
takes "only a cross-test run" "" "${cross}"
takes "a run by hand named before runs were named" "" "${unnamed}"
takes "a push to another branch" "" "${elsewhere}"
takes "no run" "" ""

if ((FAILED > 0)); then
    echo "${FAILED} of $((PASSED + FAILED)) cases failed" >&2
    exit 1
fi
echo "scripts/check-workflows.sh: ${PASSED} cases pass. It passes on the workflows as they are and fails on each change that would let a release skip a check, and release.yml's filter takes the tagged commit's own Test run."
