#!/usr/bin/env bash
#
# Tests scripts/check-workflows.sh. The Test workflow runs it after the
# check; run it by hand with `bash scripts/test_check_workflows.sh` on a
# checkout with LF line endings. It needs jq and git, which GitHub's runners
# have.
#
# The check runs on copies of .github/workflows and docker/release-images:
# once as they are, where it must pass, and once for each change below,
# where it must fail with that change's problem alone. Each change would let
# a release, or a move of :latest, skip something it relies on, or carry on
# past a refusal. Then the filter release.yml's and latest.yml's checks jobs
# pick a commit's Test run with runs on sample lists of runs, the order the
# latest jobs put node versions in runs on sample versions, and the lines
# latest.yml's checks job reads the node's code with run on a sample
# repository.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
DIR=.github/workflows
BUILD="${DIR}/build.yml"
LATEST="${DIR}/latest.yml"
RELEASE="${DIR}/release.yml"
TEST="${DIR}/test.yml"
RELEASE_IMAGES=docker/release-images
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
if ! command -v git > /dev/null; then
    echo "::error::git is needed to run latest.yml's lines that read the node's code" >&2
    exit 1
fi

WORK=$(mktemp -d)
trap 'rm -rf "${WORK}"' EXIT
COPY="${WORK}/copy"

# A fresh copy of the check, the workflows and docker/release-images, for
# the next case.
fresh() {
    STALE=""
    rm -rf "${COPY}"
    mkdir -p "${COPY}/scripts" "${COPY}/${DIR}" "${COPY}/docker"
    cp scripts/check-workflows.sh "${COPY}/scripts/"
    cp -R "${DIR}/." "${COPY}/${DIR}/"
    cp "${RELEASE_IMAGES}" "${COPY}/docker/"
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

# The lines of the step named NAME in a file of the copy: from its
# "- name:" line up to the next step, comment, job or top-level key.
step_lines() {
    NAME="      - name: $2" awk '
        $0 == ENVIRON["NAME"] { inside = 1; print; next }
        inside && /^(      - |      #|  [^ ]|[^ ])/ { exit }
        inside { print }
    ' "${COPY}/$1"
}

# Takes the step named NAME out of a file of the copy. If there is no such
# step, the case is out of date: it fails, unrun.
drop_step() {
    local lines
    lines=$(step_lines "$1" "$2")
    if [[ -z "${lines}" ]]; then
        STALE="$1 has no step named '$2'"
        return
    fi
    edit "$1" "${lines}"$'\n' ''
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
expect "a fifth workflow" "holds: build.yml ci.yml latest.yml release.yml test.yml;"

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

# And moves :latest forward only, so a re-run, which needs no approval,
# can't undo an image release or a withdrawal.
GUARD_NAME="Check latest's node is older than this release's"
GUARD="release.yml's latest job must check, in a step named \"${GUARD_NAME}\", that :latest's node is older than the release's, or that :latest already has the image"
fresh
drop_step "${RELEASE}" "${GUARD_NAME}"
expect "latest moved by a release whatever it has" "${GUARD}"

fresh
edit "${RELEASE}" 'if [[ "$order" != "newer than" ]]; then' 'if false; then'
expect "latest moved back by a re-run of a release" "${GUARD}"

fresh
edit "${RELEASE}" 'if [[ "$current" == "$DIGEST" ]]; then' 'if true; then'
expect "any image taken for the release's own" "${GUARD}"

fresh
edit "${RELEASE}" 'current_version=$(docker run --rm --entrypoint /opt/ant/ant-node "$IMAGE@$current" --version)' \
    'current_version="ant-node 0.0.0"'
expect "latest's node version not read from its image by a release" "${GUARD}"

fresh
edit "${RELEASE}" $'| if $n > $c then "newer than" elif $n == $c then "the same as" else "older than" end\'); then' \
    $'| if $n >= $c then "newer than" elif $n == $c then "the same as" else "older than" end\'); then'
expect "a release ordering versions its own way" "release.yml's latest job must order node versions as latest.yml does"

fresh
GUARD_LINES=$(step_lines "${RELEASE}" "${GUARD_NAME}")
drop_step "${RELEASE}" "${GUARD_NAME}"
printf '\n%s\n' "${GUARD_LINES}" >> "${COPY}/${RELEASE}"
expect "latest read by a release only after it moved" "release.yml's latest job must check :latest (${GUARD_NAME}) before it moves it"

# A release's latest job writes to the registry once, to move :latest: any
# other write, such as one before its checks, fails.
EARLY_WRITES=('docker push "$IMAGE:latest"'
    'docker buildx imagetools create --tag "$IMAGE:latest" "$IMAGE@$DIGEST"'
    'crane tag "$IMAGE@$DIGEST" latest')
for write in "${EARLY_WRITES[@]}"; do
    fresh
    edit "${RELEASE}" $'      - name: Check this is the latest release\n' \
        $'      - name: Move latest early\n        run: '"${write}"$'\n\n      - name: Check this is the latest release\n'
    expect "latest written by a release before its checks (${write})" \
        "release.yml's latest job must move :latest once, with exactly"
done

fresh
edit "${RELEASE}" 'docker buildx imagetools create --prefer-index=false --tag "$IMAGE:latest" "$IMAGE@$DIGEST"' \
    'docker buildx imagetools create --prefer-index=false --tag "$IMAGE:latest" "$IMAGE:$TAG"'
expect "latest moved by a release to another image than the Build run's" \
    "release.yml's latest job must move :latest once, with exactly"

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

# Every refusal stops its step, and nothing ends a step before its checks.
fresh
edit "${RELEASE}" $'            echo "::error::$tag is not vX.Y.Z+ds.N"\n            exit 1\n' \
    $'            echo "::error::$tag is not vX.Y.Z+ds.N"\n'
expect "a refusal of release.yml that doesn't stop its step" "release.yml must follow each ::error:: with exit 1"

fresh
edit "${RELEASE}" $'      - name: Check this is the latest release\n        run: |\n' \
    $'      - name: Check this is the latest release\n        run: |\n          exit 0\n'
expect "a check of release.yml ended before it starts" \
    "release.yml must have no line that ends a step early or lets a failure pass"

# Only the latest jobs of release.yml and latest.yml, and build.yml's image
# job, write to the registry, the image job only to load the image it built,
# tagged with its commit, and push that tag; nothing else names :latest.
REGISTRY="only release.yml's and latest.yml's latest jobs, which move :latest, and build.yml's image job, which pushes the image as :<commit>, may write to the registry"
LATEST_NAMED="only the latest jobs of release.yml and latest.yml may name the image's :latest tag"
fresh
edit "${BUILD}" $'        run: docker push "$IMAGE:${{ github.sha }}"\n' \
    $'        run: docker push "$IMAGE:${{ github.sha }}" && docker push "$IMAGE:latest"\n'
expect ":latest pushed by Build" "${REGISTRY}"

fresh
edit "${BUILD}" $'      - name: Record the image digest\n' \
    $'      - name: Tag latest\n        run: crane tag "$IMAGE@$DIGEST" latest\n\n      - name: Record the image digest\n'
expect "latest tagged by Build's image job" "${REGISTRY}"

fresh
edit "${BUILD}" 'tags: ${{ env.IMAGE }}:${{ github.sha }}' 'tags: ${{ env.IMAGE }}:latest'
expect "the image built as latest by Build" "${REGISTRY}"

fresh
edit "${BUILD}" $'          load: true\n' $'          push: true\n'
expect "the image pushed by Build's build action" "${REGISTRY}"

fresh
edit "${BUILD}" $'      - name: Save image\n' \
    $'      - name: Push by hand\n        run: |\n          docker \\\n            push "$IMAGE:${{ github.sha }}-x"\n\n      - name: Save image\n'
expect "an image pushed by Build in a command split over lines" "${REGISTRY}"

fresh
edit "${RELEASE}" $'      - name: Install ant-keygen 0.1.0\n' \
    $'      - name: Move latest\n        run: docker buildx imagetools create --tag "$IMAGE:latest" x\n\n      - name: Install ant-keygen 0.1.0\n'
expect ":latest moved by publish" "${REGISTRY}"

fresh
edit "${LATEST}" $'          echo "version=$version" >> "$GITHUB_OUTPUT"\n' \
    $'          docker pull "$IMAGE:latest"\n          echo "version=$version" >> "$GITHUB_OUTPUT"\n'
expect ":latest named by latest.yml's checks job" "${LATEST_NAMED}"

# Only the job that publishes runs in the release environment, which holds
# the signing key and admits only v*+ds.* tags, only the job that moves
# :latest in the latest environment, which admits only main, and
# latest.yml reads no secret.
IN_ENVIRONMENT="only release.yml's publish job and latest.yml's latest job may run in an environment"
fresh
edit "${BUILD}" $'    needs: [linux-x64, manager]\n' $'    needs: [linux-x64, manager]\n    environment: release\n'
expect "the release environment in Build" "${IN_ENVIRONMENT}"

fresh
edit "${LATEST}" $'  checks:\n    runs-on: ubuntu-24.04\n' $'  checks:\n    runs-on: ubuntu-24.04\n    environment: release\n'
expect "latest.yml's checks job in the release environment" "${IN_ENVIRONMENT}"

fresh
edit "${LATEST}" $'      VERSION: ${{ needs.checks.outputs.version }}\n' \
    $'      VERSION: ${{ needs.checks.outputs.version }}\n      KEY: ${{ secrets.RELEASE_SIGNING_KEY_HEX }}\n'
expect "the release signing key in latest.yml" "latest.yml must read no secret"

# latest.yml: run only by hand, from main, and approved in the latest
# environment; it moves :latest, once its checks pass, to an image with the
# node :latest has, a published release's, from :latest's commit or a later
# one, with any change to the node's code since its release acknowledged,
# unless the run is a withdrawal, which goes back to an image :latest held
# before.
BY_HAND="latest.yml must run only by hand"
fresh
edit "${LATEST}" $'on:\n  workflow_dispatch:\n' $'on:\n  push:\n    branches: [main]\n  workflow_dispatch:\n'
expect "latest.yml on pushes to main" "${BY_HAND}"

fresh
edit "${LATEST}" $'on:\n  workflow_dispatch:\n' $'on:\n  workflow_run:\n    workflows: [Build]\n    types: [completed]\n  workflow_dispatch:\n'
expect "latest.yml after every Build run" "${BY_HAND}"

fresh
edit "${LATEST}" $'        type: boolean\n        default: false\n' $'        type: boolean\n        default: true\n'
expect "every run a withdrawal unless unset" "${BY_HAND}"

fresh
edit "${LATEST}" $'        type: string\n        default: ""\n' $'        type: string\n        default: reviewed\n'
expect "every run's node changes acknowledged unless unset" "${BY_HAND}"

# Each run is named for the digest it moves :latest to, which a withdrawal's
# check matches earlier runs by.
RUN_NAME="latest.yml must name each run for the digest it moves :latest to, then the commit"
fresh
edit "${LATEST}" "$(grep '^run-name:' "${LATEST}")"$'\n' ''
expect "Latest runs not named" "${RUN_NAME}"

fresh
edit "${LATEST}" '}}${{ inputs.digest }} from ${{ inputs.commit }}' '}}${{ inputs.commit }}'
expect "Latest runs named for their commit alone" "${RUN_NAME}"

fresh
edit "${LATEST}" $'\npermissions: {}\n' $'\npermissions: write-all\n'
expect "latest.yml's default permissions" "latest.yml must set permissions: {} at the top"

fresh
edit "${LATEST}" $'concurrency:\n  group: release\n' $'concurrency:\n  group: latest\n'
expect "latest.yml outside release.yml's concurrency group" "latest.yml must share release.yml's concurrency group"

fresh
edit "${LATEST}" $'      packages: read\n' $'      packages: write\n'
expect "the checks job able to write packages" "latest.yml's checks job must be able to write nothing"

fresh
edit "${LATEST}" 'if [[ "$GITHUB_REF" != refs/heads/main ]]; then' 'if false; then'
expect "a run from another ref" "latest.yml's checks job must refuse a run from any ref but main"

OWN="latest.yml's checks job must check out the commit it runs from"
fresh
edit "${LATEST}" $'      - name: Check the workflows\n        run: bash scripts/check-workflows.sh\n' ''
expect "latest.yml's own commit not checked" "${OWN}"

fresh
edit "${LATEST}" $'          fetch-depth: 0\n' $'          fetch-depth: 0\n          ref: ${{ inputs.commit }}\n'
expect "the commit given checked in place of the one latest.yml runs from" "${OWN}"

INPUTS="latest.yml's checks job must take the commit, the digest, the withdraw switch and node_code from the inputs, and refuse anything but a full commit, a sha256 digest and an empty or 64-digit node_code"
fresh
edit "${LATEST}" 'if [[ ! "$COMMIT" =~ ^[0-9a-f]{40}$ ]]; then' 'if [[ -z "$COMMIT" ]]; then'
expect "a commit by a short SHA or a branch" "${INPUTS}"

fresh
edit "${LATEST}" 'if [[ ! "$DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]; then' 'if [[ -z "$DIGEST" ]]; then'
expect "a digest of any form" "${INPUTS}"

fresh
edit "${LATEST}" $'      WITHDRAW: ${{ inputs.withdraw }}\n      NODE_CODE: ${{ inputs.node_code }}\n    steps:\n' \
    $'      NODE_CODE: ${{ inputs.node_code }}\n    steps:\n'
expect "a withdrawal's image unchecked, the switch never reaching the checks" "${INPUTS}"

fresh
edit "${LATEST}" $'      NODE_CODE: ${{ inputs.node_code }}\n    steps:\n' $'    steps:\n'
expect "node_code never reaching the checks" "${INPUTS}"

fresh
edit "${LATEST}" 'if [[ -n "$NODE_CODE" && ! "$NODE_CODE" =~ ^[0-9a-f]{64}$ ]]; then' 'if false; then'
expect "node_code of any form" "${INPUTS}"

# The latest job's environment: GitHub makes one a job names when there is
# none, with no protection, so the checks job refuses a run unless it asks
# for a reviewer's approval that administrators can't bypass, and admits
# only main.
APPROVAL="latest.yml's checks job must refuse a run unless the latest environment asks for a required reviewer's approval, which administrators can't bypass, and admits only main"
fresh
drop_step "${LATEST}" "Check the latest environment asks for an approval"
expect "the latest environment's approval never checked" "${APPROVAL}"

fresh
edit "${LATEST}" ', .can_admins_bypass == false' ''
expect "a latest environment administrators can bypass" "${APPROVAL}"

fresh
edit "${LATEST}" ' || "$branches" != "branch main"' ''
expect "a latest environment that admits any branch" "${APPROVAL}"

fresh
edit "${LATEST}" '| select(.type == "required_reviewers") | .reviewers[]?] | length > 0)' '] | length >= 0)'
expect "a latest environment with no reviewer" "${APPROVAL}"

fresh
edit "${LATEST}" 'if [[ "$status" != ahead && "$status" != identical ]]; then' 'if false; then'
expect "a commit not on main" "latest.yml's checks job must check that the commit is on main"

BUILD_RUN="latest.yml's checks job must check that the commit's Build run from its push to main succeeded, smoke test included"
fresh
edit "${LATEST}" '-f head_sha="$COMMIT" -f event=push -f branch=main -f per_page=1 \' '-f head_sha="$COMMIT" -f per_page=1 \'
expect "a Build run by hand, or on another branch" "${BUILD_RUN}"

fresh
edit "${LATEST}" $'if [[ "$conclusion" != success ]]; then\n            echo "::error::Build run' \
    $'if false; then\n            echo "::error::Build run'
expect "a failed Build run" "${BUILD_RUN}"

fresh
edit "${LATEST}" 'if [[ "$smoke" != success ]]; then' 'if false; then'
expect "the smoke test unchecked" "${BUILD_RUN}"

fresh
edit "${LATEST}" $'            echo "::error::the smoke test of Build run $run ended \'${smoke:-not run}\', not success"\n            exit 1\n' \
    $'            echo "::warning::the smoke test of Build run $run ended \'${smoke:-not run}\', not success"\n'
expect "a failed smoke test only warned of" "${BUILD_RUN}"

TEST_RUN="latest.yml's checks job must check that the commit's newest Test run, from its push to main or by hand without the listings cross-test, succeeded"
fresh
edit "${LATEST}" $'if [[ "$conclusion" != success ]]; then\n            echo "::error::Test run' \
    $'if false; then\n            echo "::error::Test run'
expect "a failed Test run" "${TEST_RUN}"

fresh
edit "${LATEST}" '-f head_sha="$COMMIT" -f per_page=100 \' '-f per_page=100 \'
expect "another commit's Test run" "${TEST_RUN}"

fresh
edit "${LATEST}" '(.event == "workflow_dispatch" and .display_title == "Test by hand")' '.event == "workflow_dispatch"'
expect "a Test run by hand with the listings cross-test" "${TEST_RUN}"

DIGEST_CHECKED="latest.yml's checks job must check that the digest is the image the commit's Build run from its push to main pushed, by its attestation or its record, or, for a withdrawal alone, by docker/release-images"
fresh
edit "${LATEST}" $'select(.buildTrigger == "push") | .runInvocationURI] | first // empty\' || true)\n          kept=' \
    $'.runInvocationURI] | first // empty\' || true)\n          kept='
expect "an image attested by a run by hand" "${DIGEST_CHECKED}"

fresh
edit "${LATEST}" '--source-ref refs/heads/main --source-digest "$COMMIT" --format json \' '--format json \'
expect "an image attested from another ref or commit" "${DIGEST_CHECKED}"

fresh
edit "${LATEST}" $'"oci://$IMAGE@$DIGEST" --repo "$GH_REPO" \\\n            --signer-workflow "$GH_REPO/.github/workflows/build.yml" \\\n' \
    $'"oci://$IMAGE@$DIGEST" --repo "$GH_REPO" \\\n'
expect "an image attested by another workflow" "${DIGEST_CHECKED}"

NO_RECORD='if [[ -z "$attested" && "$recorded" != "$DIGEST" && -z "$listed" ]]; then'
fresh
edit "${LATEST}" "${NO_RECORD}" 'if false; then'
expect "an image with no record" "${DIGEST_CHECKED}"

fresh
edit "${LATEST}" "${NO_RECORD}" \
    'if [[ -z "$attested" && "$recorded" != "$DIGEST" && -z "$listed" && "$tagged" != "$DIGEST" ]]; then'
expect "the commit's tag taken in place of an expired record" "${DIGEST_CHECKED}"

fresh
edit "${LATEST}" 'if [[ -n "$recorded" && "$recorded" != "$DIGEST" ]]; then' 'if false; then'
expect "a digest the Build run didn't record" "${DIGEST_CHECKED}"

fresh
edit "${LATEST}" 'if ((kept > 0)); then' 'if false; then'
expect "the Build run's record never read" "${DIGEST_CHECKED}"

# docker/release-images, the record that doesn't expire of the images
# releases put on :latest before the Build workflow attested its images, is
# taken for a withdrawal alone, by the line of a tag on the commit that
# names the digest.
fresh
edit "${LATEST}" $'          if [[ "$WITHDRAW" == true ]]; then\n            tag=$(awk' $'          if true; then\n            tag=$(awk'
expect "docker/release-images taken in a run that is no withdrawal" "${DIGEST_CHECKED}"

fresh
edit "${LATEST}" ' && "$(git rev-parse -q --verify "refs/tags/$tag^{commit}" || true)" == "$COMMIT" ]]; then' ' ]]; then'
expect "a line of docker/release-images taken for a tag not on the commit" "${DIGEST_CHECKED}"

fresh
edit "${LATEST}" '&& $2 == commit && $3 == digest { print $1 }' '&& $2 == commit { print $1 }'
expect "a line of docker/release-images taken for another digest of the commit" "${DIGEST_CHECKED}"

fresh
edit "${LATEST}" $'          else\n            record=attestation\n' $'          else\n            record=artifact\n'
expect "the attestation alone handed on as the Build run's record" "${DIGEST_CHECKED}"

# The list itself: exactly the images those releases put on :latest.
LISTED="docker/release-images must list exactly the images releases put on :latest before the Build workflow attested its images"
fresh
edit "${RELEASE_IMAGES}" 'sha256:edd2e1623fa31176f37c99ff6d3e0f40743fba0680f86dc2eac59b5b974e0208' \
    'sha256:0000000000000000000000000000000000000000000000000000000000000000'
expect "another digest listed for a release's image" "${LISTED}"

fresh
printf '%s\n' 'v0.23.0+ds.16 0000000000000000000000000000000000000000 sha256:0000000000000000000000000000000000000000000000000000000000000000' \
    >> "${COPY}/${RELEASE_IMAGES}"
expect "an image added to docker/release-images" "${LISTED}"

fresh
rm "${COPY}/${RELEASE_IMAGES}"
expect "docker/release-images left out" "${LISTED}"

fresh
edit "${LATEST}" 'version=$(docker run --rm --entrypoint /opt/ant/ant-node "$IMAGE@$DIGEST" --version)' \
    'version="ant-node 0.23.0+ds.15"'
expect "a node version not read from the image" "latest.yml's checks job must read the node version from the image"

PUBLISHED="latest.yml's checks job must refuse an image whose node is not a published release's"
fresh
edit "${LATEST}" 'if [[ "$published" != false ]]; then' 'if false; then'
expect "a node with no published release" "${PUBLISHED}"

fresh
edit "${LATEST}" "--jq '.isDraft or .isPrerelease'" "--jq '.isDraft'"
expect "a pre-release taken as published" "${PUBLISHED}"

fresh
edit "${LATEST}" 'if [[ ! "$version" =~ ^ant-node\ ([0-9]+\.[0-9]+\.[0-9]+\+ds\.[1-9][0-9]*)$ ]]; then' \
    'if [[ ! "$version" =~ ^ant-node\ (.*)$ ]]; then'
expect "a node of any version taken as a release's" "${PUBLISHED}"

# What the node is built from may change since its release only with an
# acknowledgement that names that change: node_code, the fingerprint of the
# git object each path has at the tag and at the commit.
CHANGES="latest.yml's checks job must refuse, unless the run is a withdrawal, any change since the node's release to what the node is built from, unless node_code is that change's fingerprint, and list the changes in the summary"
fresh
edit "${LATEST}" 'changes=$(git diff --stat "$released" "$COMMIT" -- "${paths[@]}")' 'changes=""'
expect "the node's changes left out of the summary" "${CHANGES}"

NODE_PATHS='paths=(src vendor Cargo.toml Cargo.lock build.rs rust-toolchain rust-toolchain.toml .cargo)'
fresh
edit "${LATEST}" "${NODE_PATHS}" 'paths=(src Cargo.toml Cargo.lock build.rs rust-toolchain rust-toolchain.toml .cargo)'
expect "the vendored crates left out of the node's code" "${CHANGES}"

fresh
edit "${LATEST}" "${NODE_PATHS}" 'paths=(src vendor Cargo.toml Cargo.lock build.rs)'
expect "the toolchain's files left out of the node's code" "${CHANGES}"

fresh
edit "${LATEST}" 'for path in "${paths[@]}"; do' 'for path in src; do'
expect "the node's code read from src alone" "${CHANGES}"

ACK='if [[ "$WITHDRAW" != true && "$(node_code "$released")" != "$(node_code "$COMMIT")" && "$NODE_CODE" != "$fingerprint" ]]; then'
fresh
edit "${LATEST}" "${ACK}" 'if false; then'
expect "a change to the node's code taken with no acknowledgement" "${CHANGES}"

fresh
edit "${LATEST}" "${ACK}" 'if [[ "$WITHDRAW" != true && "$(node_code "$released")" != "$(node_code "$COMMIT")" && -z "$NODE_CODE" ]]; then'
expect "any node_code taken as the acknowledgement" "${CHANGES}"

fresh
edit "${LATEST}" 'fingerprint=$({ node_code "$released"; node_code "$COMMIT"; } | sha256sum | cut -d'"' '"' -f1)' \
    'fingerprint=$(node_code "$COMMIT" | sha256sum | cut -d'"' '"' -f1)'
expect "a fingerprint of the commit's code alone, not of the change" "${CHANGES}"

# The checks job hands on the Build run and the node version alone, each
# written once by the step that checked it, and 'Check the digest' the
# record it took.
HANDS_ON="latest.yml's checks job must hand on exactly the Build run and the node version its steps checked"
fresh
edit "${LATEST}" '      version: ${{ steps.version.outputs.version }}' '      version: ant-node 0.23.0+ds.15'
expect "a node version handed on as a literal" "${HANDS_ON}"

fresh
edit "${LATEST}" '      run: ${{ steps.build_run.outputs.run }}' '      run: "39413254639"'
expect "a Build run handed on as a literal" "${HANDS_ON}"

fresh
edit "${LATEST}" $'      version: ${{ steps.version.outputs.version }}\n' \
    $'      version: ${{ steps.version.outputs.version }}\n      tag: v0.23.0+ds.15\n'
expect "another value handed on" "${HANDS_ON}"

FROM_VERSION="latest.yml's checks job must hand on what 'Read the image's node version' checked"
fresh
edit "${LATEST}" 'echo "version=$version" >> "$GITHUB_OUTPUT"' 'echo "version=ant-node 0.23.0+ds.15" >> "$GITHUB_OUTPUT"'
expect "a literal node version written for the latest job" "${FROM_VERSION}"

fresh
edit "${LATEST}" $'          echo "version=$version" >> "$GITHUB_OUTPUT"\n' \
    $'          echo "version=$version" >> "$GITHUB_OUTPUT"\n          echo "version=ant-node 0.23.0+ds.15" >> "$GITHUB_OUTPUT"\n'
expect "the node version written again after it" "${FROM_VERSION}"

fresh
edit "${LATEST}" $'        id: version\n' ''
expect "the node version's step with no id" "${FROM_VERSION}"

fresh
edit "${LATEST}" $'        id: version\n' $'        id: node_version\n'
expect "the node version's step under another id" "${FROM_VERSION}"

FROM_BUILD_RUN="latest.yml's checks job must hand on what 'Check the commit's Build run' checked"
fresh
edit "${LATEST}" 'echo "run=$run" >> "$GITHUB_OUTPUT"' 'echo "run=39413254639" >> "$GITHUB_OUTPUT"'
expect "a literal Build run written for the latest job" "${FROM_BUILD_RUN}"

fresh
edit "${LATEST}" $'        id: build_run\n' ''
expect "the Build run's step with no id" "${FROM_BUILD_RUN}"

fresh
edit "${LATEST}" $'        id: build_run\n' $'        id: build\n'
expect "the Build run's step under another id" "${FROM_BUILD_RUN}"

FROM_DIGEST="latest.yml's checks job must hand on what 'Check the digest' checked"
fresh
edit "${LATEST}" 'echo "record=$record" >> "$GITHUB_OUTPUT"' 'echo "record=artifact" >> "$GITHUB_OUTPUT"'
expect "a literal record written for the withdrawal's check" "${FROM_DIGEST}"

fresh
edit "${LATEST}" $'        id: digest\n' ''
expect "the digest's step with no id" "${FROM_DIGEST}"

# A withdrawal goes back to an image :latest held before: one an earlier
# Latest run from main moved it to, by the digest its title starts with, or
# a release's.
WITHDRAWAL="latest.yml's checks job must check that a withdrawal goes back to an image :latest held before"
fresh
edit "${LATEST}" 'if [[ -z "$held" ]]; then' 'if false; then'
expect "a withdrawal to any image" "${WITHDRAWAL}"

fresh
edit "${LATEST}" $'if [[ "$WITHDRAW" == true ]]; then\n            tags=' $'if false; then\n            tags='
expect "a withdrawal's image never checked" "${WITHDRAWAL}"

fresh
edit "${LATEST}" '-f head_sha="$COMMIT" -f event=push -f status=success -f per_page=100 \' \
    '-f head_sha="$COMMIT" -f event=push -f per_page=100 \'
expect "a withdrawal to a release whose Release run failed" "${WITHDRAWAL}"

fresh
edit "${LATEST}" '-f branch=main -f event=workflow_dispatch -f status=success -f per_page=100 \' \
    '-f status=success -f per_page=100 \'
expect "a withdrawal to an image a Latest run on another branch moved latest to" "${WITHDRAWAL}"

fresh
edit "${LATEST}" 'if [[ "$title" == "Point latest at $DIGEST"* || "$title" == "Withdraw latest to $DIGEST"* ]]; then' \
    'if [[ "$title" == "Point latest at "* || "$title" == "Withdraw latest to "* ]]; then'
expect "a withdrawal to any image of a commit an earlier Latest run moved latest to" "${WITHDRAWAL}"

fresh
edit "${LATEST}" 'if [[ -z "$by_latest" && "$RECORD" == attestation ]]; then' 'if false; then'
expect "a withdrawal taking any attested image of a release's commit unsaid" "${WITHDRAWAL}"

fresh
edit "${LATEST}" $'          RECORD: ${{ steps.digest.outputs.record }}\n' ''
expect "the record taken never reaching the withdrawal's check" "${WITHDRAWAL}"

fresh
edit "${LATEST}" $'    environment: latest\n' ''
expect "latest.yml's latest job outside the latest environment" "latest.yml's latest job must run in the latest environment"

fresh
edit "${LATEST}" $'    environment: latest\n' $'    environment: release\n'
expect "latest.yml's latest job in the release environment" "latest.yml's latest job must run in the latest environment"

fresh
edit "${LATEST}" $'    needs: checks\n' ''
expect "latest.yml's latest job without its checks" "latest.yml's latest job must need the checks job"

LATEST_PERMISSIONS="latest.yml's latest job must ask for attestations: read, contents: read and packages: write and nothing else"
fresh
edit "${LATEST}" $'      packages: write\n' $'      packages: write\n      contents: write\n'
expect "latest.yml's latest job able to write more" "${LATEST_PERMISSIONS}"

fresh
edit "${LATEST}" $'    permissions:\n      attestations: read\n      contents: read\n      packages: write\n' \
    $'    permissions:\n      attestations: read\n      packages: write\n'
expect "latest.yml's latest job unable to compare commits" "${LATEST_PERMISSIONS}"

fresh
edit "${LATEST}" $'      attestations: read\n      contents: read\n      packages: write\n' \
    $'      attestations: write\n      contents: read\n      packages: write\n'
expect "latest.yml's latest job able to write attestations" "${LATEST_PERMISSIONS}"

VERSIONS="latest.yml's latest job must refuse an image whose node is newer than :latest's, or older unless the run is a withdrawal"
fresh
edit "${LATEST}" 'if [[ "$order" == "older than" && "$WITHDRAW" != true ]]; then' 'if false; then'
expect "an older node outside a withdrawal" "${VERSIONS}"

fresh
edit "${LATEST}" 'if [[ "$order" == "newer than" ]]; then' 'if false; then'
expect "a newer node with no release" "${VERSIONS}"

fresh
edit "${LATEST}" 'current_version=$(docker run --rm --entrypoint /opt/ant/ant-node "$IMAGE@$current" --version)' \
    'current_version="ant-node 0.0.0"'
expect ":latest's node version not read from its image" "${VERSIONS}"

fresh
edit "${LATEST}" $'| if $n > $c then "newer than" elif $n == $c then "the same as" else "older than" end\'); then' \
    $'| "the same as"\'); then'
expect "every node taken as the one :latest has" "latest.yml's latest job must order node versions as nodes do"

BEFORE="latest.yml's latest job must refuse, unless the run is a withdrawal, an image built from a commit before the one :latest's image was built from"
fresh
edit "${LATEST}" 'if [[ "$relation" != ahead && "$relation" != identical && "$WITHDRAW" != true ]]; then' \
    'if [[ "$relation" == diverged && "$WITHDRAW" != true ]]; then'
expect "an older image with the same node outside a withdrawal" "${BEFORE}"

fresh
edit "${LATEST}" 'relation=$(gh api "repos/$GH_REPO/compare/$base...$COMMIT" --jq .status 2>/dev/null || true)' \
    'relation=identical'
expect "the commit never compared with :latest's" "${BEFORE}"

fresh
edit "${LATEST}" 'base=$marker' 'base=$COMMIT'
expect ":latest's commit not taken from its image marker" "${BEFORE}"

fresh
edit "${LATEST}" 'base=$(gh api "repos/$GH_REPO/commits/refs/tags/v${current_version#ant-node }" --jq .sha 2>/dev/null || true)' \
    'base=$COMMIT'
expect ":latest's commit not taken from its release's tag when it has no marker" "${BEFORE}"

# The summary gives the command that moves :latest back only for an image
# that a withdrawal can still go back to once its Build run's record has
# expired: one attested, or listed in docker/release-images.
LASTING="latest.yml's latest job must give the command that moves :latest back only for an image with a record that doesn't expire"
fresh
edit "${LATEST}" $'            if [[ -n "$lasting" ]]; then\n' $'            if [[ "$base" =~ ^[0-9a-f]{40}$ ]]; then\n'
expect "the command that moves latest back given for an image with no lasting record" "${LASTING}"

fresh
edit "${LATEST}" '--source-ref refs/heads/main --source-digest "$base" --format json \' '--format json \'
expect ":latest's image taken as attested whatever commit it was built from" "${LASTING}"

fresh
edit "${LATEST}" '!/^#/ && NF == 3 && $2 == base && $3 == current { found = 1 }' '!/^#/ && NF == 3 && $2 == base { found = 1 }'
expect ":latest's image taken as listed for any digest of its commit" "${LASTING}"

fresh
edit "${LATEST}" $'      DIGEST: ${{ inputs.digest }}\n      WITHDRAW: ${{ inputs.withdraw }}\n      RUN:' \
    $'      DIGEST: ${{ needs.checks.outputs.other }}\n      WITHDRAW: ${{ inputs.withdraw }}\n      RUN:'
expect "latest.yml's latest job moving another digest" \
    "latest.yml's latest job must move :latest to the digest the checks job checked"

MOVED="latest.yml's latest job must move :latest once, with exactly"
fresh
edit "${LATEST}" 'docker buildx imagetools create --prefer-index=false --tag "$IMAGE:latest" "$IMAGE@$DIGEST"' \
    'docker buildx imagetools create --prefer-index=false --tag "$IMAGE:latest" "$IMAGE:$COMMIT"'
expect "latest moved to the commit's tag, not the checked digest" "${MOVED}"

fresh
edit "${LATEST}" 'if [[ "$now" != "$DIGEST" ]]; then' 'if false; then'
expect "latest moved and not checked" "${MOVED}"

for write in "${EARLY_WRITES[@]}"; do
    fresh
    edit "${LATEST}" $'      - name: Check the image against latest\'s, and write the summary\n' \
        $'      - name: Move latest early\n        run: '"${write}"$'\n\n      - name: Check the image against latest\'s, and write the summary\n'
    expect "latest written by latest.yml before its checks (${write})" "${MOVED}"
done

MOVE_LINE=$'          docker buildx imagetools create --prefer-index=false --tag "$IMAGE:latest" "$IMAGE@$DIGEST"\n'
fresh
edit "${LATEST}" "${MOVE_LINE}" ''
edit "${LATEST}" $'          echo "- $IMAGE:latest is now $DIGEST." | tee -a "$GITHUB_STEP_SUMMARY"\n' \
    "${MOVE_LINE}"$'          echo "- $IMAGE:latest is now $DIGEST." | tee -a "$GITHUB_STEP_SUMMARY"\n'
expect "latest checked before it moved" "${MOVED}"

SUMMARY_LINE=$'          echo "Moving $IMAGE:latest from $current to $DIGEST." >> "$GITHUB_STEP_SUMMARY"\n'
SUMMARY_FIRST="latest.yml's latest job must write the digest :latest had (\$current) and the new one (\$DIGEST) to the step summary before it moves :latest"
fresh
edit "${LATEST}" "${SUMMARY_LINE}" ''
edit "${LATEST}" $'          echo "- $IMAGE:latest is now $DIGEST." | tee -a "$GITHUB_STEP_SUMMARY"\n' \
    $'          echo "- $IMAGE:latest is now $DIGEST." | tee -a "$GITHUB_STEP_SUMMARY"\n'"${SUMMARY_LINE}"
expect "the digest :latest had written only after it moved" "${SUMMARY_FIRST}"

fresh
edit "${LATEST}" "${SUMMARY_LINE}" $'          echo "Moving $IMAGE:latest to $DIGEST." >> "$GITHUB_STEP_SUMMARY"\n'
expect "the digest :latest had left out of the summary" "${SUMMARY_FIRST}"

COMPARE_REFUSAL=$(grep -A3 -F 'if [[ "$relation" != ahead && "$relation" != identical && "$WITHDRAW" != true ]]; then' "${LATEST}")$'\n'
fresh
edit "${LATEST}" "${COMPARE_REFUSAL}" ''
edit "${LATEST}" "${MOVE_LINE}" "${MOVE_LINE}${COMPARE_REFUSAL}"
expect "a check of latest.yml made after the move" "latest.yml's latest job must make every check before it moves :latest"

fresh
edit "${LATEST}" $'    needs: checks\n' $'    needs: checks\n    if: ${{ always() }}\n'
expect "latest.yml's latest job after failed checks" "latest.yml must have no if:"

fresh
edit "${LATEST}" $'      - name: Check the commit is on main\n' \
    $'      - name: Check the commit is on main\n        continue-on-error: true\n'
expect "a check of latest.yml continuing on error" "latest.yml must not continue on error"

# Every refusal stops its step, nothing ends a step before its checks or
# lets a failure pass, and the steps take the inputs and what the checks
# found from their job's env alone.
STOPS="latest.yml must follow each ::error:: with exit 1"
fresh
edit "${LATEST}" $'            echo "::error::run this workflow from main (--ref main), not $GITHUB_REF"\n            exit 1\n' \
    $'            echo "::error::run this workflow from main (--ref main), not $GITHUB_REF"\n'
expect "a refusal of latest.yml that doesn't stop its step" "${STOPS}"

fresh
edit "${LATEST}" $'only a withdrawal (withdraw) may move it back"\n            exit 1\n          fi\n          marker=' \
    $'only a withdrawal (withdraw) may move it back"\n            true\n          fi\n          marker='
expect "a refusal of latest.yml carrying on with true" "${STOPS}"

EARLY="latest.yml must have no line that ends a step early or lets a failure pass"
fresh
edit "${LATEST}" $'      - name: Check the commit is on main\n        run: |\n' \
    $'      - name: Check the commit is on main\n        run: |\n          exit 0\n'
expect "a check of latest.yml ended before it starts" "${EARLY}"

fresh
edit "${LATEST}" $'      - name: Check the commit is on main\n        run: |\n' \
    $'      - name: Check the commit is on main\n        run: |\n          set +e\n'
expect "a check of latest.yml carrying on after a failure" "${EARLY}"

fresh
edit "${LATEST}" $'      - name: Check the commit is on main\n        run: |\n' \
    $'      - name: Check the commit is on main\n        shell: bash {0}\n        run: |\n'
expect "a check of latest.yml in a shell without -e" "latest.yml must run every step in bash, as its defaults set it"

OWN_ENV="latest.yml's steps must take their environment from their job's env alone"
fresh
edit "${LATEST}" $'      - name: Check the image against latest\'s, and write the summary\n' \
    $'      - name: Check the image against latest\'s, and write the summary\n        env:\n          WITHDRAW: "true"\n'
expect "every run taken as a withdrawal by the version check" "${OWN_ENV}"

fresh
edit "${LATEST}" $'      - name: Point latest at it\n' \
    $'      - name: Point latest at it\n        env:\n          DIGEST: sha256:0000000000000000000000000000000000000000000000000000000000000000\n'
expect "latest moved to a digest a step sets" "${OWN_ENV}"

fresh
edit "${LATEST}" $'          RUN: ${{ steps.build_run.outputs.run }}\n' \
    $'          RUN: ${{ steps.build_run.outputs.run }}\n          COMMIT: ${{ github.sha }}\n'
expect "the digest checked against another commit" "${OWN_ENV}"

fresh
edit "${LATEST}" $'          RECORD: ${{ steps.digest.outputs.record }}\n' \
    $'          RECORD: ${{ steps.digest.outputs.record }}\n          DIGEST: ${{ inputs.commit }}\n'
expect "a withdrawal checked for another digest" "${OWN_ENV}"

fresh
edit "${LATEST}" $'          echo "- $COMMIT is on main." >> "$GITHUB_STEP_SUMMARY"\n' \
    $'          echo "- $COMMIT is on main." >> "$GITHUB_STEP_SUMMARY"\n          echo "WITHDRAW=true" >> "$GITHUB_ENV"\n'
expect "a later step's environment changed" "latest.yml must change no later step's environment"

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
edit "${BUILD}" 'run: bash scripts/check-darwin-nodes.sh target/release/darwin-nodes' 'run: true'
expect "the manager without darwin-client's own check" "build.yml's manager job must build darwin-nodes with --locked"

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

# The image is attested, by the digest the run recorded: the record of it
# latest.yml takes once the digest artifact has expired.
ATTESTED="build.yml's image job must attest the image it pushed, by the digest it recorded"
fresh
drop_step "${BUILD}" "Attest the image"
expect "the image not attested" "${ATTESTED}"

fresh
edit "${BUILD}" 'subject-digest: ${{ steps.digest.outputs.digest }}' 'subject-digest: ${{ inputs.digest }}'
expect "another digest attested" "${ATTESTED}"

fresh
edit "${BUILD}" $'      id-token: write\n' ''
expect "the image job unable to attest" "build.yml's image job must be able to attest the image"

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
# commit's Test run from the runs of test.yml; latest.yml's checks job picks
# the given commit's with the same filter (the check holds both to it).
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

# The jq filter latest.yml's latest job orders the image's node version
# against :latest's with: the lines after the one that passes it
# --arg current "$current_version", up to its closing quote. release.yml's
# latest job orders versions with the same one (the check holds both to it).
version_order=$(awk -v q="'" '
    !inside && index($0, "--arg current \"$current_version\" " q) { inside = 1; next }
    inside && index($0, q "); then") { print substr($0, 1, index($0, q "); then") - 1); exit }
    inside { print }
' "${LATEST}")

# The filter must put the version NEW reports against CURRENT as WANT
# ("newer than", "the same as" or "older than"), or fail when WANT is
# "refused".
orders() {
    local name=$1 want=$2 new=$3 current=$4 got
    if [[ -z "${version_order}" ]]; then
        fail "latest.yml's version order, ${name}: this case is out of date: no jq filter follows --arg current \"\$current_version\" in ${LATEST}"
        return
    fi
    if ! got=$(jq -rn --arg new "${new}" --arg current "${current}" "${version_order}" 2> /dev/null); then
        got=refused
    fi
    if [[ "${got}" != "${want}" ]]; then
        fail "latest.yml's version order, ${name}: put '${new}' against '${current}' as '${got}', not '${want}'"
        return
    fi
    PASSED=$((PASSED + 1))
}

orders "the same version" "the same as" "ant-node 0.23.0+ds.15" "ant-node 0.23.0+ds.15"
orders "the next build" "newer than" "ant-node 0.23.0+ds.16" "ant-node 0.23.0+ds.15"
orders "the build before" "older than" "ant-node 0.23.0+ds.14" "ant-node 0.23.0+ds.15"
orders "build numbers compared as numbers" "newer than" "ant-node 0.23.0+ds.10" "ant-node 0.23.0+ds.9"
orders "a later upstream version over any build" "newer than" "ant-node 0.23.1" "ant-node 0.23.0+ds.99"
orders "an upstream version before its ds.1" "older than" "ant-node 0.23.0" "ant-node 0.23.0+ds.1"
orders "the minor version before the patch" "older than" "ant-node 0.23.9+ds.40" "ant-node 0.24.0"
orders "a pre-release" refused "ant-node 0.24.0-rc.1" "ant-node 0.23.0+ds.15"
orders "another build label" refused "ant-node 0.23.0+dev.16" "ant-node 0.23.0+ds.15"
orders "build ds.0" refused "ant-node 0.23.0+ds.0" "ant-node 0.23.0+ds.15"
orders "more after the version" refused "ant-node 0.23.0+ds.15 (patched)" "ant-node 0.23.0+ds.15"
orders "another program's version" refused "darwin-nodes 0.23.0" "ant-node 0.23.0+ds.15"
orders "no version read from :latest's image" refused "ant-node 0.23.0+ds.15" ""

# The lines latest.yml's checks job reads the node's code with, from its
# "Read the image's node version" step: from paths=( to the end of
# node_code(), and the line that makes the fingerprint (the check holds the
# step to them). They run in a sample repository, as the step runs them in
# the checkout: a change since the release's tag to what the node is built
# from must show, and no other change; and the fingerprint must name the
# change, the same for the same change from the same tag whatever else the
# commit changed, and another for any other change or tag.
node_code_lines=$(awk '
    { line = $0; sub(/^[ \t]+/, "", line) }
    index(line, "paths=(") == 1 { inside = 1 }
    inside { print line }
    inside && line == "}" { exit }
' "${LATEST}")
fingerprint_line=$(sed -n 's/^[[:space:]]*\(fingerprint=\$(.*\)$/\1/p' "${LATEST}")

REPO="${WORK}/repo"
git init -q "${REPO}"

# Commits FILE, holding TEXT, on top of COMMIT ("" for the first commit),
# and prints the new commit.
commit_on() {
    if [[ -n "$1" ]]; then
        git -C "${REPO}" checkout -q --detach "$1"
    fi
    mkdir -p "$(dirname "${REPO}/$2")"
    printf '%s\n' "$3" > "${REPO}/$2"
    git -C "${REPO}" add -A
    git -C "${REPO}" -c user.name=test -c user.email=test@example.com commit -q -m "$2"
    git -C "${REPO}" rev-parse HEAD
}

# The node's code from the tag at RELEASED to COMMIT, as the step reads it:
# "changed" or "unchanged", then the fingerprint.
node_code_of() {
    (
        cd "${REPO}"
        released=$1
        COMMIT=$2
        fingerprint=""
        eval "${node_code_lines}"
        eval "${fingerprint_line}"
        if [[ "$(node_code "${released}")" != "$(node_code "${COMMIT}")" ]]; then
            echo "changed ${fingerprint}"
        else
            echo "unchanged ${fingerprint}"
        fi
    )
}

# The node's code from RELEASED to COMMIT must read as WANT.
code_reads() {
    local name=$1 want=$2 got
    if [[ -z "${node_code_lines}" || -z "${fingerprint_line}" ]]; then
        fail "latest.yml's node code, ${name}: this case is out of date: no paths=( ... node_code() lines or fingerprint= line in ${LATEST}"
        return
    fi
    if ! got=$(node_code_of "$3" "$4"); then
        fail "latest.yml's node code, ${name}: the step's lines failed"
        return
    fi
    if [[ "${got%% *}" != "${want}" ]]; then
        fail "latest.yml's node code, ${name}: read as ${got%% *}, not ${want}"
        return
    fi
    PASSED=$((PASSED + 1))
}

# The fingerprints of the code from RELEASED to COMMIT and from OTHER_RELEASED
# to OTHER_COMMIT must be WANT: the same, or different.
fingerprints() {
    local name=$1 want=$2 one two got=different
    if ! one=$(node_code_of "$3" "$4") || ! two=$(node_code_of "$5" "$6"); then
        fail "latest.yml's node code, ${name}: the step's lines failed"
        return
    fi
    if [[ "${one#* }" =~ ^[0-9a-f]{64}$ && "${one#* }" == "${two#* }" ]]; then
        got=same
    fi
    if [[ ! "${one#* }" =~ ^[0-9a-f]{64}$ || "${got}" != "${want}" ]]; then
        fail "latest.yml's node code, ${name}: fingerprints ${one#* } and ${two#* }, not ${want}"
        return
    fi
    PASSED=$((PASSED + 1))
}

tag=$(commit_on "" src/lib.rs "fn main() {}")
tag=$(commit_on "${tag}" Cargo.toml "[package]")
tag=$(commit_on "${tag}" Cargo.lock "version = 3")
tag=$(commit_on "${tag}" build.rs "fn main() {}")
tag=$(commit_on "${tag}" vendor/ant-protocol/src/lib.rs "pub fn f() {}")
tag=$(commit_on "${tag}" docker/Dockerfile "FROM ubuntu:24.04")
readme=$(commit_on "${tag}" README.md "A node.")
image=$(commit_on "${tag}" docker/Dockerfile "FROM ubuntu:26.04")
source=$(commit_on "${tag}" src/lib.rs "fn main() { run() }")
source_readme=$(commit_on "${source}" README.md "A node.")
other_source=$(commit_on "${tag}" src/lib.rs "fn main() { stop() }")
vendored=$(commit_on "${tag}" vendor/ant-protocol/src/lib.rs "pub fn g() {}")
manifest=$(commit_on "${tag}" Cargo.toml "[package] # changed")
lockfile=$(commit_on "${tag}" Cargo.lock "version = 4")
build_script=$(commit_on "${tag}" build.rs "fn main() { println!() }")
toolchain=$(commit_on "${tag}" rust-toolchain.toml "[toolchain]")
old_toolchain=$(commit_on "${tag}" rust-toolchain "1.80.0")
cargo_config=$(commit_on "${tag}" .cargo/config.toml "[build]")
other_tag=$(commit_on "${tag}" src/other.rs "pub fn h() {}")

code_reads "the tag's own commit" unchanged "${tag}" "${tag}"
code_reads "a change to the README alone" unchanged "${tag}" "${readme}"
code_reads "a change to the image's files alone" unchanged "${tag}" "${image}"
code_reads "a change to src" changed "${tag}" "${source}"
code_reads "a change to a vendored crate" changed "${tag}" "${vendored}"
code_reads "a change to Cargo.toml" changed "${tag}" "${manifest}"
code_reads "a change to Cargo.lock" changed "${tag}" "${lockfile}"
code_reads "a change to build.rs" changed "${tag}" "${build_script}"
code_reads "a toolchain file added" changed "${tag}" "${toolchain}"
code_reads "an old-style toolchain file added" changed "${tag}" "${old_toolchain}"
code_reads "cargo's settings added" changed "${tag}" "${cargo_config}"
fingerprints "the same change, with a change outside the node's code" same \
    "${tag}" "${source}" "${tag}" "${source_readme}"
fingerprints "another change to the same file" different \
    "${tag}" "${source}" "${tag}" "${other_source}"
fingerprints "a change to another path" different \
    "${tag}" "${source}" "${tag}" "${toolchain}"
fingerprints "the same commit's code from another tag" different \
    "${tag}" "${source}" "${other_tag}" "${source}"

if ((FAILED > 0)); then
    echo "${FAILED} of $((PASSED + FAILED)) cases failed" >&2
    exit 1
fi
echo "scripts/check-workflows.sh: ${PASSED} cases pass. It passes on the workflows as they are and fails on each change that would let a release, or a move of :latest, skip a check or carry on past a refusal; the Test run filter takes a commit's own Test run, the latest jobs order node versions as nodes do, and latest.yml reads a change to the node's code, and only that, and names it by its fingerprint."
