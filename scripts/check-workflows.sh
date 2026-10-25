#!/usr/bin/env bash
#
# Checks that the GitHub workflows keep what releases rely on. The Build
# workflow's notices job and the Test workflow run it on every run; run it
# by hand with `bash scripts/check-workflows.sh`.
#
# It fails unless:
# - .github/workflows holds exactly build.yml, release.yml and test.yml;
# - every action a workflow uses is pinned to a full commit SHA;
# - release.yml runs only for v*+ds.* tags and grants no permissions unless
#   a job asks for them;
# - release.yml's build job compares the release key compiled into the node
#   with sign/release-signing-key.pub;
# - its checks job waits for the tagged commit's Test and Build runs and
#   fails unless both succeeded;
# - the Test run it takes is from the push to main, or one run by hand that
#   test.yml names "Test by hand": one without the listings cross-test,
#   which runs only weekly or when a run by hand asks for it;
# - test.yml's test job runs every test: the library and binaries, the
#   vendored ant-listings' and ant-protocol's own tests, every target in
#   tests/ (each file, and each folder with a mod.rs), and the mixed-version
#   harness's verdict tests, so the Test run a release waits for has run
#   every pin;
# - its publish job runs in the release environment and needs build and
#   checks;
# - its latest job needs checks and publish;
# - nothing in release.yml has an if: or continues on error, so no job can
#   run after a failed need and no step can skip a check;
# - build.yml's manager job builds the image's node manager, darwin-nodes,
#   with --locked from the darwin-client commit docker/manager-revision pins,
#   once it has checked that commit is on darwin-client's main, and checks
#   the binary for Autonomi's addresses and the reset and log-forwarding
#   routes; its image job needs that job and passes the image marker;
# - build.yml's linux-x64 job, which builds the image's node, and
#   release.yml's build job, which builds the node releases install, build it
#   with exactly `cargo build --release --locked --bin ant-node`, so no
#   feature joins the build unnoticed, and check the binary for the test
#   seams' variables (src/test_seams.rs) after building it and before
#   packaging it;
# - nothing but release.yml's latest job names the image's :latest tag.
#
# A release runs release.yml as it is at the tagged commit, so this can't
# stop a tag on a commit that changed it; it makes that commit's Build and
# Test runs fail, and the release waits for both.
#
# scripts/test_check_workflows.sh tests it, in the Test workflow.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
DIR=.github/workflows
BUILD="${DIR}/build.yml"
RELEASE="${DIR}/release.yml"
TEST="${DIR}/test.yml"
PROBLEMS=0

problem() {
    echo "::error::$*"
    PROBLEMS=$((PROBLEMS + 1))
}

# A top-level section of a workflow, release.yml unless another is given:
# its key line and the lines under it, leaving out comments and blank lines.
section() {
    awk -v key="$1" '
        /^[^ #]/ { inside = (index($0, key ":") == 1) }
        inside && !/^[ \t]*(#|$)/ { print }
    ' "${2:-${RELEASE}}"
}

# A job of a workflow, release.yml unless another is given: its key line and
# the lines under it, leaving out comments and blank lines.
job() {
    awk -v name="$1" '
        /^[^ #]/ { in_jobs = ($0 ~ /^jobs:/); current = ""; next }
        in_jobs && /^  [A-Za-z0-9_-]+:/ { current = $1; sub(/:$/, "", current) }
        in_jobs && current == name && !/^[ \t]*(#|$)/ { print }
    ' "${2:-${RELEASE}}"
}

# The jobs a job of a workflow needs, release.yml unless another is given,
# one a line, from `needs: x` or `needs: [x, y]`.
needs() {
    job "$1" "${2:-${RELEASE}}" | sed -n 's/^    needs:[[:space:]]*//p' | sed 's/[][]//g' | tr ',' '\n' \
        | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | sed '/^$/d'
}

# The workflow files.
found=$(ls -A "${DIR}" | LC_ALL=C sort | tr '\n' ' ')
if [[ "${found}" != "build.yml release.yml test.yml " ]]; then
    problem "${DIR} holds: ${found% }; expected exactly build.yml release.yml test.yml"
fi

# Every action pinned to a commit: owner/repo[/path]@<40 hex digits>.
while IFS= read -r hit; do
    place=${hit%%:*}
    rest=${hit#*:}
    place="${place}:${rest%%:*}"
    ref=$(sed -e 's/^[[:space:]]*-\{0,1\}[[:space:]]*uses:[[:space:]]*//' \
        -e 's/[[:space:]]#.*$//' -e 's/[[:space:]]*$//' -e "s/^[\"']//" -e "s/[\"']\$//" \
        <<<"${rest#*:}")
    if [[ ! "${ref}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}$ ]]; then
        problem "${place}: '${ref}' is not pinned to a full commit SHA"
    fi
done < <(grep -nE '^[[:space:]]*(-[[:space:]]*)?uses:' "${DIR}"/*.yml || true)

if [[ ! -f "${RELEASE}" ]]; then
    problem "${RELEASE} is missing"
else
    # Triggered only by a v*+ds.* tag.
    expected_on=$(printf '%s\n' "on:" "  push:" "    tags: ['v*\\+ds.*']")
    if [[ "$(section on)" != "${expected_on}" ]]; then
        problem "release.yml must run only for v*+ds.* tags (on: push: tags: ['v*\\+ds.*']); it has: $(section on | tr '\n' ' ')"
    fi

    # No permissions unless a job asks for them.
    if [[ "$(section permissions)" != "permissions: {}" ]]; then
        problem "release.yml must set permissions: {} at the top; it has: $(section permissions | tr '\n' ' ')"
    fi

    # The build job compares the key compiled into the node with the
    # published key.
    build=$(job build)
    for line in \
        "sed -n '/^const RELEASE_SIGNING_KEY/,/^];/p' src/upgrade/signature.rs" \
        'if ! cmp -s "$RUNNER_TEMP/embedded.pub" sign/release-signing-key.pub; then'; do
        if ! grep -qF -- "${line}" <<<"${build}"; then
            problem "release.yml's build job no longer compares the key compiled into the node with sign/release-signing-key.pub (missing: ${line})"
        fi
    done

    # The checks job waits for the commit's Test and Build runs, and fails
    # unless both succeeded.
    checks=$(job checks)
    for line in \
        'actions/workflows/test.yml/runs' \
        'actions/workflows/build.yml/runs'; do
        if ! grep -qF -- "${line}" <<<"${checks}"; then
            problem "release.yml's checks job no longer looks for the commit's runs (missing: ${line})"
        fi
    done
    if [[ $(grep -cF -- '-f head_sha="$GITHUB_SHA"' <<<"${checks}" || true) -ne 2 ]]; then
        problem "release.yml's checks job must look for both runs of the tagged commit (-f head_sha=\"\$GITHUB_SHA\")"
    fi
    if [[ $(grep -cF -- '!= "success" ]]; then' <<<"${checks}" || true) -ne 2 ]]; then
        problem "release.yml's checks job must fail unless both runs succeeded"
    fi

    # The Test run it takes is the newest from the push to main, or by hand
    # under the name test.yml gives a run without the listings cross-test
    # (below). A later run with the cross-test, which also tests
    # darwin-client, can't take its place.
    pick='--jq '\''[.workflow_runs[] | select((.event == "push" and .head_branch == "main") or (.event == "workflow_dispatch" and .display_title == "Test by hand"))] | max_by(.run_number) | .id // empty'\'''
    if ! grep -qF -- "${pick}" <<<"${checks}"; then
        problem "release.yml's checks job must take the Test run from the push to main, or one by hand without the listings cross-test (missing: ${pick})"
    fi

    # Publishing waits for the build and the checks, in the release
    # environment, whose reviewers approve it.
    if ! grep -qx '    environment: release' <<<"$(job publish)"; then
        problem "release.yml's publish job must run in the release environment"
    fi
    for need in build checks; do
        if ! needs publish | grep -qx "${need}"; then
            problem "release.yml's publish job must need ${need}"
        fi
    done

    # :latest moves only after the publish, to the image of the checked
    # Build run.
    for need in checks publish; do
        if ! needs latest | grep -qx "${need}"; then
            problem "release.yml's latest job must need ${need}"
        fi
    done

    # A job runs only once the jobs it needs succeeded, and a step only once
    # the steps before it did. A condition would change that: if: always()
    # on publish or latest runs it after a failed need, and if: false on a
    # step of the checks or build job skips that check. So would a job or
    # step that continues on error.
    while IFS= read -r hit; do
        problem "release.yml must have no if: (a condition can run a job after a failed need, or skip a check): ${hit}"
    done < <(grep -nE '^[[:space:]]*(-[[:space:]]+)?if:' "${RELEASE}" || true)
    while IFS= read -r hit; do
        problem "release.yml must not continue on error: ${hit}"
    done < <(grep -nE '^[[:space:]]*(-[[:space:]]+)?continue-on-error:' "${RELEASE}" || true)
fi

if [[ ! -f "${TEST}" ]]; then
    problem "${TEST} is missing"
else
    # A run by hand is named for whether it runs the listings cross-test, and
    # the cross-test runs only weekly or when a run by hand asks for it. So a
    # run named "Test by hand", which release.yml's checks job may take,
    # tested this commit alone.
    expected_name="run-name: \${{ github.event_name == 'workflow_dispatch' && (inputs.listings_cross_test && 'Test by hand, with the listings cross-test' || 'Test by hand') || '' }}"
    if [[ "$(section run-name "${TEST}")" != "${expected_name}" ]]; then
        problem "test.yml must name a run by hand for whether it runs the listings cross-test (${expected_name}); it has: $(section run-name "${TEST}" | tr '\n' ' ')"
    fi
    expected_if="    if: \${{ github.event_name == 'schedule' || inputs.listings_cross_test }}"
    if [[ "$(job listings-cross-test "${TEST}" | grep -E '^    if:' || true)" != "${expected_if}" ]]; then
        problem "test.yml's listings-cross-test job must run only weekly or when a run by hand asks for it (${expected_if#    })"
    fi

    # The test job runs every test, so a pin counts once it is written: the
    # library and binaries, the vendored crates' own tests under this lock,
    # and every integration test target, found by the loop rather than
    # listed, so a new file in tests/ runs without a workflow change.
    test_job=$(job test "${TEST}")
    for line in \
        'run: cargo test --locked --features test-utils --lib --bins' \
        'run: cargo test --locked -p ant-listings' \
        'run: cargo test --locked --manifest-path vendor/ant-protocol/Cargo.toml' \
        'run: python3 scripts/test_mixed_versions_verdicts.py' \
        'for target in tests/*.rs tests/*/mod.rs; do' \
        'cargo test --locked --features test-utils --test "$name" "${args[@]}" || failed+=("$name")'; do
        if ! grep -qF -- "${line}" <<<"${test_job}"; then
            problem "test.yml's test job no longer runs every test (missing: ${line})"
        fi
    done
fi

if [[ ! -f "${BUILD}" ]]; then
    problem "${BUILD} is missing"
else
    # The image's node manager is built from the darwin-client commit
    # docker/manager-revision pins, with that commit's own lockfile, and the
    # binary the image ships is checked for what darwin-client's tests say it
    # leaves out.
    manager=$(job manager "${BUILD}")
    for line in \
        '{ read -r repository; read -r revision; } < docker/manager-revision' \
        'run: cargo build --release --locked -p ant-cli --bin darwin-nodes' \
        'for text in WithAutonomi logs.autonomi.com /api/v1/reset /api/v1/logs/forward; do'; do
        if ! grep -qF -- "${line}" <<<"${manager}"; then
            problem "build.yml's manager job must build darwin-nodes with --locked from the commit docker/manager-revision pins, and check the binary (missing: ${line})"
        fi
    done
    # Only from a commit on darwin-client's main, with main's history to
    # check it against: a commit only on a branch is lost if the branch is
    # squashed or rebased into main.
    for line in \
        'git -C darwin-client fetch -q --filter=blob:none origin +refs/heads/main:refs/remotes/origin/main' \
        'if ! git -C darwin-client merge-base --is-ancestor "$revision" refs/remotes/origin/main; then'; do
        if ! grep -qF -- "${line}" <<<"${manager}"; then
            problem "build.yml's manager job must check that the commit docker/manager-revision pins is on darwin-client's main (missing: ${line})"
        fi
    done
    # The image takes that build, and the commit it is built from as the
    # image marker every node gets.
    if ! needs image "${BUILD}" | grep -qx manager; then
        problem "build.yml's image job must need the manager job"
    fi
    if ! grep -qF -- 'build-args: IMAGE_VERSION=${{ github.sha }}' <<<"$(job image "${BUILD}")"; then
        problem "build.yml's image job must pass the image marker (build-args: IMAGE_VERSION=\${{ github.sha }})"
    fi
fi

# The node is built the same way for the image (build.yml's linux-x64 job)
# and for releases (release.yml's build job), with no feature, and each job
# checks the binary it built for the test seams' variables before packaging
# it: a build with test-utils has seams a test sets from the environment.
NODE_BUILD='run: cargo build --release --locked --bin ant-node'
SEAM_BINARY='if [[ ! -s target/release/ant-node ]]; then'
SEAM_CHECK='if grep -aqF ANT_TEST_SEAM_ target/release/ant-node; then'
for place in "${BUILD}:linux-x64" "${RELEASE}:build"; do
    file=${place%:*}
    name=${place#*:}
    [[ -f "${file}" ]] || continue
    where="$(basename "${file}")'s ${name} job"
    text=$(job "${name}" "${file}" | sed 's/^[[:space:]]*//')
    builds=$(grep -n 'cargo build' <<<"${text}" || true)
    if [[ "${builds#*:}" != "${NODE_BUILD}" ]]; then
        problem "${where} must build the node once, with exactly '${NODE_BUILD}' (it has: $(cut -d: -f2- <<<"${builds}" | tr '\n' ' '))"
        continue
    fi
    built=${builds%%:*}
    binary=$(grep -nxF -- "${SEAM_BINARY}" <<<"${text}" | cut -d: -f1 | tr '\n' ' ' || true)
    seam=$(grep -nxF -- "${SEAM_CHECK}" <<<"${text}" | cut -d: -f1 | tr '\n' ' ' || true)
    package=$(grep -nxF -- '- name: Package' <<<"${text}" | cut -d: -f1 | tr '\n' ' ' || true)
    if [[ ! "${binary}" =~ ^[0-9]+\ $ || ! "${seam}" =~ ^[0-9]+\ $ || ! "${package}" =~ ^[0-9]+\ $ ]] \
        || ((built > binary || binary > seam || seam > package)); then
        problem "${where} must check the node it built for test seams once, after the build and before the Package step (${SEAM_BINARY} ... ${SEAM_CHECK})"
    fi
done

# Only release.yml's latest job names the image's :latest tag, outside
# comments.
while IFS= read -r hit; do
    problem "only release.yml's latest job may write the :latest tag: ${hit}"
done < <(awk '
    FNR == 1 { in_jobs = 0; current = "" }
    /^[^ #]/ { in_jobs = ($0 ~ /^jobs:/); current = "" }
    in_jobs && /^  [A-Za-z0-9_-]+:/ { current = $1; sub(/:$/, "", current) }
    /^[ \t]*#/ { next }
    /:latest([^A-Za-z0-9_.-]|$)|value=latest|latest=(true|auto)/ {
        name = FILENAME
        sub(/^.*\//, "", name)
        if (!(name == "release.yml" && current == "latest")) {
            print FILENAME ":" FNR ": " $0
        }
    }
' "${DIR}"/*.yml)

if ((PROBLEMS > 0)); then
    echo "${PROBLEMS} problem(s) in ${DIR}" >&2
    exit 1
fi
echo "${DIR}: build.yml, release.yml and test.yml, every action pinned; release.yml runs for v*+ds.* tags with no default permissions, checks the release key, and publishes in the release environment only after the tagged commit's Test and Build runs succeeded, its Test run from the push to main or by hand without the listings cross-test, with no condition and nothing continuing on error; test.yml's test job runs every test; build.yml builds the image's node manager with --locked from the pinned darwin-client commit, which is on darwin-client's main, checks it, and gives the image its marker; build.yml and release.yml build the node with no feature and check it for test seams before packaging it; only release.yml's latest job moves :latest."
