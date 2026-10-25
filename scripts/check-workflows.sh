#!/usr/bin/env bash
#
# Checks that the GitHub workflows keep what releases rely on. The Build
# workflow's notices job, the Test workflow and latest.yml's checks job run
# it on every run; run it by hand with `bash scripts/check-workflows.sh`.
#
# It fails unless:
# - .github/workflows holds exactly build.yml, latest.yml, release.yml and
#   test.yml;
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
# - its latest job needs checks and publish, and moves :latest forward only:
#   before it moves it, it reads :latest and refuses unless :latest's node
#   is older than the release's, ordered as latest.yml orders them, or
#   :latest already has the image, so a re-run, which needs no approval,
#   can't undo an image release or a withdrawal;
# - nothing in release.yml or latest.yml has an if: or continues on error,
#   so no job can run after a failed need and no step can skip a check;
# - in both, the condition of each check pinned below is followed by its
#   ::error::, every ::error:: is followed by exit 1, so each refusal stops
#   its step, and no line ends a step early or carries on after a failure
#   (exit 0, exit, true, : or set +... standing alone);
# - latest.yml, which moves :latest when no node release does (an image
#   release, or a withdrawal), runs only by hand, with a commit, a digest
#   and a withdraw switch that is off unless set, grants no permissions
#   unless a job asks for them, and shares release.yml's concurrency group;
#   its steps run in bash, as its defaults set it, take the inputs and what
#   the checks found only from their job's env (no step sets its own but
#   'Check the digest', which takes the Build run the step before it found),
#   and change no later step's environment;
# - its checks job can write nothing; refuses a run from any ref but main
#   and inputs other than a full commit and a sha256 digest; runs this
#   script on the commit it runs from; checks that the commit is on main,
#   that its Build run from its push to main succeeded, smoke test included,
#   and so did its newest Test run, taken as release.yml takes it, and that
#   the digest is the image that Build run pushed, by its build provenance
#   attestation (signed for build.yml on main at the commit, by a run a push
#   started) or by the digest the run recorded, which must be the digest
#   while it is kept; reads the image's node version, which must be a
#   published release's, and writes the node's code changes since that
#   release's tag to the summary; and, for a withdrawal, checks that the
#   commit is a v*+ds.* tag's whose Release run succeeded, or one an earlier
#   Latest run moved :latest to;
# - its latest job needs the checks job, runs in the release environment,
#   can read contents and write packages and nothing else, refuses an image
#   whose node is newer than :latest's, and, unless the run is a withdrawal,
#   one whose node is older or that was built from a commit before the one
#   :latest's image was built from (its image marker, or for an image with
#   none its node release's tag), orders versions as nodes do, makes every
#   check before it moves :latest, writes :latest's digest and the new one
#   to the step summary, and only then moves :latest, to the checked
#   digest, and checks it;
# - build.yml's manager job builds the image's node manager, darwin-nodes,
#   with --locked from the darwin-client commit docker/manager-revision pins,
#   once it has checked that commit is on darwin-client's main, and checks
#   that the binary names that commit (the leading 7 or more characters
#   ant-cli's build script records) and holds none of Autonomi's addresses
#   and the reset and log-forwarding routes; its image job needs that job
#   and passes the image marker, and attests the image it pushed (build
#   provenance, kept in GitHub's attestations API);
# - build.yml's linux-x64 job, which builds the image's node, and
#   release.yml's build job, which builds the node releases install, build it
#   with exactly `cargo build --release --locked --bin ant-node`, so no
#   feature joins the build unnoticed, and check the binary for the test
#   seams' variables (src/test_seams.rs) after building it and before
#   packaging it;
# - nothing but the latest jobs of release.yml and latest.yml names the
#   image's :latest tag;
# - no job but release.yml's publish job and latest.yml's latest job runs
#   in an environment, and latest.yml reads no secret: the release
#   environment, which holds the release signing key, admits latest.yml's
#   runs from main as well as release.yml's from v*+ds.* tags.
#
# A release runs release.yml as it is at the tagged commit, so this can't
# stop a tag on a commit that changed it; it makes that commit's Build and
# Test runs fail, and the release waits for both. latest.yml runs as it is
# at main's head when it is dispatched, which can be later than the commit
# it is given, so its checks job runs this script itself, on that head.
#
# scripts/test_check_workflows.sh tests it, in the Test workflow.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
DIR=.github/workflows
BUILD="${DIR}/build.yml"
LATEST="${DIR}/latest.yml"
RELEASE="${DIR}/release.yml"
TEST="${DIR}/test.yml"
PROBLEMS=0

# How the latest jobs move :latest: to an image by its digest, copying the
# manifest as it is.
MOVE_LATEST='docker buildx imagetools create --prefer-index=false --tag "$IMAGE:latest" "$IMAGE@$DIGEST"'

# The jq program the latest jobs order node versions with, as nodes order
# them: by X.Y.Z, then by the +ds. build number, a version with none coming
# before ds.1; any other form is refused.
NODE_VERSION_ORDER=$(cat <<'EOF'
def node_version:
capture("^ant-node (?<major>[0-9]+)\\.(?<minor>[0-9]+)\\.(?<patch>[0-9]+)(\\+ds\\.(?<build>[1-9][0-9]*))?$")
// error("not ant-node X.Y.Z or X.Y.Z+ds.N: \(.)")
| [.major, .minor, .patch, (.build // "0")] | map(tonumber);
($new | node_version) as $n | ($current | node_version) as $c
| if $n > $c then "newer than" elif $n == $c then "the same as" else "older than" end
EOF
)

# The Test run release.yml's checks job and latest.yml's checks job take:
# the newest from the push to main, or by hand under the name test.yml
# gives a run without the listings cross-test (below). A later run with the
# cross-test, which also tests darwin-client, can't take its place.
TEST_RUN_PICK='--jq '\''[.workflow_runs[] | select((.event == "push" and .head_branch == "main") or (.event == "workflow_dispatch" and .display_title == "Test by hand"))] | max_by(.run_number) | .id // empty'\'''

# The step of release.yml's latest job that reads :latest before the move.
GUARD_STEP="Check latest's node is older than this release's"

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

# A step of a job's lines (as job gives them), by its name: from its
# "- name:" line up to the next step.
step() {
    awk -v name="$1" '
        /^      - / { inside = ($0 == "      - name: " name) }
        inside { print }
    ' <<<"$2"
}

# The jobs a job of a workflow needs, release.yml unless another is given,
# one a line, from `needs: x` or `needs: [x, y]`.
needs() {
    job "$1" "${2:-${RELEASE}}" | sed -n 's/^    needs:[[:space:]]*//p' | sed 's/[][]//g' | tr ',' '\n' \
        | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | sed '/^$/d'
}

# The permissions a job of a workflow asks for, one a line ("actions: read",
# or "write-all"), from its permissions: key.
job_permissions() {
    job "$1" "$2" | awk '
        /^    [A-Za-z0-9_-]+:/ { inside = 0 }
        /^    permissions:/ {
            inside = 1
            sub(/^    permissions:[[:space:]]*/, "")
            if ($0 != "") { print }
            next
        }
        inside && /^      / { sub(/^[[:space:]]+/, ""); print }
    '
}

# The jq program lines of TEXT order node versions with: the lines after the
# one that passes it --arg current "$current_version", up to its closing
# quote, with their leading spaces left out.
version_order() {
    awk -v q="'" '
        { sub(/^[ \t]+/, "") }
        !inside && index($0, "--arg current \"$current_version\" " q) { inside = 1; next }
        inside && index($0, q "); then") { print substr($0, 1, index($0, q "); then") - 1); exit }
        inside { print }
    ' <<<"$1"
}

# One problem saying WHAT unless TEXT (a job's or a step's lines) holds every
# one of the LINES. A line given as "refuse: COND" must be a line of TEXT of
# its own, leading spaces aside, and be followed directly, each time, by an
# `echo "::error::...` line, which is followed by exit 1 (see below): when
# COND holds, the step stops there.
holds() {
    local text=$1 what=$2 line missing=""
    shift 2
    for line in "$@"; do
        if [[ "${line}" == "refuse: "* ]]; then
            line=${line#refuse: }
            if ! COND="${line}" awk '
                { sub(/^[ \t]+/, "") }
                expect { if (index($0, "echo \"::error::") != 1) { bad = 1 }; expect = 0 }
                $0 == ENVIRON["COND"] { found = 1; expect = 1 }
                END { exit !(found && !bad && !expect) }
            ' <<<"${text}"; then
                missing+="${missing:+; }${line} followed by an ::error:: line"
            fi
        elif ! grep -qF -- "${line}" <<<"${text}"; then
            missing+="${missing:+; }${line}"
        fi
    done
    if [[ -n "${missing}" ]]; then
        problem "${what} (missing: ${missing})"
    fi
}

# The workflow files.
found=$(ls -A "${DIR}" | LC_ALL=C sort | tr '\n' ' ')
if [[ "${found}" != "build.yml latest.yml release.yml test.yml " ]]; then
    problem "${DIR} holds: ${found% }; expected exactly build.yml latest.yml release.yml test.yml"
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
    # without the listings cross-test.
    if ! grep -qF -- "${TEST_RUN_PICK}" <<<"${checks}"; then
        problem "release.yml's checks job must take the Test run from the push to main, or one by hand without the listings cross-test (missing: ${TEST_RUN_PICK})"
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

    # And only forward: a re-run of the latest job needs no approval, so
    # before the move it reads :latest, and refuses unless :latest's node is
    # older than the release's, or :latest already has the image. Otherwise
    # latest.yml moved it after the release (an image release, or a
    # withdrawal), and a re-run must not undo that.
    release_latest=$(job latest)
    guard=$(step "${GUARD_STEP}" "${release_latest}")
    holds "${guard}" "release.yml's latest job must check, in a step named \"${GUARD_STEP}\", that :latest's node is older than the release's, or that :latest already has the image" \
        'DIGEST: ${{ steps.image.outputs.digest }}' \
        'current=$(docker buildx imagetools inspect "$IMAGE:latest" --format '\''{{json .Manifest}}'\'' | jq -r .digest)' \
        'if [[ "$current" == "$DIGEST" ]]; then' \
        'current_version=$(docker run --rm --entrypoint /opt/ant/ant-node "$IMAGE@$current" --version)' \
        'if ! order=$(jq -rn --arg new "ant-node $VERSION" --arg current "$current_version" '\''' \
        'refuse: if [[ "$order" != "newer than" ]]; then'
    if [[ -n "${guard}" ]]; then
        if [[ "$(version_order "${guard}")" != "${NODE_VERSION_ORDER}" ]]; then
            problem "release.yml's latest job must order node versions as latest.yml does (the jq program after --arg current \"\$current_version\" must be scripts/check-workflows.sh's NODE_VERSION_ORDER)"
        fi
        guard_at=$(grep -nxF -- "      - name: ${GUARD_STEP}" <<<"${release_latest}" | head -n 1 | cut -d: -f1)
        moved_at=$(grep -nF -- "${MOVE_LATEST}" <<<"${release_latest}" | head -n 1 | cut -d: -f1 || true)
        if [[ -z "${moved_at}" ]] || ((guard_at > moved_at)); then
            problem "release.yml's latest job must check :latest (${GUARD_STEP}) before it moves it (${MOVE_LATEST})"
        fi
    fi
fi

if [[ ! -f "${LATEST}" ]]; then
    problem "${LATEST} is missing"
else
    # Run only by hand, with the commit and digest of the image :latest
    # gets, and whether the run is a withdrawal, off unless set. The inputs'
    # descriptions may change.
    expected_on=$(printf '%s\n' "on:" "  workflow_dispatch:" "    inputs:" \
        "      commit:" "        required: true" "        type: string" \
        "      digest:" "        required: true" "        type: string" \
        "      withdraw:" "        type: boolean" "        default: false")
    latest_on=$(section on "${LATEST}" | grep -v '^[[:space:]]*description:' || true)
    if [[ "${latest_on}" != "${expected_on}" ]]; then
        problem "latest.yml must run only by hand (on: workflow_dispatch:), with the inputs commit and digest, required, and withdraw, false unless set; it has: $(tr '\n' ' ' <<<"${latest_on}")"
    fi

    # No permissions unless a job asks for them.
    if [[ "$(section permissions "${LATEST}")" != "permissions: {}" ]]; then
        problem "latest.yml must set permissions: {} at the top; it has: $(section permissions "${LATEST}" | tr '\n' ' ')"
    fi

    # In release.yml's concurrency group, so a release's latest job and this
    # workflow never move :latest at once.
    if [[ -z "$(section concurrency)" || "$(section concurrency "${LATEST}")" != "$(section concurrency)" ]]; then
        problem "latest.yml must share release.yml's concurrency group; it has: $(section concurrency "${LATEST}" | tr '\n' ' ')"
    fi

    # Every step in bash, which GitHub runs with -e and -o pipefail, so a
    # failed command stops it: as the defaults set it, with no shell of a
    # job's or a step's own.
    if [[ "$(section defaults "${LATEST}")" != $'defaults:\n  run:\n    shell: bash' ]] \
        || [[ $(grep -cE '^[[:space:]]*(-[[:space:]]+)?shell:' "${LATEST}" || true) -ne 1 ]]; then
        problem "latest.yml must run every step in bash, as its defaults set it (defaults: run: shell: bash), with no shell of a job's or a step's own"
    fi

    # The checks job: everything that can be checked before the approval,
    # by a job that can write nothing.
    checks=$(job checks "${LATEST}")
    if [[ -z "${checks}" ]] || job_permissions checks "${LATEST}" | grep -qE 'write'; then
        problem "latest.yml's checks job must be able to write nothing; it asks for: $(job_permissions checks "${LATEST}" | tr '\n' ' ')"
    fi
    holds "${checks}" "latest.yml's checks job must refuse a run from any ref but main" \
        'refuse: if [[ "$GITHUB_REF" != refs/heads/main ]]; then'
    holds "${checks}" "latest.yml's checks job must take the commit, the digest and the withdraw switch from the inputs, and refuse anything but a full commit and a sha256 digest" \
        'COMMIT: ${{ inputs.commit }}' \
        'DIGEST: ${{ inputs.digest }}' \
        'WITHDRAW: ${{ inputs.withdraw }}' \
        'refuse: if [[ ! "$COMMIT" =~ ^[0-9a-f]{40}$ ]]; then' \
        'refuse: if [[ ! "$DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]; then'

    # This workflow runs as it is at main's head, which can be later than
    # the commit given: the checks job checks out that head (whole, for the
    # node's changes and the tags below) and runs this script on it.
    checkout=$(step "Check out the commit this runs from" "${checks}")
    if ! grep -qE '^        uses: actions/checkout@' <<<"${checkout}" \
        || ! grep -qxF '          fetch-depth: 0' <<<"${checkout}" \
        || grep -qE '^          (ref|repository|path):' <<<"${checkout}" \
        || [[ "$(step "Check the workflows" "${checks}")" != $'      - name: Check the workflows\n        run: bash scripts/check-workflows.sh' ]]; then
        problem "latest.yml's checks job must check out the commit it runs from, with its whole history and no other ref (a step named \"Check out the commit this runs from\"), and run scripts/check-workflows.sh on it (\"Check the workflows\")"
    fi

    holds "${checks}" "latest.yml's checks job must check that the commit is on main" \
        'status=$(gh api "repos/$GH_REPO/compare/$COMMIT...main" --jq .status 2>/dev/null || true)' \
        'refuse: if [[ "$status" != ahead && "$status" != identical ]]; then'
    holds "$(step "Check the commit's Build run" "${checks}")" \
        "latest.yml's checks job must check that the commit's Build run from its push to main succeeded, smoke test included" \
        'run=$(gh api -X GET "repos/$GH_REPO/actions/workflows/build.yml/runs" \' \
        '-f head_sha="$COMMIT" -f event=push -f branch=main -f per_page=1 \' \
        'refuse: if [[ -z "$run" ]]; then' \
        'conclusion=$(gh api "repos/$GH_REPO/actions/runs/$run" --jq '\''.conclusion // empty'\'')' \
        'refuse: if [[ "$conclusion" != success ]]; then' \
        'smoke=$(gh api "repos/$GH_REPO/actions/runs/$run/jobs" \' \
        '--jq '\''[.jobs[] | select(.name == "image") | .steps[] | select(.name == "Smoke test") | .conclusion] | join(" ")'\'')' \
        'refuse: if [[ "$smoke" != success ]]; then'
    holds "$(step "Check the commit's Test run" "${checks}")" \
        "latest.yml's checks job must check that the commit's newest Test run, from its push to main or by hand without the listings cross-test, succeeded" \
        'run=$(gh api -X GET "repos/$GH_REPO/actions/workflows/test.yml/runs" \' \
        '-f head_sha="$COMMIT" -f per_page=100 \' \
        "${TEST_RUN_PICK}" \
        'refuse: if [[ -z "$run" ]]; then' \
        'conclusion=$(gh api "repos/$GH_REPO/actions/runs/$run" --jq '\''.conclusion // empty'\'')' \
        'refuse: if [[ "$conclusion" != success ]]; then'

    # The digest is the image the commit's push Build run pushed: by the
    # attestation that run made, signed for build.yml on main at the commit
    # by a run a push started (the certificate's trigger, which GitHub's
    # token sets), which doesn't expire, or by the digest the run recorded,
    # which must be the digest while it is kept. The commit's tag in the
    # registry is only noted.
    holds "$(step "Check the digest" "${checks}")" \
        "latest.yml's checks job must check that the digest is the image the commit's Build run from its push to main pushed, by its attestation or its record" \
        'RUN: ${{ steps.build_run.outputs.run }}' \
        'attested=$(gh attestation verify "oci://$IMAGE@$DIGEST" --repo "$GH_REPO" \' \
        '--signer-workflow "$GH_REPO/.github/workflows/build.yml" \' \
        '--source-ref refs/heads/main --source-digest "$COMMIT" --format json \' \
        '--jq '\''[.[].verificationResult.signature.certificate | select(.buildTrigger == "push") | .runInvocationURI] | first // empty'\'' || true)' \
        '--jq '\''[.artifacts[] | select(.name == "storage-node-digest" and (.expired | not))] | length'\'')' \
        'if ((kept > 0)); then' \
        'gh run download "$RUN" --name storage-node-digest --dir "$RUNNER_TEMP/digest"' \
        'recorded=$(cat "$RUNNER_TEMP/digest/storage-node-digest.txt")' \
        'refuse: if [[ -n "$recorded" && "$recorded" != "$DIGEST" ]]; then' \
        'refuse: if [[ -z "$attested" && "$recorded" != "$DIGEST" ]]; then'

    # The image's node: read from the image, a published release's, and
    # the node's code changes since that release's tag in the summary, for
    # the approval.
    version_step=$(step "Read the image's node version" "${checks}")
    holds "${version_step}" "latest.yml's checks job must read the node version from the image" \
        'version=$(docker run --rm --entrypoint /opt/ant/ant-node "$IMAGE@$DIGEST" --version)'
    holds "${version_step}" "latest.yml's checks job must refuse an image whose node is not a published release's" \
        'refuse: if [[ ! "$version" =~ ^ant-node\ ([0-9]+\.[0-9]+\.[0-9]+\+ds\.[1-9][0-9]*)$ ]]; then' \
        'tag="v${BASH_REMATCH[1]}"' \
        'published=$(gh release view "$tag" --json isDraft,isPrerelease --jq '\''.isDraft or .isPrerelease'\'' 2>/dev/null || true)' \
        'refuse: if [[ "$published" != false ]]; then'
    holds "${version_step}" "latest.yml's checks job must write the node's code changes since its release's tag to the summary" \
        'released=$(git rev-parse -q --verify "refs/tags/$tag^{commit}" || true)' \
        'refuse: if [[ ! "$released" =~ ^[0-9a-f]{40}$ ]]; then' \
        'changes=$(git diff --stat "$released" "$COMMIT" -- src vendor Cargo.toml Cargo.lock build.rs)' \
        'echo "${changes:-no change}"' \
        '} >> "$GITHUB_STEP_SUMMARY"'

    # A withdrawal goes back to an image :latest held before: the commit of
    # a v*+ds.* tag whose Release run succeeded, or one an earlier Latest
    # run moved :latest to.
    holds "$(step "Check a withdrawal goes back to an image latest had" "${checks}")" \
        "latest.yml's checks job must check that a withdrawal goes back to an image :latest held before" \
        'if [[ "$WITHDRAW" == true ]]; then' \
        'tags=$(git tag --points-at "$COMMIT" --list '\''v*+ds.*'\'')' \
        'releases=$(gh api -X GET "repos/$GH_REPO/actions/workflows/release.yml/runs" \' \
        '-f head_sha="$COMMIT" -f event=push -f status=success -f per_page=100 \' \
        'moved=$(gh api -X GET "repos/$GH_REPO/actions/workflows/latest.yml/runs" \' \
        '-f status=success -f per_page=100 \' \
        'if [[ -n "$tag" ]] && grep -qxF -- "$tag" <<<"$tags"; then' \
        'if [[ "$title" == "Point latest at $COMMIT" || "$title" == "Withdraw latest to $COMMIT" ]]; then' \
        'held=${by_release:-$by_latest}' \
        'refuse: if [[ -z "$held" ]]; then'

    # The latest job: approved in the release environment, after the checks
    # job, and able to read contents and write packages alone.
    latest=$(job latest "${LATEST}")
    if ! grep -qx '    environment: release' <<<"${latest}"; then
        problem "latest.yml's latest job must run in the release environment"
    fi
    if ! needs latest "${LATEST}" | grep -qx checks; then
        problem "latest.yml's latest job must need the checks job"
    fi
    if [[ "$(job_permissions latest "${LATEST}" | LC_ALL=C sort | tr '\n' ' ')" != "contents: read packages: write " ]]; then
        problem "latest.yml's latest job must ask for contents: read and packages: write and nothing else; it asks for: $(job_permissions latest "${LATEST}" | tr '\n' ' ')"
    fi
    # A newer node goes to :latest only with its release; an older one only
    # in a withdrawal. Versions are ordered as nodes order them.
    holds "${latest}" "latest.yml's latest job must refuse an image whose node is newer than :latest's, or older unless the run is a withdrawal" \
        'VERSION: ${{ needs.checks.outputs.version }}' \
        'WITHDRAW: ${{ inputs.withdraw }}' \
        'current=$(docker buildx imagetools inspect "$IMAGE:latest" --format '\''{{json .Manifest}}'\'' | jq -r .digest)' \
        'current_version=$(docker run --rm --entrypoint /opt/ant/ant-node "$IMAGE@$current" --version)' \
        'if ! order=$(jq -rn --arg new "$VERSION" --arg current "$current_version" '\''' \
        'refuse: if [[ "$order" == "newer than" ]]; then' \
        'refuse: if [[ "$order" == "older than" && "$WITHDRAW" != true ]]; then'
    if [[ "$(version_order "${latest}")" != "${NODE_VERSION_ORDER}" ]]; then
        problem "latest.yml's latest job must order node versions as nodes do (the jq program after --arg current \"\$current_version\" must be scripts/check-workflows.sh's NODE_VERSION_ORDER)"
    fi
    # And, unless the run is a withdrawal, an image built from the commit
    # :latest's image was built from, or a later one: the commit its image
    # marker names, or, for an image with none, which only a release put on
    # :latest, the commit of that release's tag.
    holds "${latest}" "latest.yml's latest job must refuse, unless the run is a withdrawal, an image built from a commit before the one :latest's image was built from" \
        'GH_REPO: ${{ github.repository }}' \
        'COMMIT: ${{ inputs.commit }}' \
        'marker=$(docker run --rm --entrypoint cat "$IMAGE@$current" /opt/ant/image-version 2>/dev/null || true)' \
        'if [[ "$marker" =~ ^[0-9a-f]{40}$ ]]; then' \
        'base=$marker' \
        'base=$(gh api "repos/$GH_REPO/commits/refs/tags/v${current_version#ant-node }" --jq .sha 2>/dev/null || true)' \
        'relation=$(gh api "repos/$GH_REPO/compare/$base...$COMMIT" --jq .status 2>/dev/null || true)' \
        'refuse: if [[ "$relation" != ahead && "$relation" != identical && "$WITHDRAW" != true ]]; then'
    # :latest moves once, to the digest the checks job checked, and is
    # checked; every other check comes before the move, and so does the
    # step summary naming the digest :latest had, for a later run to move it
    # back, and the new one.
    if ! grep -qF 'DIGEST: ${{ inputs.digest }}' <<<"${latest}"; then
        problem "latest.yml's latest job must move :latest to the digest the checks job checked (missing: DIGEST: \${{ inputs.digest }})"
    fi
    text=$(sed 's/^[[:space:]]*//' <<<"${latest}")
    moved=$(grep -nxF -- "${MOVE_LATEST}" <<<"${text}" | cut -d: -f1 | tr '\n' ' ' || true)
    checked=$(grep -nxF -- 'if [[ "$now" != "$DIGEST" ]]; then' <<<"${text}" | cut -d: -f1 | tr '\n' ' ' || true)
    if [[ ! "${moved}" =~ ^[0-9]+\ $ || ! "${checked}" =~ ^[0-9]+\ $ ]] || ((moved > checked)); then
        problem "latest.yml's latest job must move :latest once, with exactly '${MOVE_LATEST}', then check it (if [[ \"\$now\" != \"\$DIGEST\" ]]; then)"
    else
        while IFS=: read -r at hit; do
            if ((at > moved && at != checked + 1)); then
                problem "latest.yml's latest job must make every check before it moves :latest, but the move's own check: line ${at} of the job refuses after it: ${hit}"
            fi
        done < <(grep -nF 'echo "::error::' <<<"${text}" || true)
        recorded=$(grep -nE '\$current([^A-Za-z0-9_]|$).*\$DIGEST([^A-Za-z0-9_]|$).*>> "\$GITHUB_STEP_SUMMARY"$' <<<"${text}" \
            | head -n 1 | cut -d: -f1 || true)
        if [[ -z "${recorded}" ]] || ((recorded > moved)); then
            problem "latest.yml's latest job must write the digest :latest had (\$current) and the new one (\$DIGEST) to the step summary before it moves :latest"
        fi
    fi

    # The inputs and what the checks found reach each step only from its
    # job's env, which the lines above pin. No step sets its own, but the
    # checks job's 'Check the digest', which takes the Build run the step
    # before it found, and no step changes a later step's environment
    # (GITHUB_ENV, GITHUB_PATH), so none can give a check or the move
    # another commit, digest, withdraw switch or version.
    while IFS=$'\t' read -r where entry; do
        if [[ "${where}"$'\t'"${entry}" != $'checks: Check the digest\tRUN: ${{ steps.build_run.outputs.run }}' ]]; then
            problem "latest.yml's steps must take their environment from their job's env alone (only the checks job's 'Check the digest' sets RUN: \${{ steps.build_run.outputs.run }}): ${where}: ${entry}"
        fi
    done < <(awk '
        function flush(   i) {
            for (i = 1; i <= n; i++) { print where "\t" entries[i] }
            n = 0
        }
        /^[ \t]*(#|$)/ { next }
        /^[^ ]/ { flush(); in_jobs = ($0 ~ /^jobs:/); in_env = 0; next }
        !in_jobs { next }
        /^  [^ ]/ { flush(); job = $1; sub(/:$/, "", job); in_env = 0; next }
        /^      - / { flush(); name = "a step with no name"; in_env = 0; $0 = "        " substr($0, 9) }
        in_env && /^          / { entry = $0; sub(/^[ \t]+/, "", entry); entries[++n] = entry; next }
        { in_env = 0 }
        /^        name:/ { name = $0; sub(/^        name:[ \t]*/, "", name) }
        /^        env:/ {
            in_env = 1
            entry = $0
            sub(/^        env:[ \t]*/, "", entry)
            if (entry != "") { entries[++n] = entry }
        }
        { where = job ": " name }
        END { flush() }
    ' "${LATEST}")
    while IFS= read -r hit; do
        problem "latest.yml must change no later step's environment (GITHUB_ENV, GITHUB_PATH): ${hit}"
    done < <(grep -nE 'GITHUB_ENV|GITHUB_PATH' "${LATEST}" | grep -vE '^[0-9]+:[[:space:]]*#' || true)
fi

# A job runs only once the jobs it needs succeeded, and a step only once the
# steps before it did. A condition would change that: if: always() on
# release.yml's publish or latest job, or on latest.yml's latest job, runs it
# after a failed need, and if: false on a step of a checks or build job skips
# that check. So would a job or step that continues on error. And a check
# stops its step only if exit 1 follows its ::error::, and only if nothing
# ends the step before it (exit 0, exit, true, : standing alone) or lets a
# failed command pass (set +e, or any set +...). || true after a command is
# how a step reads what may not be there.
for file in "${RELEASE}" "${LATEST}"; do
    [[ -f "${file}" ]] || continue
    name=$(basename "${file}")
    while IFS= read -r hit; do
        problem "${name} must have no if: (a condition can run a job after a failed need, or skip a check): ${hit}"
    done < <(grep -nE '^[[:space:]]*(-[[:space:]]+)?if:' "${file}" || true)
    while IFS= read -r hit; do
        problem "${name} must not continue on error: ${hit}"
    done < <(grep -nE '^[[:space:]]*(-[[:space:]]+)?continue-on-error:' "${file}" || true)
    while IFS=$'\t' read -r kind at hit; do
        if [[ "${kind}" == error ]]; then
            problem "${name} must follow each ::error:: with exit 1, so the refusal stops its step: line ${at} has '${hit}'"
        else
            problem "${name} must have no line that ends a step early or lets a failure pass (exit 0, exit, true, :, set +...): line ${at}: ${hit}"
        fi
    done < <(awk '
        /^[ \t]*#/ { next }
        { line = $0; sub(/^[ \t]+/, "", line); sub(/[ \t]+$/, "", line) }
        after_error {
            after_error = 0
            if (line != "exit 1") { print "error\t" FNR "\t" line; next }
        }
        index(line, "echo \"::error::") == 1 { after_error = 1; next }
        line == "true" || line == ":" || (line ~ /^exit([ \t]|$)/ && line != "exit 1") || line ~ /^set[ \t]+\+/ {
            print "early\t" FNR "\t" line
        }
    ' "${file}")
done

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
    # binary the image ships is checked for the commit it names (as many of
    # its leading characters as git needed, 7 or more) and for what
    # darwin-client's tests say it leaves out.
    manager=$(job manager "${BUILD}")
    for line in \
        '{ read -r repository; read -r revision; } < docker/manager-revision' \
        'run: cargo build --release --locked -p ant-cli --bin darwin-nodes' \
        'commit=${version##*commit }' \
        'if [[ ! "$commit" =~ ^[0-9a-f]{7,40}$ || "$REVISION" != "$commit"* ]]; then' \
        'for text in WithAutonomi logs.autonomi.com /api/v1/reset /api/v1/logs/forward; do' \
        'run: bash scripts/check-darwin-nodes.sh target/release/darwin-nodes'; do
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
    image=$(job image "${BUILD}")
    if ! needs image "${BUILD}" | grep -qx manager; then
        problem "build.yml's image job must need the manager job"
    fi
    if ! grep -qF -- 'build-args: IMAGE_VERSION=${{ github.sha }}' <<<"${image}"; then
        problem "build.yml's image job must pass the image marker (build-args: IMAGE_VERSION=\${{ github.sha }})"
    fi
    # And it attests the image it pushed, by the digest it recorded: the
    # record of it latest.yml takes once the digest artifact has expired.
    image_permissions=$(job_permissions image "${BUILD}")
    if ! grep -qx 'attestations: write' <<<"${image_permissions}" || ! grep -qx 'id-token: write' <<<"${image_permissions}"; then
        problem "build.yml's image job must be able to attest the image (attestations: write, id-token: write); it asks for: $(tr '\n' ' ' <<<"${image_permissions}")"
    fi
    holds "${image}" "build.yml's image job must attest the image it pushed, by the digest it recorded, in GitHub's attestations API" \
        'echo "digest=$digest" >> "$GITHUB_OUTPUT"' \
        'uses: actions/attest-build-provenance@' \
        'subject-name: ghcr.io/project-dspace/storage-node' \
        'subject-digest: ${{ steps.digest.outputs.digest }}' \
        'push-to-registry: false'
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

# Only the latest jobs of release.yml and latest.yml name the image's
# :latest tag, outside comments.
while IFS= read -r hit; do
    problem "only the latest jobs of release.yml and latest.yml may write the :latest tag: ${hit}"
done < <(awk '
    FNR == 1 { in_jobs = 0; current = "" }
    /^[^ #]/ { in_jobs = ($0 ~ /^jobs:/); current = "" }
    in_jobs && /^  [A-Za-z0-9_-]+:/ { current = $1; sub(/:$/, "", current) }
    /^[ \t]*#/ { next }
    /:latest([^A-Za-z0-9_.-]|$)|value=latest|latest=(true|auto)/ {
        name = FILENAME
        sub(/^.*\//, "", name)
        if (!((name == "release.yml" || name == "latest.yml") && current == "latest")) {
            print FILENAME ":" FNR ": " $0
        }
    }
' "${DIR}"/*.yml)

# The release environment holds the release signing key, and admits runs
# from main for latest.yml as well as release.yml's from v*+ds.* tags. Only
# release.yml's publish job, which signs, and latest.yml's latest job run in
# an environment, and latest.yml reads no secret.
while IFS= read -r hit; do
    problem "only release.yml's publish job and latest.yml's latest job may run in an environment: ${hit}"
done < <(awk '
    FNR == 1 { in_jobs = 0; current = "" }
    /^[^ #]/ { in_jobs = ($0 ~ /^jobs:/); current = "" }
    in_jobs && /^  [A-Za-z0-9_-]+:/ { current = $1; sub(/:$/, "", current) }
    /^[ \t]*#/ { next }
    /^[ \t]+environment:/ {
        name = FILENAME
        sub(/^.*\//, "", name)
        if (!((name == "release.yml" && current == "publish") || (name == "latest.yml" && current == "latest"))) {
            print FILENAME ":" FNR ": " $0
        }
    }
' "${DIR}"/*.yml)
if [[ -f "${LATEST}" ]]; then
    while IFS= read -r hit; do
        problem "latest.yml must read no secret: its latest job runs in the release environment, which holds the release signing key: ${hit}"
    done < <(awk '/^[ \t]*#/ { next } /secrets/ { print FILENAME ":" FNR ": " $0 }' "${LATEST}")
fi

if ((PROBLEMS > 0)); then
    echo "${PROBLEMS} problem(s) in ${DIR}" >&2
    exit 1
fi
echo "${DIR}: build.yml, latest.yml, release.yml and test.yml, every action pinned; release.yml runs for v*+ds.* tags with no default permissions, checks the release key, publishes in the release environment only after the tagged commit's Test and Build runs succeeded, its Test run from the push to main or by hand without the listings cross-test, and moves :latest forward only; latest.yml runs only by hand with no default permissions, checks its own commit with this script, and moves :latest in the release environment only after checking, with nothing that can write, that the run is from main and that the image is the one a commit on main's Build run tested and attested or recorded, from a commit whose Test run succeeded, with a published release's node, to the node :latest has, or an older one or an older image only in a withdrawal, back to an image :latest held, naming :latest's digest in the summary first; neither has a condition, anything continuing on error, or a refusal that doesn't stop its step; test.yml's test job runs every test; build.yml builds the image's node manager with --locked from the pinned darwin-client commit, which is on darwin-client's main, checks it, gives the image its marker, and attests the image; build.yml and release.yml build the node with no feature and check it for test seams before packaging it; only the latest jobs of release.yml and latest.yml move :latest, and only release.yml's publish job and latest.yml's latest job run in an environment."
