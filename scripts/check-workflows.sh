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
#   release, or a withdrawal), runs only by hand, with a commit, a digest,
#   a withdraw switch that is off unless set, a node_code that is empty
#   unless set and an allow_unmarked switch that is off unless set, names each run for the digest it moves :latest to, then the
#   commit, grants no permissions unless a job asks for them, and shares
#   release.yml's concurrency group; its steps run in bash, as its defaults
#   set it, take the inputs and what the checks found only from their job's
#   env (no step sets its own but 'Check the digest', which takes the Build
#   run the step before it found, and the withdrawal's check, which takes
#   the record 'Check the digest' took), and change no later step's
#   environment;
# - its checks job can write nothing, and hands the latest job the Build run
#   and the node version alone, each from the step that checked it, which
#   writes it once; refuses a run from any ref but main, inputs other than a
#   full commit, a sha256 digest and an empty or 64-digit node_code, and a
#   run whose latest environment doesn't ask for the approval of exactly
#   one required reviewer, the user its REVIEWER names, which
#   administrators can't bypass, or admits any branch but main; runs this script on the commit it runs from; checks that the
#   commit is on main, that its Build run from its push to main succeeded,
#   smoke test included, and so did its newest Test run, taken as
#   release.yml takes it, and that the digest is the image that Build run
#   pushed, by its build provenance attestation (signed for build.yml on
#   main at the commit, by a run a push started) or by the digest the run
#   recorded, which must be the digest while it is kept, or, for a
#   withdrawal alone, by the line of a v*+ds.* tag on the commit in
#   docker/release-images; reads the image's node version, which must be a
#   published release's, and, unless the run is a withdrawal, refuses any
#   change since that release's tag to what the node is built from unless
#   node_code is that change's fingerprint, made from the git object of
#   each of those paths at the tag and at the commit; and, for a withdrawal,
#   checks that an earlier Latest run from main moved :latest to the digest,
#   by its title, or that the commit is a v*+ds.* tag's whose Release run
#   succeeded;
# - its latest job needs the checks job, runs in the latest environment, can
#   read attestations and contents and write packages and nothing else,
#   refuses an image whose node is newer than :latest's, and, unless the
#   run is a withdrawal, one whose node is older or that was built from a
#   commit before the one :latest's image was built from (its image marker,
#   or for an image with none its node release's tag), refuses an image
#   with no image marker when :latest's has one unless allow_unmarked is
#   set, refuses a withdrawal to an older node until the withdrawn release
#   is deleted, a draft or a pre-release, checks again that the image's
#   node is a published release, orders versions as nodes do, makes every
#   check before it moves :latest, writes :latest's digest and the new one
#   to the step summary, with the command that moves it back only for an
#   image attested or listed in docker/release-images (with allow_unmarked
#   when it must have it), and only then moves :latest, to the checked
#   digest, and checks it;
# - docker/release-images lists exactly the images releases put on :latest
#   before the Build workflow attested its images (RELEASE_IMAGES below);
# - rust-toolchain.toml pins an exact Rust release (channel = "X.Y.Z"), and
#   no workflow builds with another (RUSTUP_TOOLCHAIN, cargo +... or
#   rustc +...), so the node an image release ships is built with its
#   release's compiler, and a compiler change is a change to the node's
#   code that node_code must name;
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
# - only the latest jobs of release.yml and latest.yml, and build.yml's
#   image job, write to the registry: each latest job once, with exactly
#   MOVE_LATEST below, and the image job only with the lines it has to load
#   the image it built, tagged with its commit alone, and push that tag;
#   and nothing else outside the latest jobs names the image's :latest tag;
# - no job but release.yml's publish job and latest.yml's latest job runs
#   in an environment, the publish job in the release environment, which
#   holds the release signing key and admits only v*+ds.* tags, and the
#   latest job in the latest environment, and latest.yml reads no secret.
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
RELEASE_IMAGES_FILE=docker/release-images
PROBLEMS=0

# How the latest jobs move :latest: to an image by its digest, copying the
# manifest as it is.
MOVE_LATEST='docker buildx imagetools create --prefer-index=false --tag "$IMAGE:latest" "$IMAGE@$DIGEST"'

# A line of a job that writes to the registry, or names what a build action
# tags or pushes, its leading spaces left out (an extended regular
# expression): imagetools create, docker push or tag, a build pushed with
# --push or to an output of type registry or image, the build-push action
# and its push and tags inputs, a registry's manifests path, and the other
# tools that copy or tag images.
REGISTRY_WRITE='^(push|tags):|imagetools[ \t]+create|docker[ \t]+((image|buildx|compose)[ \t]+)?(push|tag)([ \t]|$)|--push|type=(registry|image)|push=true|build-push-action|/manifests/|(^|[^A-Za-z0-9_-])(crane|oras|skopeo|regctl|podman|buildah)([^A-Za-z0-9_-]|$)'

# The registry writes build.yml's image job makes: it loads the image it
# builds with the build-push action, tagged with its commit alone, then
# pushes that tag.
IMAGE_BUILD='- uses: docker/build-push-action@'
IMAGE_TAGS='tags: ${{ env.IMAGE }}:${{ github.sha }}'
IMAGE_PUSH='run: docker push "$IMAGE:${{ github.sha }}"'

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

# A Latest run's title: the digest it moves :latest to, then the commit. A
# withdrawal's check matches earlier runs by the digest at its start.
LATEST_RUN_NAME="run-name: \${{ inputs.withdraw && 'Withdraw latest to ' || 'Point latest at ' }}\${{ inputs.digest }} from \${{ inputs.commit }}"

# The image each release put on :latest before the Build workflow attested
# its images, as docker/release-images records it for a withdrawal: the
# release's tag, the commit the image was built from and its digest, as each
# release's Release run moved :latest to it, which the commit's Build run
# recorded and the registry has as storage-node:<commit>. Images built since
# are attested, so the list is complete.
RELEASE_IMAGES=$(cat <<'EOF'
v0.23.0+ds.13 cbd133ba3665b38b81890a6932ae1657d7a32712 sha256:2ae69c3f66c44a16aa77aba712eafdaa3b7e99a3fe75ff46c238a003510b84e9
v0.23.0+ds.14 bbfdf5e4f2ec7b117be4032f3a129c9e51058a6c sha256:70d2647fdc12ce3f2c2ac8a87ab7bf801294049ef405af26dfef4225955487ce
v0.23.0+ds.15 20fbb745d06909a5adee55bcd9fc9113e466ce16 sha256:edd2e1623fa31176f37c99ff6d3e0f40743fba0680f86dc2eac59b5b974e0208
EOF
)

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

# The entries under a key of a job of a workflow (permissions, outputs),
# one a line ("actions: read", or "write-all"), its leading spaces left out.
job_key() {
    job "$1" "$2" | awk -v key="$3" '
        /^    [A-Za-z0-9_-]+:/ { inside = 0 }
        index($0, "    " key ":") == 1 {
            inside = 1
            sub(/^    [A-Za-z0-9_-]+:[[:space:]]*/, "")
            if ($0 != "") { print }
            next
        }
        inside && /^      / { sub(/^[[:space:]]+/, ""); print }
    '
}

# The permissions a job of a workflow asks for, one a line.
job_permissions() {
    job_key "$1" "$2" permissions
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

# A step of latest.yml's checks job that hands on what it checked: one
# problem unless the step's lines (STEP) give it the id ID and write to
# GITHUB_OUTPUT once, with exactly LINE.
hands_on() {
    local text=$1 name=$2 id=$3 line=$4 writes
    writes=$(grep -F 'GITHUB_OUTPUT' <<<"${text}" | sed 's/^[[:space:]]*//' || true)
    if ! grep -qxF -- "        id: ${id}" <<<"${text}" || [[ "${writes}" != "${line}" ]]; then
        problem "latest.yml's checks job must hand on what '${name}' checked, from that step alone: it must have id: ${id} and write to GITHUB_OUTPUT once, with exactly '${line}'"
    fi
}

# The lines of every workflow's jobs that write to the registry
# (REGISTRY_WRITE), outside comments, as "file<TAB>job<TAB>line<TAB>text":
# the text with its leading spaces left out, and a line that ends in \
# joined to the next.
REGISTRY_WRITES=$(awk -v write="${REGISTRY_WRITE}" '
    FNR == 1 { in_jobs = 0; current = ""; pending = "" }
    /^[^ #]/ { in_jobs = ($0 ~ /^jobs:/); current = ""; pending = ""; next }
    !in_jobs || /^[ \t]*#/ { next }
    /^  [A-Za-z0-9_-]+:/ { current = $1; sub(/:$/, "", current) }
    {
        text = $0
        sub(/^[ \t]+/, "", text)
        sub(/[ \t]+$/, "", text)
        if (pending != "") { text = pending " " text; at = pending_at } else { at = FNR }
        if (text ~ /\\$/) {
            pending = substr(text, 1, length(text) - 1)
            sub(/[ \t]+$/, "", pending)
            pending_at = at
            next
        }
        pending = ""
        if (text ~ write) {
            name = FILENAME
            sub(/^.*\//, "", name)
            print name "\t" current "\t" at "\t" text
        }
    }
' "${DIR}"/*.yml)

# The registry writes of a job of a workflow (by file name), their text
# alone, one a line.
writes_of() {
    awk -F '\t' -v file="$1" -v name="$2" '$1 == file && $2 == name { print $4 }' <<<"${REGISTRY_WRITES}"
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
    # withdrawal), and a re-run must not undo that. The job writes to the
    # registry once, to move :latest, and after that check.
    release_latest=$(job latest)
    guard=$(step "${GUARD_STEP}" "${release_latest}")
    holds "${guard}" "release.yml's latest job must check, in a step named \"${GUARD_STEP}\", that :latest's node is older than the release's, or that :latest already has the image" \
        'DIGEST: ${{ steps.image.outputs.digest }}' \
        'current=$(docker buildx imagetools inspect "$IMAGE:latest" --format '\''{{json .Manifest}}'\'' | jq -r .digest)' \
        'if [[ "$current" == "$DIGEST" ]]; then' \
        'current_version=$(docker run --rm --entrypoint /opt/ant/ant-node "$IMAGE@$current" --version)' \
        'if ! order=$(jq -rn --arg new "ant-node $VERSION" --arg current "$current_version" '\''' \
        'refuse: if [[ "$order" != "newer than" ]]; then'
    # The command that withdraws the release sets allow_unmarked when
    # :latest's image has no marker and the release's has one.
    [[ -z "${guard}" ]] || holds "${guard}" "release.yml's latest job must set allow_unmarked in the command that withdraws the release when :latest's image has no image marker and the release's has one" \
        'current_marker=$(docker run --rm --entrypoint cat "$IMAGE@$current" /opt/ant/image-version 2>/dev/null || true)' \
        'release_marker=$(docker run --rm --entrypoint cat "$IMAGE@$DIGEST" /opt/ant/image-version 2>/dev/null || true)' \
        'if [[ ! "$current_marker" =~ ^[0-9a-f]{40}$ && "$release_marker" =~ ^[0-9a-f]{40}$ ]]; then' \
        'back=" -f allow_unmarked=true"' \
        '-f commit=$built_from -f digest=$current -f withdraw=true$back\`"'
    if [[ -n "${guard}" ]] && [[ "$(version_order "${guard}")" != "${NODE_VERSION_ORDER}" ]]; then
        problem "release.yml's latest job must order node versions as latest.yml does (the jq program after --arg current \"\$current_version\" must be scripts/check-workflows.sh's NODE_VERSION_ORDER)"
    fi
    if [[ "$(writes_of release.yml latest)" != "${MOVE_LATEST}" ]]; then
        problem "release.yml's latest job must move :latest once, with exactly '${MOVE_LATEST}', and write to the registry nowhere else"
    elif [[ -n "${guard}" ]]; then
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
    # gets, whether the run is a withdrawal, off unless set, node_code,
    # empty unless set, and allow_unmarked, off unless set. The inputs'
    # descriptions may change.
    expected_on=$(printf '%s\n' "on:" "  workflow_dispatch:" "    inputs:" \
        "      commit:" "        required: true" "        type: string" \
        "      digest:" "        required: true" "        type: string" \
        "      withdraw:" "        type: boolean" "        default: false" \
        "      node_code:" "        type: string" '        default: ""' \
        "      allow_unmarked:" "        type: boolean" "        default: false")
    latest_on=$(section on "${LATEST}" | grep -v '^[[:space:]]*description:' || true)
    if [[ "${latest_on}" != "${expected_on}" ]]; then
        problem "latest.yml must run only by hand (on: workflow_dispatch:), with the inputs commit and digest, required, withdraw, false unless set, node_code, empty unless set, and allow_unmarked, false unless set; it has: $(tr '\n' ' ' <<<"${latest_on}")"
    fi

    # Each run named for the digest it moves :latest to, then the commit: a
    # withdrawal's check matches earlier runs by the digest.
    if [[ "$(section run-name "${LATEST}")" != "${LATEST_RUN_NAME}" ]]; then
        problem "latest.yml must name each run for the digest it moves :latest to, then the commit (${LATEST_RUN_NAME}), as a withdrawal's check matches earlier runs; it has: $(section run-name "${LATEST}" | tr '\n' ' ')"
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
    holds "${checks}" "latest.yml's checks job must take the commit, the digest, the withdraw switch and node_code from the inputs, and refuse anything but a full commit, a sha256 digest and an empty or 64-digit node_code" \
        'COMMIT: ${{ inputs.commit }}' \
        'DIGEST: ${{ inputs.digest }}' \
        'WITHDRAW: ${{ inputs.withdraw }}' \
        'NODE_CODE: ${{ inputs.node_code }}' \
        'refuse: if [[ ! "$COMMIT" =~ ^[0-9a-f]{40}$ ]]; then' \
        'refuse: if [[ ! "$DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]; then' \
        'refuse: if [[ -n "$NODE_CODE" && ! "$NODE_CODE" =~ ^[0-9a-f]{64}$ ]]; then'

    # The latest job runs in the latest environment. GitHub makes an
    # environment a job names when there is none, with no protection, so
    # the checks job refuses a run unless that environment asks for the
    # approval of exactly one required reviewer, the user the job's
    # REVIEWER names (one place, the job's env), which administrators can't
    # bypass, and admits only main. prevent_self_review is deliberately
    # not checked, and the step says so: the one reviewer also dispatches
    # the runs.
    approval_step=$(step "Check the latest environment asks for an approval" "${checks}")
    holds "${approval_step}" \
        "latest.yml's checks job must refuse a run unless the latest environment asks for the approval of one required reviewer, the user REVIEWER names, which administrators can't bypass, and admits only main" \
        'environment=$(gh api "repos/$GH_REPO/environments/latest" \' \
        '--jq '\''[.can_admins_bypass == false, .deployment_branch_policy.custom_branch_policies == true] | all'\'' 2>/dev/null || true)' \
        'branches=$(gh api "repos/$GH_REPO/environments/latest/deployment-branch-policies" \' \
        '--jq '\''[.branch_policies[] | "\(.type) \(.name)"] | join(", ")'\'' 2>/dev/null || true)' \
        'refuse: if [[ "$environment" != true || "$reviewers" != "User $REVIEWER" || "$branches" != "branch main" ]]; then' \
        'reviewers=$(gh api "repos/$GH_REPO/environments/latest" \' \
        '--jq '\''[.protection_rules[]? | select(.type == "required_reviewers") | .reviewers[]? | "\(.type) \(.reviewer.login // .reviewer.slug)"] | join(", ")'\'' 2>/dev/null || true)'
    holds "$(cat "${LATEST}")" \
        "latest.yml's checks job must say, above its approval check, why prevent_self_review is not checked" \
        '# prevent_self_review is deliberately not checked: the one reviewer'
    if [[ $(grep -cE '^      REVIEWER: [A-Za-z0-9](-?[A-Za-z0-9])*$' <<<"${checks}" || true) -ne 1 \
        || $(grep -cE '^[[:space:]]*REVIEWER:' "${LATEST}" || true) -ne 1 ]]; then
        problem "latest.yml's checks job must name the latest environment's one required reviewer once, in its env, as REVIEWER: <GitHub login>, and nothing else may set REVIEWER"
    fi

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
    build_run_step=$(step "Check the commit's Build run" "${checks}")
    holds "${build_run_step}" \
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
    # token sets), by that Build run, any attempt of it (the certificate's
    # run URI), or, for a withdrawal, by any push run of the commit, which
    # doesn't expire, or by the digest the run recorded,
    # which must be the digest while it is kept, or, for a withdrawal alone,
    # by the line in docker/release-images of a v*+ds.* tag on the commit.
    # The step hands on which record it took, for the withdrawal's check.
    # The commit's tag in the registry is only noted.
    digest_step=$(step "Check the digest" "${checks}")
    holds "${digest_step}" \
        "latest.yml's checks job must check that the digest is the image the commit's Build run from its push to main pushed, by its attestation or its record, or, for a withdrawal alone, by docker/release-images" \
        'RUN: ${{ steps.build_run.outputs.run }}' \
        'if [[ "$WITHDRAW" != true ]]; then' \
        'from_run="https://github.com/$GH_REPO/actions/runs/$RUN/"' \
        'from_run="https://github.com/$GH_REPO/actions/runs/"' \
        'attested=$({ gh attestation verify "oci://$IMAGE@$DIGEST" --repo "$GH_REPO" \' \
        '--signer-workflow "$GH_REPO/.github/workflows/build.yml" \' \
        '--source-ref refs/heads/main --source-digest "$COMMIT" --format json || true; } \' \
        '| jq -r --arg from "$from_run" '\''[.[]?.verificationResult.signature.certificate | select(.buildTrigger == "push" and (.runInvocationURI | startswith($from))) | .runInvocationURI] | first // empty'\'' || true)' \
        '--jq '\''[.artifacts[] | select(.name == "storage-node-digest" and (.expired | not))] | length'\'')' \
        'if ((kept > 0)); then' \
        'gh run download "$RUN" --name storage-node-digest --dir "$RUNNER_TEMP/digest"' \
        'recorded=$(cat "$RUNNER_TEMP/digest/storage-node-digest.txt")' \
        'refuse: if [[ -n "$recorded" && "$recorded" != "$DIGEST" ]]; then' \
        'if [[ "$WITHDRAW" == true ]]; then' \
        'tag=$(awk -v commit="$COMMIT" -v digest="$DIGEST" '\''!/^#/ && NF == 3 && $2 == commit && $3 == digest { print $1 }'\'' docker/release-images)' \
        'if [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+\+ds\.[1-9][0-9]*$ && "$(git rev-parse -q --verify "refs/tags/$tag^{commit}" || true)" == "$COMMIT" ]]; then' \
        'listed=$tag' \
        'refuse: if [[ -z "$attested" && "$recorded" != "$DIGEST" && -z "$listed" ]]; then' \
        'if [[ "$recorded" == "$DIGEST" ]]; then' \
        'record=artifact' \
        'elif [[ -n "$listed" ]]; then' \
        'record=listed' \
        'record=attestation'

    # The image's node: read from the image, a published release's, and,
    # unless the run is a withdrawal, refused when what the node is built
    # from changed since that release's tag, unless node_code is the
    # fingerprint of that change: made from the git object each of those
    # paths has at the tag and at the commit, so it names that change alone.
    # The summary lists the changes either way.
    version_step=$(step "Read the image's node version" "${checks}")
    holds "${version_step}" "latest.yml's checks job must read the node version from the image" \
        'version=$(docker run --rm --entrypoint /opt/ant/ant-node "$IMAGE@$DIGEST" --version)'
    holds "${version_step}" "latest.yml's checks job must refuse an image whose node is not a published release's" \
        'refuse: if [[ ! "$version" =~ ^ant-node\ ([0-9]+\.[0-9]+\.[0-9]+\+ds\.[1-9][0-9]*)$ ]]; then' \
        'tag="v${BASH_REMATCH[1]}"' \
        'published=$(gh release view "$tag" --json isDraft,isPrerelease --jq '\''.isDraft or .isPrerelease'\'' 2>/dev/null || true)' \
        'refuse: if [[ "$published" != false ]]; then'
    holds "${version_step}" "latest.yml's checks job must refuse, unless the run is a withdrawal, any change since the node's release to what the node is built from, unless node_code is that change's fingerprint, and list the changes in the summary" \
        'released=$(git rev-parse -q --verify "refs/tags/$tag^{commit}" || true)' \
        'refuse: if [[ ! "$released" =~ ^[0-9a-f]{40}$ ]]; then' \
        'paths=(src vendor Cargo.toml Cargo.lock build.rs rust-toolchain rust-toolchain.toml .cargo)' \
        'for path in "${paths[@]}"; do' \
        'echo "$path $(git rev-parse -q --verify "$1:$path" || echo none)"' \
        'changes=$(git diff --stat "$released" "$COMMIT" -- "${paths[@]}")' \
        'fingerprint=$({ node_code "$released"; node_code "$COMMIT"; } | sha256sum | cut -d'\'' '\'' -f1)' \
        'echo "${changes:-no change}"' \
        '} >> "$GITHUB_STEP_SUMMARY"' \
        'refuse: if [[ "$WITHDRAW" != true && "$(node_code "$released")" != "$(node_code "$COMMIT")" && "$NODE_CODE" != "$fingerprint" ]]; then'

    # A withdrawal goes back to an image :latest held before: one an earlier
    # Latest run from main moved :latest to, matched by the digest its title
    # starts with (run-name above), or the image of a v*+ds.* tag on the
    # commit whose Release run succeeded, noted in the summary when the
    # record of that run's digest has expired.
    holds "$(step "Check a withdrawal goes back to an image latest had" "${checks}")" \
        "latest.yml's checks job must check that a withdrawal goes back to an image :latest held before" \
        'RECORD: ${{ steps.digest.outputs.record }}' \
        'if [[ "$WITHDRAW" == true ]]; then' \
        'tags=$(git tag --points-at "$COMMIT" --list '\''v*+ds.*'\'')' \
        'releases=$(gh api -X GET "repos/$GH_REPO/actions/workflows/release.yml/runs" \' \
        '-f head_sha="$COMMIT" -f event=push -f status=success -f per_page=100 \' \
        'moved=$(gh api -X GET "repos/$GH_REPO/actions/workflows/latest.yml/runs" \' \
        '-f branch=main -f event=workflow_dispatch -f status=success -f per_page=100 \' \
        'if [[ "$title" == "Point latest at $DIGEST"* || "$title" == "Withdraw latest to $DIGEST"* ]]; then' \
        'if [[ -n "$tag" ]] && grep -qxF -- "$tag" <<<"$tags"; then' \
        'held=${by_latest:-$by_release}' \
        'refuse: if [[ -z "$held" ]]; then' \
        'if [[ -z "$by_latest" && "$RECORD" == attestation ]]; then'

    # The checks job hands the latest job the Build run it found and the
    # node version it read, and nothing else, each written once by the step
    # that checked it; 'Check the digest' hands the withdrawal's check the
    # record it took the same way.
    expected_outputs=$(printf '%s\n' \
        'run: ${{ steps.build_run.outputs.run }}' \
        'version: ${{ steps.version.outputs.version }}')
    if [[ "$(job_key checks "${LATEST}" outputs)" != "${expected_outputs}" ]]; then
        problem "latest.yml's checks job must hand on exactly the Build run and the node version its steps checked ($(tr '\n' ' ' <<<"${expected_outputs}")); it has: $(job_key checks "${LATEST}" outputs | tr '\n' ' ')"
    fi
    hands_on "${build_run_step}" "Check the commit's Build run" build_run 'echo "run=$run" >> "$GITHUB_OUTPUT"'
    hands_on "${version_step}" "Read the image's node version" version 'echo "version=$version" >> "$GITHUB_OUTPUT"'
    hands_on "${digest_step}" "Check the digest" digest 'echo "record=$record" >> "$GITHUB_OUTPUT"'

    # The latest job: approved in the latest environment, after the checks
    # job, and able to read attestations and contents and write packages
    # alone.
    latest=$(job latest "${LATEST}")
    if ! grep -qx '    environment: latest' <<<"${latest}"; then
        problem "latest.yml's latest job must run in the latest environment"
    fi
    if ! needs latest "${LATEST}" | grep -qx checks; then
        problem "latest.yml's latest job must need the checks job"
    fi
    if [[ "$(job_permissions latest "${LATEST}" | LC_ALL=C sort | tr '\n' ' ')" != "attestations: read contents: read packages: write " ]]; then
        problem "latest.yml's latest job must ask for attestations: read, contents: read and packages: write and nothing else; it asks for: $(job_permissions latest "${LATEST}" | tr '\n' ' ')"
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
    # The image's node must still be a published release after the
    # approval: a release can be withdrawn while a run waits for it.
    holds "${latest}" "latest.yml's latest job must check again, after the approval, that the image's node is a published release" \
        'release="v${VERSION#ant-node }"' \
        'published_now=$(gh release view "$release" --json isDraft,isPrerelease --jq '\''.isDraft or .isPrerelease'\'' 2>/dev/null || true)' \
        'refuse: if [[ "$published_now" != false ]]; then'
    # A withdrawal to an older node only once the withdrawn release, the one
    # :latest's node belongs to, is deleted, a draft or a pre-release, by
    # GitHub's answer; any other answer, such as an error, refuses it. The
    # summary says so.
    holds "${latest}" "latest.yml's latest job must refuse a withdrawal to an older node until the withdrawn release, :latest's node's, is deleted, a draft or a pre-release" \
        'if [[ "$order" == "older than" ]]; then' \
        'withdrawn="v${current_version#ant-node }"' \
        'if ! withdrawn_state=$(gh api "repos/$GH_REPO/releases/tags/$withdrawn" \' \
        '--jq '\''if .draft or .prerelease then "a draft or a pre-release" else "published" end'\'' 2>"$RUNNER_TEMP/withdrawn.err"); then' \
        'if grep -qF '\''HTTP 404'\'' "$RUNNER_TEMP/withdrawn.err"; then' \
        'withdrawn_state=deleted' \
        'refuse: if [[ "$withdrawn_state" != deleted && "$withdrawn_state" != "a draft or a pre-release" ]]; then' \
        'echo "- The withdrawn release $withdrawn, of $current_version, is $withdrawn_state:'
    # A move from an image with a marker (darwin-nodes as its node manager)
    # to one with none (Autonomi's, which deletes a node's data when its
    # disk runs low) only with allow_unmarked, which the summary shows.
    holds "${latest}" "latest.yml's latest job must refuse, unless allow_unmarked is set, a move from an image with an image marker to one with none" \
        'ALLOW_UNMARKED: ${{ inputs.allow_unmarked }}' \
        'target_marker=$(docker run --rm --entrypoint cat "$IMAGE@$DIGEST" /opt/ant/image-version 2>/dev/null || true)' \
        'refuse: if [[ "$marker" =~ ^[0-9a-f]{40}$ && ! "$target_marker" =~ ^[0-9a-f]{40}$ && "$ALLOW_UNMARKED" != true ]]; then' \
        'echo "- **allow_unmarked is set:'
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
    # The command that moves :latest back goes in the summary only for an
    # image with a record a later withdrawal can use once its Build run's
    # artifact has expired: its attestation, or its line in
    # docker/release-images.
    holds "${latest}" "latest.yml's latest job must give the command that moves :latest back only for an image with a record that doesn't expire: its attestation, or its line in docker/release-images" \
        'attested=$(gh attestation verify "oci://$IMAGE@$current" --repo "$GH_REPO" \' \
        '--signer-workflow "$GH_REPO/.github/workflows/build.yml" \' \
        '--source-ref refs/heads/main --source-digest "$base" --format json \' \
        'listed=$(gh api "repos/$GH_REPO/contents/docker/release-images?ref=$GITHUB_SHA" -H '\''Accept: application/vnd.github.raw+json'\'' || true)' \
        'if [[ -n "$attested" ]]; then' \
        'elif awk -v base="$base" -v current="$current" '\''!/^#/ && NF == 3 && $2 == base && $3 == current { found = 1 } END { exit !found }'\'' <<<"$listed"; then' \
        'if [[ -n "$lasting" ]]; then'
    # The command moving :latest back sets allow_unmarked when the image
    # before has no marker and the new one has one, as it then must.
    holds "${latest}" "latest.yml's latest job must set allow_unmarked in the command that moves :latest back when the image before has no image marker and the new one has one" \
        'if [[ ! "$marker" =~ ^[0-9a-f]{40}$ && "$target_marker" =~ ^[0-9a-f]{40}$ ]]; then' \
        'back=" -f allow_unmarked=true"' \
        '-f commit=$base -f digest=$current -f withdraw=true$back\`"'
    # :latest moves once, to the digest the checks job checked, and is
    # checked; the job writes to the registry nowhere else, every other
    # check comes before the move, and so does the step summary naming the
    # digest :latest had, for a later run to move it back, and the new one.
    if ! grep -qF 'DIGEST: ${{ inputs.digest }}' <<<"${latest}"; then
        problem "latest.yml's latest job must move :latest to the digest the checks job checked (missing: DIGEST: \${{ inputs.digest }})"
    fi
    text=$(sed 's/^[[:space:]]*//' <<<"${latest}")
    moved=$(grep -nxF -- "${MOVE_LATEST}" <<<"${text}" | cut -d: -f1 | tr '\n' ' ' || true)
    checked=$(grep -nxF -- 'if [[ "$now" != "$DIGEST" ]]; then' <<<"${text}" | cut -d: -f1 | tr '\n' ' ' || true)
    if [[ "$(writes_of latest.yml latest)" != "${MOVE_LATEST}" || ! "${moved}" =~ ^[0-9]+\ $ || ! "${checked}" =~ ^[0-9]+\ $ ]] \
        || ((moved > checked)); then
        problem "latest.yml's latest job must move :latest once, with exactly '${MOVE_LATEST}', write to the registry nowhere else, then check it (if [[ \"\$now\" != \"\$DIGEST\" ]]; then)"
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
    # before it found, and the withdrawal's check, which takes the record
    # 'Check the digest' took, and no step changes a later step's
    # environment (GITHUB_ENV, GITHUB_PATH), so none can give a check or the
    # move another commit, digest, withdraw switch, node_code or version.
    while IFS=$'\t' read -r where entry; do
        case "${where}"$'\t'"${entry}" in
            $'checks: Check the digest\tRUN: ${{ steps.build_run.outputs.run }}') ;;
            $'checks: Check a withdrawal goes back to an image latest had\tRECORD: ${{ steps.digest.outputs.record }}') ;;
            *)
                problem "latest.yml's steps must take their environment from their job's env alone (only the checks job's 'Check the digest' sets RUN: \${{ steps.build_run.outputs.run }}, and its withdrawal's check RECORD: \${{ steps.digest.outputs.record }}): ${where}: ${entry}"
                ;;
        esac
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

# Only the latest jobs of release.yml and latest.yml, each once to move
# :latest (checked above), and build.yml's image job write to the registry:
# the image job loads the image it built, tagged with its commit alone, and
# pushes that tag.
while IFS=$'\t' read -r file name at text; do
    [[ -n "${file}" ]] || continue
    case "${file}:${name}" in
        release.yml:latest | latest.yml:latest) continue ;;
        build.yml:image)
            if [[ "${text}" == "${IMAGE_BUILD}"* || "${text}" == "${IMAGE_TAGS}" || "${text}" == "${IMAGE_PUSH}" ]]; then
                continue
            fi
            ;;
    esac
    problem "only release.yml's and latest.yml's latest jobs, which move :latest, and build.yml's image job, which pushes the image as :<commit>, may write to the registry, each with its own lines: ${DIR}/${file}:${at}: ${text}"
done <<<"${REGISTRY_WRITES}"

# Nothing else outside the latest jobs of release.yml and latest.yml names
# the image's :latest tag, outside comments (a line that writes to the
# registry is checked above).
while IFS= read -r hit; do
    problem "only the latest jobs of release.yml and latest.yml may name the image's :latest tag: ${hit}"
done < <(awk -v write="${REGISTRY_WRITE}" '
    FNR == 1 { in_jobs = 0; current = "" }
    /^[^ #]/ { in_jobs = ($0 ~ /^jobs:/); current = "" }
    in_jobs && /^  [A-Za-z0-9_-]+:/ { current = $1; sub(/:$/, "", current) }
    /^[ \t]*#/ { next }
    /:latest([^A-Za-z0-9_.-]|$)|value=latest|latest=(true|auto)/ {
        name = FILENAME
        sub(/^.*\//, "", name)
        text = $0
        sub(/^[ \t]+/, "", text)
        if (!((name == "release.yml" || name == "latest.yml") && current == "latest") && !(in_jobs && text ~ write)) {
            print FILENAME ":" FNR ": " $0
        }
    }
' "${DIR}"/*.yml)

# The release environment holds the release signing key and admits only
# v*+ds.* tags, for release.yml's publish job, which signs; the latest
# environment admits only main, for latest.yml's latest job, which moves
# :latest. No other job runs in an environment, and latest.yml reads no
# secret.
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
        problem "latest.yml must read no secret: it needs none, and the release environment's signing key is release.yml's publish job's alone: ${hit}"
    done < <(awk '/^[ \t]*#/ { next } /secrets/ { print FILENAME ":" FNR ": " $0 }' "${LATEST}")
fi

# The compiler: rust-toolchain.toml pins an exact Rust release, which rustup
# takes over the stable toolchain the workflows install, and no workflow
# builds with another, so an image release's node is built with its
# release's compiler, and a compiler change is a change to the node's code
# (latest.yml's node_code paths hold rust-toolchain.toml).
TOOLCHAIN_FILE=rust-toolchain.toml
if [[ ! -f "${TOOLCHAIN_FILE}" || -e rust-toolchain ]] \
    || [[ $(grep -cE '^[[:space:]]*channel[[:space:]]*=' "${TOOLCHAIN_FILE}" || true) -ne 1 ]] \
    || ! grep -qxE 'channel = "[0-9]+\.[0-9]+\.[0-9]+"' "${TOOLCHAIN_FILE}"; then
    problem "${TOOLCHAIN_FILE} must pin an exact Rust release, once (channel = \"X.Y.Z\"), with no rust-toolchain beside it, so every workflow builds the node with one compiler; it has: $(grep -E 'channel' "${TOOLCHAIN_FILE}" 2>/dev/null | tr '\n' ' ' || true)"
fi
while IFS= read -r hit; do
    problem "no workflow may build with a toolchain other than ${TOOLCHAIN_FILE}'s: ${hit}"
done < <(grep -nE 'RUSTUP_TOOLCHAIN|(cargo|rustc|rustup run) \+' "${DIR}"/*.yml | grep -vE '^[^:]+:[0-9]+:[[:space:]]*#' || true)

# The images releases put on :latest before the Build workflow attested its
# images, which latest.yml takes from docker/release-images for a
# withdrawal: exactly RELEASE_IMAGES, comments and blank lines aside.
listed_images=$(grep -vE '^(#|$)' "${RELEASE_IMAGES_FILE}" 2>/dev/null || true)
if [[ "${listed_images}" != "${RELEASE_IMAGES}" ]]; then
    problem "${RELEASE_IMAGES_FILE} must list exactly the images releases put on :latest before the Build workflow attested its images, each by its tag, its commit and its digest (RELEASE_IMAGES in scripts/check-workflows.sh); it has: $(tr '\n' ' ' <<<"${listed_images:-nothing}")"
fi

if ((PROBLEMS > 0)); then
    echo "${PROBLEMS} problem(s) in ${DIR}" >&2
    exit 1
fi
echo "${DIR}: build.yml, latest.yml, release.yml and test.yml, every action pinned; release.yml runs for v*+ds.* tags with no default permissions, checks the release key, publishes in the release environment only after the tagged commit's Test and Build runs succeeded, its Test run from the push to main or by hand without the listings cross-test, and moves :latest forward only; latest.yml runs only by hand with no default permissions, names each run for its digest, checks its own commit with this script, and moves :latest in the latest environment, once it asks for its one reviewer's approval, only after checking, with nothing that can write, that the run is from main and that the image is the one a commit on main's Build run tested and attested or recorded (for a withdrawal, also listed in ${RELEASE_IMAGES_FILE}), from a commit whose Test run succeeded, with a published release's node whose code changes since its tag node_code names, to the node :latest has, or an older one or an older image only in a withdrawal, back to an image :latest held, to an image without a marker from one with a marker only with allow_unmarked, to an older node only once the withdrawn release is gone, with its release checked again after the approval, naming :latest's digest in the summary first; neither has a condition, anything continuing on error, or a refusal that doesn't stop its step; test.yml's test job runs every test; build.yml builds the image's node manager with --locked from the pinned darwin-client commit, which is on darwin-client's main, checks it, gives the image its marker, and attests the image; build.yml and release.yml build the node with no feature and check it for test seams before packaging it; only the latest jobs of release.yml and latest.yml move :latest, each with one registry write, and build.yml's image job alone pushes other tags; only release.yml's publish job runs in the release environment, and latest.yml's latest job in the latest environment; ${RELEASE_IMAGES_FILE} lists the images releases put on :latest before attestation; ${TOOLCHAIN_FILE} pins an exact Rust release that every workflow builds with."
