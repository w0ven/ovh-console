#!/usr/bin/env bash
set -euo pipefail

# 针对 .github/scripts/docker-release.sh 的全分支确定性自测脚本：
# 覆盖：
# 1. prepare-build: 双标签均缺失 -> skip_build=false 且不异常退出
# 2. prepare-build: 仅版本存在（双架构 revision 匹配）-> 无损补齐 sha-<12> 并 skip_build=true
# 3. prepare-build: 仅 sha-<12> 存在 -> 无损补齐 version 并 skip_build=true
# 4. prepare-build: 两者均存在且 digest 一致 -> 幂等跳过且 create 调用为 0
# 5. prepare-build: 凭据助手报错 "executable file not found" -> 严禁判作 missing，必须报错退出
# 6. prepare-build: 缺少 arm64 revision -> 拒绝复用报错退出
# 7. reconcile-floating: latest 需要更新，但最后一个系列探测遇 503 失败 -> 必须中止且 imagetools create 调用次数严格为 0
# 8. reconcile-floating: 正常收敛 latest 与全部系列（跳过确认为 manifest unknown 的未发布高版本）

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TARGET_SCRIPT="${SCRIPT_DIR}/docker-release.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
SHORT_SHA="aaaaaaaaaaaa"
IMAGE="ghcr.io/w0ven/ovh-console"
DIGEST_38="sha256:3838383838383838383838383838383838383838383838383838383838383838"
DIGEST_37="sha256:3737373737373737373737373737373737373737373737373737373737373737"
DIGEST_OLD="sha256:1010101010101010101010101010101010101010101010101010101010101010"

FAKE_GIT="${TMP}/fake-git"
cat > "$FAKE_GIT" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  rev-parse)
    echo "$MOCK_HEAD_SHA"
    ;;
  show)
    echo "2026-11-22T00:00:00Z"
    ;;
  ls-remote)
    if [[ "$*" == *"--tags"* ]]; then
      printf '%b' "$MOCK_REMOTE_TAGS"
    else
      echo "${MOCK_REMOTE_TAG_SHA}	refs/tags/${MOCK_TAG_NAME}"
    fi
    ;;
  *)
    echo "unexpected git call: $*" >&2
    exit 1
    ;;
esac
EOF
chmod +x "$FAKE_GIT"

FAKE_DOCKER="${TMP}/fake-docker"
cat > "$FAKE_DOCKER" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
# docker buildx imagetools <inspect|create> ...
sub="$3"
if [ "$sub" = "create" ]; then
  echo "$*" >> "$MOCK_CREATE_LOG"
  exit 0
fi
if [ "$sub" = "inspect" ]; then
  ref="$4"
  if [ "${5:-}" = "--format" ]; then
    printf '%s\n' "$MOCK_INSPECT_JSON"
    exit 0
  fi
  # 按 MOCK_BEHAVIOR_FILE 逐行匹配: "<ref>|<rc>|<output>"
  while IFS='|' read -r pat rc msg; do
    [ -z "$pat" ] && continue
    if [ "$pat" = "$ref" ]; then
      if [ "$rc" -eq 0 ]; then
        echo "Digest: ${msg}"
        exit 0
      else
        printf '%b\n' "$msg" >&2
        exit "$rc"
      fi
    fi
  done < "$MOCK_BEHAVIOR_FILE"
  echo "ERROR: ${ref}: manifest unknown" >&2
  exit 1
fi
echo "unexpected docker call: $*" >&2
exit 1
EOF
chmod +x "$FAKE_DOCKER"

VALID_MULTIARCH_JSON="$(printf '{"linux/amd64":{"config":{"Labels":{"org.opencontainers.image.revision":"%s"}}},"linux/arm64":{"config":{"Labels":{"org.opencontainers.image.revision":"%s"}}}}' "$SHA" "$SHA")"

MISSING_ARM64_JSON="$(printf '{"linux/amd64":{"config":{"Labels":{"org.opencontainers.image.revision":"%s"}}}}' "$SHA")"

run_case() {
  local name="$1"
  shift
  echo "==> [CASE] $name"
  "$@"
  echo "    PASS"
}

# Case 1: 双标签均缺失 -> skip_build=false，正常退出 0
run_case "prepare-build: both tags missing -> build" bash -c '
  set -euo pipefail
  out="'"$TMP"'/out1"; : > "$out"
  clog="'"$TMP"'/clog1"; : > "$clog"
  beh="'"$TMP"'/beh1"
  cat > "$beh" <<EOB
'"$IMAGE"':0.4.38|1|ERROR: '"$IMAGE"':0.4.38: manifest unknown
'"$IMAGE"':sha-'"$SHORT_SHA"'|1|ERROR: '"$IMAGE"':sha-'"$SHORT_SHA"': manifest unknown
EOB
  DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
  MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
  MOCK_BEHAVIOR_FILE="$beh" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'""'"' \
  REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
  VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
  GITHUB_OUTPUT="$out" \
  bash "'"$TARGET_SCRIPT"'" prepare-build
  grep -qx "skip_build=false" "$out"
  [ ! -s "$clog" ]
'

# Case 2: 仅版本存在（双架构核验通过）-> 补齐 sha-<12> 且 skip_build=true
run_case "prepare-build: version only -> backfill sha tag" bash -c '
  set -euo pipefail
  out="'"$TMP"'/out2"; : > "$out"
  clog="'"$TMP"'/clog2"; : > "$clog"
  beh="'"$TMP"'/beh2"
  cat > "$beh" <<EOB
'"$IMAGE"':0.4.38|0|'"$DIGEST_38"'
'"$IMAGE"':sha-'"$SHORT_SHA"'|1|ERROR: '"$IMAGE"':sha-'"$SHORT_SHA"': manifest unknown
EOB
  DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
  MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
  MOCK_BEHAVIOR_FILE="$beh" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
  REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
  VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
  GITHUB_OUTPUT="$out" \
  bash "'"$TARGET_SCRIPT"'" prepare-build
  grep -qx "skip_build=true" "$out"
  grep -qx "reused_digest='"$DIGEST_38"'" "$out"
  [ "$(wc -l < "$clog")" -eq 1 ]
  grep -q "sha-'"$SHORT_SHA"'" "$clog"
'

# Case 3: 仅 sha-<12> 存在 -> 补齐 version 且 skip_build=true
run_case "prepare-build: sha only -> backfill version tag" bash -c '
  set -euo pipefail
  out="'"$TMP"'/out3"; : > "$out"
  clog="'"$TMP"'/clog3"; : > "$clog"
  beh="'"$TMP"'/beh3"
  cat > "$beh" <<EOB
'"$IMAGE"':0.4.38|1|ERROR: '"$IMAGE"':0.4.38: manifest unknown
'"$IMAGE"':sha-'"$SHORT_SHA"'|0|'"$DIGEST_38"'
EOB
  DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
  MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
  MOCK_BEHAVIOR_FILE="$beh" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
  REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
  VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
  GITHUB_OUTPUT="$out" \
  bash "'"$TARGET_SCRIPT"'" prepare-build
  grep -qx "skip_build=true" "$out"
  [ "$(wc -l < "$clog")" -eq 1 ]
  grep -q ":0.4.38" "$clog"
'

# Case 4: 两者均存在且 digest 一致 -> 幂等跳过且零 create
run_case "prepare-build: both exist & match -> idempotent zero writes" bash -c '
  set -euo pipefail
  out="'"$TMP"'/out4"; : > "$out"
  clog="'"$TMP"'/clog4"; : > "$clog"
  beh="'"$TMP"'/beh4"
  cat > "$beh" <<EOB
'"$IMAGE"':0.4.38|0|'"$DIGEST_38"'
'"$IMAGE"':sha-'"$SHORT_SHA"'|0|'"$DIGEST_38"'
EOB
  DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
  MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
  MOCK_BEHAVIOR_FILE="$beh" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
  REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
  VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
  GITHUB_OUTPUT="$out" \
  bash "'"$TARGET_SCRIPT"'" prepare-build
  grep -qx "skip_build=true" "$out"
  [ ! -s "$clog" ]
'

# Case 5: 凭据助手报错 "executable file not found" 严禁判作 missing，必须报错退出
run_case "prepare-build: credential helper executable file not found -> abort" bash -c '
  set -euo pipefail
  out="'"$TMP"'/out5"; : > "$out"
  clog="'"$TMP"'/clog5"; : > "$clog"
  beh="'"$TMP"'/beh5"
  cat > "$beh" <<EOB
'"$IMAGE"':0.4.38|1|error getting credentials - err: exec: "docker-credential-desktop": executable file not found in \$PATH
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'""'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected abort on credential helper error" >&2
    exit 1
  fi
  [ ! -s "$clog" ]
'

# Case 6: 缺少 arm64 revision 拒绝复用
run_case "prepare-build: missing arm64 revision -> reject reuse" bash -c '
  set -euo pipefail
  out="'"$TMP"'/out6"; : > "$out"
  clog="'"$TMP"'/clog6"; : > "$clog"
  beh="'"$TMP"'/beh6"
  cat > "$beh" <<EOB
'"$IMAGE"':0.4.38|0|'"$DIGEST_38"'
'"$IMAGE"':sha-'"$SHORT_SHA"'|0|'"$DIGEST_38"'
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$MISSING_ARM64_JSON'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected reject when arm64 revision missing" >&2
    exit 1
  fi
  [ ! -s "$clog" ]
'

# Case 7: reconcile-floating 中 latest 需要更新，但最后一个系列 (0.3) 探测遇 503 失败 -> 必须中止且 create 调用次数严格为 0
run_case "reconcile-floating: last series probe 503 -> zero writes (latest untouched)" bash -c '
  set -euo pipefail
  clog="'"$TMP"'/clog7"; : > "$clog"
  beh="'"$TMP"'/beh7"
  cat > "$beh" <<EOB
'"$IMAGE"':0.3.9|0|'"$DIGEST_37"'
'"$IMAGE"':latest|0|'"$DIGEST_OLD"'
'"$IMAGE"':0.4|0|'"$DIGEST_OLD"'
'"$IMAGE"':0.3|1|ERROR: 503 Service Unavailable from ghcr.io
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_REMOTE_TAGS="'"$SHA"'\trefs/tags/v0.4.38\n'"$SHA"'\trefs/tags/v0.3.9\n" \
    MOCK_BEHAVIOR_FILE="$beh" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
    TARGET_TAG="v0.4.38" VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" \
    IMAGE_LC="'"$IMAGE"'" DIGEST="'"$DIGEST_38"'" \
    bash "'"$TARGET_SCRIPT"'" reconcile-floating 2>/dev/null; then
    echo "expected abort when last series probe fails with 503" >&2
    exit 1
  fi
  [ ! -s "$clog" ]
'

# Case 8: reconcile-floating 正常跳过确认为 manifest unknown 的未发布高版本 v0.4.39 并推进 latest + 0.4
run_case "reconcile-floating: skip confirmed missing v0.4.39 and promote latest + 0.4" bash -c '
  set -euo pipefail
  clog="'"$TMP"'/clog8"; : > "$clog"
  beh="'"$TMP"'/beh8"
  cat > "$beh" <<EOB
'"$IMAGE"':0.4.39|1|ERROR: '"$IMAGE"':0.4.39: manifest unknown
'"$IMAGE"':latest|0|'"$DIGEST_OLD"'
'"$IMAGE"':0.4|0|'"$DIGEST_OLD"'
EOB
  DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
  MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
  MOCK_REMOTE_TAGS="'"$SHA"'\trefs/tags/v0.4.39\n'"$SHA"'\trefs/tags/v0.4.38\n" \
  MOCK_BEHAVIOR_FILE="$beh" MOCK_CREATE_LOG="$clog" \
  TARGET_TAG="v0.4.38" VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" \
  IMAGE_LC="'"$IMAGE"'" DIGEST="'"$DIGEST_38"'" \
  bash "'"$TARGET_SCRIPT"'" reconcile-floating
  [ "$(wc -l < "$clog")" -eq 2 ]
  grep -q ":latest" "$clog"
  grep -q ":0.4" "$clog"
'

# Case 9: /token 端点返回 404 Not Found 严禁误判为 missing（无论 prepare-build 还是 reconcile-floating 都必须中止且零写入）
run_case "prepare-build & reconcile-floating: /token 404 Not Found -> abort with zero writes" bash -c '
  set -euo pipefail
  out="'"$TMP"'/out9"; : > "$out"
  clog="'"$TMP"'/clog9"; : > "$clog"
  beh="'"$TMP"'/beh9"
  cat > "$beh" <<EOB
'"$IMAGE"':0.4.38|1|ERROR: failed to resolve '"$IMAGE"':0.4.38: failed to fetch anonymous token: unexpected status from GET request to https://ghcr.io/token: 404 Not Found
'"$IMAGE"':sha-'"$SHORT_SHA"'|1|ERROR: '"$IMAGE"':sha-'"$SHORT_SHA"': manifest unknown
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'""'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected prepare-build abort on /token 404" >&2
    exit 1
  fi
  [ ! -s "$clog" ]

  beh9b="'"$TMP"'/beh9b"
  cat > "$beh9b" <<EOB
'"$IMAGE"':0.4.39|1|ERROR: failed to resolve '"$IMAGE"':0.4.39: failed to fetch anonymous token: unexpected status from GET request to https://ghcr.io/token: 404 Not Found
'"$IMAGE"':latest|0|'"$DIGEST_OLD"'
'"$IMAGE"':0.4|0|'"$DIGEST_OLD"'
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_REMOTE_TAGS="'"$SHA"'\trefs/tags/v0.4.39\n'"$SHA"'\trefs/tags/v0.4.38\n" \
    MOCK_BEHAVIOR_FILE="$beh9b" MOCK_CREATE_LOG="$clog" \
    TARGET_TAG="v0.4.38" VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" \
    IMAGE_LC="'"$IMAGE"'" DIGEST="'"$DIGEST_38"'" \
    bash "'"$TARGET_SCRIPT"'" reconcile-floating 2>/dev/null; then
    echo "expected reconcile-floating abort on /token 404" >&2
    exit 1
  fi
  [ ! -s "$clog" ]
'

# Case 10: exec: command not found 即使混杂 manifest unknown 字样也优先归类故障；版本号含 500（如 1.0.500）的真实 manifest unknown 不被误杀
run_case "probe-classify: exec command not found -> abort, while 1.0.500 manifest unknown -> missing" bash -c '
  set -euo pipefail
  out="'"$TMP"'/out10a"; : > "$out"
  clog="'"$TMP"'/clog10"; : > "$clog"
  beh10a="'"$TMP"'/beh10a"
  cat > "$beh10a" <<EOB
'"$IMAGE"':0.4.38|1|exec: helper: command not found (manifest unknown)
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh10a" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'""'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected abort on exec command not found" >&2
    exit 1
  fi

  out10b="'"$TMP"'/out10b"; : > "$out10b"
  beh10b="'"$TMP"'/beh10b"
  cat > "$beh10b" <<EOB
'"$IMAGE"':1.0.500|1|ERROR: '"$IMAGE"':1.0.500: manifest unknown
'"$IMAGE"':sha-'"$SHORT_SHA"'|1|ERROR: unexpected status from GET request to https://ghcr.io/v2/w0ven/ovh-console/manifests/sha-'"$SHORT_SHA"': 404 Not Found
EOB
  DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
  MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v1.0.500" \
  MOCK_BEHAVIOR_FILE="$beh10b" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'""'"' \
  REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v1.0.500" \
  VERSION="1.0.500" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
  GITHUB_OUTPUT="$out10b" \
  bash "'"$TARGET_SCRIPT"'" prepare-build
  grep -qx "skip_build=false" "$out10b"
'

# Case 11: verify-pushed 验证推送后的双架构 digest 与 revision 落地核验（正常通过，缺少 arm64 拒绝）
run_case "verify-pushed: validates immutable tags & multi-arch revision after push" bash -c '
  set -euo pipefail
  beh11="'"$TMP"'/beh11"
  cat > "$beh11" <<EOB
'"$IMAGE"':0.4.38|0|'"$DIGEST_38"'
'"$IMAGE"':sha-'"$SHORT_SHA"'|0|'"$DIGEST_38"'
EOB
  DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
  MOCK_BEHAVIOR_FILE="$beh11" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
  REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" \
  VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" DIGEST="'"$DIGEST_38"'" \
  bash "'"$TARGET_SCRIPT"'" verify-pushed

  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_BEHAVIOR_FILE="$beh11" MOCK_INSPECT_JSON='"'$MISSING_ARM64_JSON'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" DIGEST="'"$DIGEST_38"'" \
    bash "'"$TARGET_SCRIPT"'" verify-pushed 2>/dev/null; then
    echo "expected verify-pushed failure when arm64 revision missing" >&2
    exit 1
  fi
'

# Case 12: 严格边界校验 —— 其他标签 404 (0.4.380)、正则点号伪匹配 (0x4x38: not found)、异标签 manifest unknown 均必须中止且零写入
run_case "probe-classify: reject mismatched tag 404 (0.4.380), regex-dot spoof (0x4x38), and foreign manifest unknown" bash -c '
  set -euo pipefail
  clog="'"$TMP"'/clog12"; : > "$clog"

  # 12a: 0.4.380 的 404 不能误判为 0.4.38 缺失
  out12a="'"$TMP"'/out12a"; : > "$out12a"
  beh12a="'"$TMP"'/beh12a"
  cat > "$beh12a" <<EOB
'"$IMAGE"':0.4.38|1|ERROR: unexpected status from HEAD request to https://ghcr.io/v2/w0ven/ovh-console/manifests/0.4.380: 404 Not Found
'"$IMAGE"':sha-'"$SHORT_SHA"'|1|ERROR: '"$IMAGE"':sha-'"$SHORT_SHA"': manifest unknown
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh12a" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'""'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out12a" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected abort on 0.4.380 404 prefix spoof" >&2
    exit 1
  fi
  if ! grep -qx "skip_build=false" "$out12a" && [ ! -s "$clog" ]; then :; else echo "unexpected skip_build or write on 12a" >&2; exit 1; fi

  # 12b: 0x4x38: not found 不能因正则点号通配符误判为 0.4.38 缺失
  out12b="'"$TMP"'/out12b"; : > "$out12b"
  beh12b="'"$TMP"'/beh12b"
  cat > "$beh12b" <<EOB
'"$IMAGE"':0.4.38|1|ERROR: '"$IMAGE"':0x4x38: not found
'"$IMAGE"':sha-'"$SHORT_SHA"'|1|ERROR: '"$IMAGE"':sha-'"$SHORT_SHA"': manifest unknown
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh12b" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'""'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out12b" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected abort on regex-dot 0x4x38 spoof" >&2
    exit 1
  fi
  if ! grep -qx "skip_build=false" "$out12b" && [ ! -s "$clog" ]; then :; else echo "unexpected skip_build or write on 12b" >&2; exit 1; fi

  # 12c: 无关 ref 的 manifest unknown 不能误判为目标 0.4.38 缺失
  out12c="'"$TMP"'/out12c"; : > "$out12c"
  beh12c="'"$TMP"'/beh12c"
  cat > "$beh12c" <<EOB
'"$IMAGE"':0.4.38|1|ERROR: '"$IMAGE"':0.4.380: manifest unknown
'"$IMAGE"':sha-'"$SHORT_SHA"'|1|ERROR: '"$IMAGE"':sha-'"$SHORT_SHA"': manifest unknown
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh12c" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'""'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out12c" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected abort on foreign tag manifest unknown" >&2
    exit 1
  fi
  if ! grep -qx "skip_build=false" "$out12c" && [ ! -s "$clog" ]; then :; else echo "unexpected skip_build or write on 12c" >&2; exit 1; fi

  # 12d: 同一条诊断中同时出现目标 ref (0.4.38) 与另一标签 (0.4.380: manifest unknown) 属于归属冲突，
  # 无论 SHA 缺失还是 SHA 存在（企图回填版本标签），均必须中止且零写入！
  out12d="'"$TMP"'/out12d"; : > "$out12d"
  beh12d="'"$TMP"'/beh12d"
  cat > "$beh12d" <<EOB
'"$IMAGE"':0.4.38|1|ERROR: failed to resolve '"$IMAGE"':0.4.38: '"$IMAGE"':0.4.380: manifest unknown
'"$IMAGE"':sha-'"$SHORT_SHA"'|0|'"$DIGEST_38"'
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh12d" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out12d" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected abort on dual-tag attribution conflict (0.4.38 + 0.4.380: manifest unknown)" >&2
    exit 1
  fi
  if ! grep -qx "skip_build=false" "$out12d" && [ ! -s "$clog" ]; then :; else echo "unexpected skip_build or write on 12d" >&2; exit 1; fi

  # 12e: 同一条诊断中同时出现目标 manifest 路径与另一路径的 404，必须因路径归属冲突中止且零写入！
  out12e="'"$TMP"'/out12e"; : > "$out12e"
  beh12e="'"$TMP"'/beh12e"
  cat > "$beh12e" <<EOB
'"$IMAGE"':0.4.38|1|ERROR: /v2/w0ven/ovh-console/manifests/0.4.38 wrapped by GET https://ghcr.io/v2/w0ven/ovh-console/manifests/0.4.380: 404 Not Found
'"$IMAGE"':sha-'"$SHORT_SHA"'|1|ERROR: '"$IMAGE"':sha-'"$SHORT_SHA"': manifest unknown
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh12e" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'""'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out12e" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected abort on dual-manifest-path conflict" >&2
    exit 1
  fi
  if ! grep -qx "skip_build=false" "$out12e" && [ ! -s "$clog" ]; then :; else echo "unexpected skip_build or write on 12e" >&2; exit 1; fi

  # 12f: 跨仓库 MANIFEST_UNKNOWN 夹带目标 ref（MANIFEST_UNKNOWN: ghcr.io/acme/base:old; ref=...:0.4.38）
  # 即使 SHA 存在且双架构合法，也严禁判为 missing 或回填版本标签！
  out12f="'"$TMP"'/out12f"; : > "$out12f"
  beh12f="'"$TMP"'/beh12f"
  cat > "$beh12f" <<EOB
'"$IMAGE"':0.4.38|1|MANIFEST_UNKNOWN: ghcr.io/acme/base:old; ref='"$IMAGE"':0.4.38
'"$IMAGE"':sha-'"$SHORT_SHA"'|0|'"$DIGEST_38"'
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh12f" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out12f" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected abort on cross-repo MANIFEST_UNKNOWN attribution conflict" >&2
    exit 1
  fi
  if ! grep -qx "skip_build=false" "$out12f" && [ ! -s "$clog" ]; then :; else echo "unexpected skip_build or write on 12f" >&2; exit 1; fi

  # 12g: 错误 registry 主机的 404（https://registry.example.invalid/v2/w0ven/ovh-console/manifests/0.4.38: 404 Not Found）
  # 严禁丢弃主机名将其当成 ghcr.io 的缺失！
  out12g="'"$TMP"'/out12g"; : > "$out12g"
  beh12g="'"$TMP"'/beh12g"
  cat > "$beh12g" <<EOB
'"$IMAGE"':0.4.38|1|https://registry.example.invalid/v2/w0ven/ovh-console/manifests/0.4.38: 404 Not Found
'"$IMAGE"':sha-'"$SHORT_SHA"'|0|'"$DIGEST_38"'
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh12g" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out12g" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected abort on mismatched registry host 404" >&2
    exit 1
  fi
  if ! grep -qx "skip_build=false" "$out12g" && [ ! -s "$clog" ]; then :; else echo "unexpected skip_build or write on 12g" >&2; exit 1; fi

  # 12h: 合法单层仓库引用冲突（含目标 ref 的标准缺失诊断 + 另一单层仓库 registry.example.invalid/base:old），
  # 即使 SHA 存在且双架构合法，也严禁判为 missing 或回填版本标签！
  out12h="'"$TMP"'/out12h"; : > "$out12h"
  beh12h="'"$TMP"'/beh12h"
  cat > "$beh12h" <<EOB
'"$IMAGE"':0.4.38|1|ERROR: '"$IMAGE"':0.4.38: manifest unknown (caused by registry.example.invalid/base:old)
'"$IMAGE"':sha-'"$SHORT_SHA"'|0|'"$DIGEST_38"'
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_HEAD_SHA="'"$SHA"'" MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_BEHAVIOR_FILE="$beh12h" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
    REGISTRY="ghcr.io" IMAGE_NAME="w0ven/ovh-console" TARGET_TAG="v0.4.38" \
    VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" REPO_URL="https://github.com/w0ven/ovh-console" \
    GITHUB_OUTPUT="$out12h" \
    bash "'"$TARGET_SCRIPT"'" prepare-build 2>/dev/null; then
    echo "expected abort on single-segment repo conflict (registry.example.invalid/base:old)" >&2
    exit 1
  fi
  if ! grep -qx "skip_build=false" "$out12h" && [ ! -s "$clog" ]; then :; else echo "unexpected skip_build or write on 12h" >&2; exit 1; fi

  # 12i: localhost:5000/base:old 与 IPv4 registry（127.0.0.1:5000/base:old）引用冲突，
  # 即使诊断第一行出现目标 ref 的 not found，也绝不能判为 missing 或跳过高版本！
  beh12i="'"$TMP"'/beh12i"
  cat > "$beh12i" <<EOB
'"$IMAGE"':0.4.39|1|ERROR: '"$IMAGE"':0.4.39: not found\nERROR: failed to resolve localhost:5000/base:old
'"$IMAGE"':latest|0|'"$DIGEST_OLD"'
'"$IMAGE"':0.4|0|'"$DIGEST_OLD"'
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_REMOTE_TAGS="'"$SHA"'\trefs/tags/v0.4.39\n'"$SHA"'\trefs/tags/v0.4.38\n" \
    MOCK_BEHAVIOR_FILE="$beh12i" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
    TARGET_TAG="v0.4.38" VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" \
    IMAGE_LC="'"$IMAGE"'" DIGEST="'"$DIGEST_38"'" \
    bash "'"$TARGET_SCRIPT"'" reconcile-floating 2>/dev/null; then
    echo "expected abort on localhost:5000/base:old conflict during reconcile-floating" >&2
    exit 1
  fi
  [ ! -s "$clog" ]

  beh12j="'"$TMP"'/beh12j"
  cat > "$beh12j" <<EOB
'"$IMAGE"':0.4.39|1|ERROR: '"$IMAGE"':0.4.39: manifest unknown\ndetail: 127.0.0.1:5000/base:old failed
'"$IMAGE"':latest|0|'"$DIGEST_OLD"'
'"$IMAGE"':0.4|0|'"$DIGEST_OLD"'
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_REMOTE_TAGS="'"$SHA"'\trefs/tags/v0.4.39\n'"$SHA"'\trefs/tags/v0.4.38\n" \
    MOCK_BEHAVIOR_FILE="$beh12j" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$VALID_MULTIARCH_JSON'"' \
    TARGET_TAG="v0.4.38" VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" \
    IMAGE_LC="'"$IMAGE"'" DIGEST="'"$DIGEST_38"'" \
    bash "'"$TARGET_SCRIPT"'" reconcile-floating 2>/dev/null; then
    echo "expected abort on IPv4 registry 127.0.0.1:5000/base:old conflict" >&2
    exit 1
  fi
  [ ! -s "$clog" ]
'

# Case 13: reconcile-floating 探测到高版本 0.4.39 存在但 arm64 revision 缺失（半成品/未通过核验）-> 必须中止且零浮动标签写入！
run_case "reconcile-floating: reject promoting higher version 0.4.39 with missing arm64 revision (zero writes)" bash -c '
  set -euo pipefail
  clog="'"$TMP"'/clog13"; : > "$clog"
  beh="'"$TMP"'/beh13"
  cat > "$beh" <<EOB
'"$IMAGE"':0.4.39|0|'"$DIGEST_37"'
'"$IMAGE"':latest|0|'"$DIGEST_OLD"'
'"$IMAGE"':0.4|0|'"$DIGEST_OLD"'
EOB
  if DOCKER_CMD="'"$FAKE_DOCKER"'" GIT_CMD="'"$FAKE_GIT"'" PROBE_RETRY_SLEEP=0 \
    MOCK_REMOTE_TAG_SHA="'"$SHA"'" MOCK_TAG_NAME="v0.4.38" \
    MOCK_REMOTE_TAGS="'"$SHA"'\trefs/tags/v0.4.39\n'"$SHA"'\trefs/tags/v0.4.38\n" \
    MOCK_BEHAVIOR_FILE="$beh" MOCK_CREATE_LOG="$clog" MOCK_INSPECT_JSON='"'$MISSING_ARM64_JSON'"' \
    TARGET_TAG="v0.4.38" VERSION="0.4.38" EXPECTED_SHA="'"$SHA"'" \
    IMAGE_LC="'"$IMAGE"'" DIGEST="'"$DIGEST_38"'" \
    bash "'"$TARGET_SCRIPT"'" reconcile-floating 2>/dev/null; then
    echo "expected abort when higher version 0.4.39 fails multi-arch revision check" >&2
    exit 1
  fi
  [ ! -s "$clog" ]
'

echo "ALL 13 DOCKER-RELEASE SCRIPT CASES PASSED!"
