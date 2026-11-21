#!/usr/bin/env bash
set -euo pipefail

# 可测试、无 set±e 陷阱的 Docker 发布辅助脚本（供 .github/workflows/docker.yml 调用及本地全分支单测）。
# 支持通过 DOCKER_CMD / GIT_CMD 注入桩脚本进行确定性回归验收。
DOCKER_CMD="${DOCKER_CMD:-docker}"
GIT_CMD="${GIT_CMD:-git}"
PROBE_RETRY_SLEEP="${PROBE_RETRY_SLEEP:-3}"

# resolve_remote_tag_sha <tag>: 从 origin 查询 tag 解引用后的 commit SHA
resolve_remote_tag_sha() {
  local target_tag="$1"
  local remote_raw remote_deref
  remote_raw="$("$GIT_CMD" ls-remote origin "refs/tags/${target_tag}^{}" "refs/tags/${target_tag}")"
  remote_deref="$(printf '%s\n' "$remote_raw" | awk '$2 ~ /\^\{\}$/ {print $1; exit}')"
  if [ -z "$remote_deref" ]; then
    remote_deref="$(printf '%s\n' "$remote_raw" | awk '{print $1; exit}')"
  fi
  printf '%s' "$remote_deref"
}

# is_manifest_missing_error <ref> <stderr_text>:
# 严格判定该错误是否确实归属于“目标 manifest/仓库在远端 registry 不存在”：
# 1. 优先排除任何本机/凭据/token/认证/网络/代理/5xx 故障特征（HTTP 状态码仅按精确上下文匹配，绝不误伤如 1.0.500 的合法版本号）；
# 2. 强制核对 registry 主机、完整仓库路径与标签三元组完全一致（禁止丢弃 registry 主机把 registry.example.invalid 误当 ghcr.io）；
# 3. 冲突检出：若错误中出现任何非目标 registry/repo/tag 引用（含不同仓库如 ghcr.io/acme/base:old 或不同主机 URL），一律判为故障（返回 1）；
# 4. 目标对象与缺失诊断（manifest unknown / name unknown / 404 Not Found / not found）必须直接相邻绑定，不允许中间夹入其他对象。
is_manifest_missing_error() {
  local ref="$1"
  local err="$2"

  # 1) 优先排除明显的本机/凭据/token/认证/网络/代理/5xx 故障特征
  if printf '%s' "$err" | grep -Eqi \
    'executable file not found|command not found|no such file or directory|permission denied|exec:|credential|failed to fetch.*token|/token([?:/[:space:]]|$)|unauthorized|denied|authentication required|forbidden|timeout|timed out|connection refused|temporary failure|no such host|EOF|dial tcp|too many requests|status( code)?[:[:space:]]*(429|500|502|503|504)\b|\b(429|500|502|503|504)[[:space:]]+(Too Many Requests|Internal Server Error|Bad Gateway|Service Unavailable|Gateway Timeout)\b'; then
    return 1
  fi

  # 拆分 ref 为 registry、repo、tag，并构造精确 URL 路径（含 host）和正则转义串
  local ref_reg="" ref_no_reg="" ref_repo="" ref_tag=""
  local expected_path="" expected_host_path="" ref_esc="" host_path_esc=""
  if [[ "$ref" != */* ]]; then
    return 1
  fi
  ref_reg="${ref%%/*}"
  ref_no_reg="${ref#*/}"
  if [[ "$ref_no_reg" == *@* ]]; then
    ref_repo="${ref_no_reg%@*}"
    ref_tag="${ref_no_reg#*@}"
  elif [[ "$ref_no_reg" == *:* ]]; then
    ref_repo="${ref_no_reg%:*}"
    ref_tag="${ref_no_reg##*:}"
  fi
  if [ -z "$ref_reg" ] || [ -z "$ref_repo" ] || [ -z "$ref_tag" ]; then
    return 1
  fi
  expected_path="/v2/${ref_repo}/manifests/${ref_tag}"
  expected_host_path="${ref_reg}${expected_path}"
  ref_esc="$(printf '%s' "$ref" | sed 's/[][(){}.^$*+?|\\]/\\&/g')"
  host_path_esc="$(printf '%s' "$expected_host_path" | sed 's/[][(){}.^$*+?|\\]/\\&/g')"

  # 2) 冲突检出 A：所有出现的 https?://<host>/v2/.../manifests/... URL 必须精确匹配预期 registry + repo + tag
  local found_urls u host_and_path
  found_urls="$(printf '%s' "$err" | grep -Eo 'https?://[A-Za-z0-9._:-]+/v2/[A-Za-z0-9._/-]+/manifests/(sha256:[a-fA-F0-9]+|[A-Za-z0-9._-]+)' | sort -u || true)"
  if [ -n "$found_urls" ]; then
    while IFS= read -r u; do
      [ -z "$u" ] && continue
      host_and_path="${u#http://}"
      host_and_path="${host_and_path#https://}"
      if [ "$host_and_path" != "$expected_host_path" ]; then
        return 1
      fi
    done <<< "$found_urls"
  fi

  # 冲突检出 B：所有出现的相对 /v2/.../manifests/... 路径也必须精确等于 expected_path
  local found_paths p
  found_paths="$(printf '%s' "$err" | grep -Eo '/v2/[A-Za-z0-9._/-]+/manifests/(sha256:[a-fA-F0-9]+|[A-Za-z0-9._-]+)' | sort -u || true)"
  if [ -n "$found_paths" ]; then
    while IFS= read -r p; do
      [ -z "$p" ] && continue
      if [ "$p" != "$expected_path" ]; then
        return 1
      fi
    done <<< "$found_paths"
  fi

  # 冲突检出 C：扫描任意带 registry 主机（域名、IPv4/IPv6、localhost 或 host:port）的镜像引用
  #（覆盖 localhost:5000/base:old、127.0.0.1:5000/base:old、192.168.1.10/base:old、registry.example.invalid/base:old、ghcr.io/acme/base:old 以及 @sha256:... 摘要引用），
  # 凡出现任何非目标 ref 的镜像引用即判归属冲突！
  local found_refs r
  found_refs="$(printf '%s' "$err" | grep -Eo '((localhost|([0-9]{1,3}\.){3}[0-9]{1,3}|\[[0-9a-fA-F:]+\]|[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+)(:[0-9]+)?|[A-Za-z0-9-]+:[0-9]+)/[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*(:[A-Za-z0-9._-]+|@sha256:[a-fA-F0-9]{64})' | sort -u || true)"
  if [ -n "$found_refs" ]; then
    while IFS= read -r r; do
      [ -z "$r" ] && continue
      if [ "$r" != "$ref" ]; then
        return 1
      fi
    done <<< "$found_refs"
  fi

  # 3) 将“目标对象（含 host）”与“缺失诊断”严格直接相邻绑定（不允许中间夹入无关对象）：
  # (a) buildx / docker 固定格式：<ref>: (manifest unknown | name unknown | not found)
  if printf '%s' "$err" | grep -Eqi "(^|[[:space:]\"'\\(])${ref_esc}:[[:space:]]*(manifest unknown|name unknown|MANIFEST_UNKNOWN|NAME_UNKNOWN|not found)([[:space:]\"'\\),;.]|$)"; then
    return 0
  fi

  # (b) HTTP 404 / manifest unknown 直接紧跟带正确 registry 主机的 manifest URL：
  #     https?://<ref_reg>/v2/<ref_repo>/manifests/<ref_tag>: 404 Not Found / manifest unknown
  if printf '%s' "$err" | grep -Eqi "https?://${host_path_esc}([?#][^[:space:]\"']*)?:?[[:space:]]*(404 Not Found|status( code)?[:[:space:]]*404\b|manifest unknown|name unknown|MANIFEST_UNKNOWN|NAME_UNKNOWN)"; then
    return 0
  fi

  # (c) 错误码紧邻目标 ref 或带 host 的 URL（中间只允许冒号与空白，绝不允许夹入另一镜像对象）
  if printf '%s' "$err" | grep -Eqi "(MANIFEST_UNKNOWN|NAME_UNKNOWN|manifest unknown|name unknown):[[:space:]]*(${ref_esc}([:[:space:]\"'\\)]|$)|https?://${host_path_esc}([?:#[:space:]\"'\\)]|$))"; then
    return 0
  fi

  return 1
}

# probe_manifest <ref>:
# 绝不触碰 set ±e；通过 if 捕获返回码，并把结果通过全局变量返回：
#   PROBE_STATE = "found" | "missing" | "error"
#   PROBE_DIGEST = "sha256:..."（仅当 PROBE_STATE == "found"）
probe_manifest() {
  local ref="$1"
  local attempt out rc
  PROBE_STATE="error"
  PROBE_DIGEST=""

  for attempt in 1 2 3; do
    rc=0
    if out="$("$DOCKER_CMD" buildx imagetools inspect "$ref" 2>&1)"; then
      rc=0
    else
      rc=$?
    fi

    if [ "$rc" -eq 0 ]; then
      PROBE_DIGEST="$(printf '%s\n' "$out" | awk '/^Digest:/{print $2; exit}')"
      if [ -z "$PROBE_DIGEST" ]; then
        echo "::error::查到 ${ref} 但未解析到 Digest: $out" >&2
        PROBE_STATE="error"
        return 1
      fi
      PROBE_STATE="found"
      return 0
    fi

    if is_manifest_missing_error "$ref" "$out"; then
      PROBE_STATE="missing"
      return 0
    fi

    echo "::warning::查询 ${ref} 遇到非确定缺失错误（第 ${attempt} 次，rc=${rc}）: $out" >&2
    if [ "$attempt" -lt 3 ] && [ "$PROBE_RETRY_SLEEP" -gt 0 ]; then
      sleep "$((attempt * PROBE_RETRY_SLEEP))"
    fi
  done

  echo "::error::查询 ${ref} 连续 3 次出现非 404 故障，中止操作以避免误覆盖或错误回退" >&2
  PROBE_STATE="error"
  return 1
}

# json_extract_revision <arch>: 从 stdin JSON 中提取 .[arch].config.Labels["org.opencontainers.image.revision"]。
# 优先使用 jq，若环境未装 jq 则自动回退至 python3（两者均在失败或缺失时返回非零）。
json_extract_revision() {
  local arch="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -er --arg a "$arch" '.[$a].config.Labels["org.opencontainers.image.revision"]'
    return $?
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 -c '
import json, sys
arch = sys.argv[1]
data = json.load(sys.stdin)
val = data.get(arch, {}).get("config", {}).get("Labels", {}).get("org.opencontainers.image.revision")
if not isinstance(val, str) or not val:
    sys.exit(1)
print(val)
' "$arch"
    return $?
  fi
  echo "::error::需要 jq 或 python3 以解析镜像多架构 revision 标签" >&2
  return 1
}

# verify_multiarch_revision <digest_ref> <expected_sha>:
# 强制核验 linux/amd64 与 linux/arm64 两个目标架构的 org.opencontainers.image.revision 均非空且等于 expected_sha。
verify_multiarch_revision() {
  local ref="$1"
  local expected_sha="$2"
  local cfg_json arch rev
  cfg_json="$("$DOCKER_CMD" buildx imagetools inspect "$ref" --format '{{json .Image}}')"
  for arch in "linux/amd64" "linux/arm64"; do
    if ! rev="$(printf '%s' "$cfg_json" | json_extract_revision "$arch")"; then
      echo "::error::${ref} 缺少 ${arch} 架构或 org.opencontainers.image.revision 标签，拒绝复用" >&2
      return 1
    fi
    if [ -z "$rev" ] || [ "$rev" != "$expected_sha" ]; then
      echo "::error::${ref} 的 ${arch} revision (${rev:-<empty>}) != 预期提交 (${expected_sha})，拒绝复用或覆盖" >&2
      return 1
    fi
  done
}

cmd_prepare_build() {
  local head_sha remote_deref image_lc short_sha commit_iso
  head_sha="$("$GIT_CMD" rev-parse HEAD)"
  remote_deref="$(resolve_remote_tag_sha "$TARGET_TAG")"
  if [ "$head_sha" != "$EXPECTED_SHA" ] || [ "$remote_deref" != "$EXPECTED_SHA" ]; then
    echo "::error::远端 refs/tags/${TARGET_TAG} (${remote_deref:-<missing>}) 或本地 HEAD (${head_sha}) 偏离 verify SHA (${EXPECTED_SHA})，已中止" >&2
    return 1
  fi

  image_lc="$(printf '%s/%s' "$REGISTRY" "$IMAGE_NAME" | tr '[:upper:]' '[:lower:]')"
  short_sha="$(printf '%s' "$EXPECTED_SHA" | cut -c1-12)"
  commit_iso="$("$GIT_CMD" show -s --format=%cI "$EXPECTED_SHA")"
  echo "image_lc=${image_lc}" >> "$GITHUB_OUTPUT"

  if ! probe_manifest "${image_lc}:${VERSION}"; then
    return 1
  fi
  local ver_state="$PROBE_STATE"
  local ver_digest="$PROBE_DIGEST"

  if ! probe_manifest "${image_lc}:sha-${short_sha}"; then
    return 1
  fi
  local sha_state="$PROBE_STATE"
  local sha_digest="$PROBE_DIGEST"

  # 情况 A：版本标签已存在 -> 严格校验双架构 revision 与 sha-<12> 一致性后幂等复用
  if [ "$ver_state" = "found" ]; then
    verify_multiarch_revision "${image_lc}@${ver_digest}" "$EXPECTED_SHA"
    if [ "$sha_state" = "found" ]; then
      if [ "$sha_digest" != "$ver_digest" ]; then
        echo "::error::已存在 ${image_lc}:sha-${short_sha} (${sha_digest}) 与 ${image_lc}:${VERSION} (${ver_digest}) digest 不一致，拒绝覆盖" >&2
        return 1
      fi
    else
      "$DOCKER_CMD" buildx imagetools create -t "${image_lc}:sha-${short_sha}" "${image_lc}@${ver_digest}"
    fi
    {
      echo "reused_digest=${ver_digest}"
      echo "skip_build=true"
    } >> "$GITHUB_OUTPUT"
    echo "双架构核验通过，幂等复用不可变镜像 ${image_lc}:${VERSION} -> ${ver_digest}"
    return 0
  fi

  # 情况 B：版本标签确认缺失，但 sha-<12> 已存在 -> 校验双架构 revision 后直接无损打上版本标签
  if [ "$sha_state" = "found" ]; then
    verify_multiarch_revision "${image_lc}@${sha_digest}" "$EXPECTED_SHA"
    "$DOCKER_CMD" buildx imagetools create -t "${image_lc}:${VERSION}" "${image_lc}@${sha_digest}"
    {
      echo "reused_digest=${sha_digest}"
      echo "skip_build=true"
    } >> "$GITHUB_OUTPUT"
    echo "双架构核验通过，复用已有 ${image_lc}:sha-${short_sha} (${sha_digest}) 创建 ${image_lc}:${VERSION}"
    return 0
  fi

  # 情况 C：两者均严格确认为 manifest missing -> 放行首次多架构构建
  {
    echo "reused_digest="
    echo "skip_build=false"
    echo "tags<<EOF"
    echo "${image_lc}:sha-${short_sha}"
    echo "${image_lc}:${VERSION}"
    echo "EOF"
    echo "labels<<EOF"
    echo "org.opencontainers.image.source=${REPO_URL}"
    echo "org.opencontainers.image.revision=${EXPECTED_SHA}"
    echo "org.opencontainers.image.version=${VERSION}"
    echo "org.opencontainers.image.created=${commit_iso}"
    echo "org.opencontainers.image.title=ovh-console"
    echo "EOF"
  } >> "$GITHUB_OUTPUT"
}

cmd_reconcile_floating() {
  local remote_deref
  remote_deref="$(resolve_remote_tag_sha "$TARGET_TAG")"
  if [ "$remote_deref" != "$EXPECTED_SHA" ]; then
    echo "::error::收敛浮动标签前检测到远端 refs/tags/${TARGET_TAG} 已变更 (${remote_deref:-<missing>} != ${EXPECTED_SHA})，中止操作" >&2
    return 1
  fi

  # 阶段 1A：只读解析全部目标版本 digest 映射（零写入；遇任何非确定缺失的暂态查询失败立刻中止）。
  # 对非本轮 tag，除探测 digest 外，还强制核验其双架构（linux/amd64 + linux/arm64）revision 与远端 Git tag 解引用 SHA 一致，
  # 彻底杜绝另一版本工作流刚推完尚未通过推送核验、或架构残缺的半成品被误提升为 latest / major.minor！
  local remote_tags_raw remote_tags
  remote_tags_raw="$("$GIT_CMD" ls-remote --tags origin 'refs/tags/v*')"
  remote_tags="$(printf '%s\n' "$remote_tags_raw" | awk '{print $2}' | sed 's|refs/tags/||; s|\^{}||' | sort -u | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V -r)"

  local target_latest_ver=""
  local target_latest_digest=""
  declare -A target_series_ver=()
  declare -A target_series_digest=()
  local -a series_order=()
  local tag ver series need_global need_series pub_digest tag_expected_sha

  while IFS= read -r tag; do
    [ -z "$tag" ] && continue
    ver="${tag#v}"
    series="$(printf '%s' "$ver" | awk -F. '{print $1 "." $2}')"

    need_global=0
    [ -z "$target_latest_ver" ] && need_global=1
    need_series=0
    [ -z "${target_series_ver[$series]+x}" ] && need_series=1
    if [ "$need_global" -eq 0 ] && [ "$need_series" -eq 0 ]; then
      continue
    fi

    if [ "$tag" = "$TARGET_TAG" ]; then
      pub_digest="$DIGEST"
    else
      if ! probe_manifest "${IMAGE_LC}:${ver}"; then
        return 1
      fi
      if [ "$PROBE_STATE" = "missing" ]; then
        continue
      fi
      pub_digest="$PROBE_DIGEST"
      tag_expected_sha="$(resolve_remote_tag_sha "$tag")"
      if [ -z "$tag_expected_sha" ]; then
        echo "::error::无法解析远端 tag ${tag} 的提交 SHA，中止浮动标签对账" >&2
        return 1
      fi
      if ! verify_multiarch_revision "${IMAGE_LC}@${pub_digest}" "$tag_expected_sha"; then
        echo "::error::拒绝将未通过双架构 revision 核验的 ${IMAGE_LC}:${ver} (${pub_digest}) 提升为浮动标签" >&2
        return 1
      fi
    fi

    if [ "$need_global" -eq 1 ]; then
      target_latest_ver="$ver"
      target_latest_digest="$pub_digest"
    fi
    if [ "$need_series" -eq 1 ]; then
      target_series_ver["$series"]="$ver"
      target_series_digest["$series"]="$pub_digest"
      series_order+=("$series")
    fi
  done <<< "$remote_tags"

  if [ -z "$target_latest_digest" ]; then
    echo "::error::未解析到任何已发布版本用于 latest 对账，中止写入" >&2
    return 1
  fi

  # 阶段 1B：在首次 imagetools create 之前，先完成所有现有浮动标签（latest + 全部 major.minor）的只读探测；
  # 任一现有浮动标签探测失败直接中止，保证“查询失败时零写入”。
  local cur_latest_state cur_latest_digest
  if ! probe_manifest "${IMAGE_LC}:latest"; then
    return 1
  fi
  cur_latest_state="$PROBE_STATE"
  cur_latest_digest="$PROBE_DIGEST"

  declare -A cur_series_state=()
  declare -A cur_series_digest=()
  for series in "${series_order[@]}"; do
    if ! probe_manifest "${IMAGE_LC}:${series}"; then
      return 1
    fi
    cur_series_state["$series"]="$PROBE_STATE"
    cur_series_digest["$series"]="$PROBE_DIGEST"
  done

  # 阶段 2：所有目标映射与现状探测均已成功确认，执行幂等浮动标签写入
  local reconciled_lines=""
  if [ "$cur_latest_state" = "missing" ] || [ "$cur_latest_digest" != "$target_latest_digest" ]; then
    "$DOCKER_CMD" buildx imagetools create -t "${IMAGE_LC}:latest" "${IMAGE_LC}@${target_latest_digest}"
    reconciled_lines="${reconciled_lines}\n- \`latest\` -> \`${target_latest_ver}\` (\`${target_latest_digest}\`)"
  else
    reconciled_lines="${reconciled_lines}\n- \`latest\` already at \`${target_latest_ver}\` (\`${target_latest_digest}\`)"
  fi

  local sver sdig
  for series in "${series_order[@]}"; do
    sver="${target_series_ver[$series]}"
    sdig="${target_series_digest[$series]}"
    if [ "${cur_series_state[$series]}" = "missing" ] || [ "${cur_series_digest[$series]}" != "$sdig" ]; then
      "$DOCKER_CMD" buildx imagetools create -t "${IMAGE_LC}:${series}" "${IMAGE_LC}@${sdig}"
      reconciled_lines="${reconciled_lines}\n- \`${series}\` -> \`${sver}\` (\`${sdig}\`)"
    else
      reconciled_lines="${reconciled_lines}\n- \`${series}\` already at \`${sver}\` (\`${sdig}\`)"
    fi
  done

  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo "### Multi-arch Docker Image Published & Reconciled"
      echo "- **Triggered Tag**: \`${TARGET_TAG}\`"
      echo "- **Commit**: \`${EXPECTED_SHA}\`"
      echo "- **Immutable Ref**: \`${IMAGE_LC}:${VERSION}\`"
      echo "- **Digest Ref**: \`${IMAGE_LC}@${DIGEST}\`"
      echo ""
      echo "#### Reconciled Floating Tags"
      printf '%b\n' "${reconciled_lines:-"- (none)"}"
    } >> "$GITHUB_STEP_SUMMARY"
  fi
}

# cmd_verify_pushed: 对刚推送或复用的不可变镜像再做一次双架构 digest 与 revision 落地核验
cmd_verify_pushed() {
  local image_lc short_sha version_ref sha_ref digest_ref
  image_lc="$(printf '%s' "${REGISTRY}/${IMAGE_NAME}" | tr '[:upper:]' '[:lower:]')"
  short_sha="$(printf '%s' "$EXPECTED_SHA" | cut -c1-12)"
  version_ref="${image_lc}:${VERSION}"
  sha_ref="${image_lc}:sha-${short_sha}"
  digest_ref="${image_lc}@${DIGEST}"

  if [ -z "${DIGEST:-}" ]; then
    echo "::error::缺少待核验的不可变镜像 digest" >&2
    return 1
  fi

  if ! probe_manifest "$version_ref"; then
    return 1
  fi
  if [ "$PROBE_STATE" != "found" ] || [ "$PROBE_DIGEST" != "$DIGEST" ]; then
    echo "::error::推送核验失败：${version_ref} 实际 digest (${PROBE_DIGEST:-<missing>}) != 预期 (${DIGEST})" >&2
    return 1
  fi

  if ! probe_manifest "$sha_ref"; then
    return 1
  fi
  if [ "$PROBE_STATE" != "found" ] || [ "$PROBE_DIGEST" != "$DIGEST" ]; then
    echo "::error::推送核验失败：${sha_ref} 实际 digest (${PROBE_DIGEST:-<missing>}) != 预期 (${DIGEST})" >&2
    return 1
  fi

  verify_multiarch_revision "$digest_ref" "$EXPECTED_SHA"
  echo "推送后双架构复核通过：${version_ref} & ${sha_ref} -> ${DIGEST}"
}

case "${1:-}" in
  prepare-build)
    cmd_prepare_build
    ;;
  verify-pushed)
    cmd_verify_pushed
    ;;
  reconcile-floating)
    cmd_reconcile_floating
    ;;
  *)
    echo "usage: $0 {prepare-build|verify-pushed|reconcile-floating}" >&2
    exit 2
    ;;
esac
