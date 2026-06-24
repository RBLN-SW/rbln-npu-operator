# OLM 배포 테스트를 위한 operator 레포 지원 — 설계

**Date:** 2026-06-24
**Repo:** `rbln-npu-operator`
**Author:** mskim

## 배경 / 문제

OLM 아티팩트(bundle/catalog)는 **릴리즈 시점에만** 생성되고(`.github/workflows/release.yaml`의 `olm-bundle` 잡), 검증은 정적 `operator-sdk bundle validate`뿐이며, 그 뒤 certified-operators로 PR이 열린다. **실제 OLM 설치(CatalogSource → Subscription → CSV)를 수행하는 CI는 어디에도 없다.** 모든 기능 테스트(sentinel)는 Helm으로 배포한다. 따라서 "릴리즈에서 OLM을 패키징하기 전에 OLM 배포가 실제로 되는지"는 한 번도 검증되지 않는다.

## 결정 (테스트 배치)

- **매 PR (GH Actions, 클러스터 0):** 정적 OLM 검증 — `make bundle && operator-sdk bundle validate`, `make catalog && opm validate`.
- **matrix CI (test-infra, 기존 동적 OpenShift 프로파일):** `install` 패밀리에 **OpenShift 한정 OLM 설치 케이스 하나** 추가 — bundle+FBC catalog 빌드 → ns+OperatorGroup+CatalogSource+Subscription → CSV `Succeeded` → RBLNClusterPolicy CR apply → **operand DaemonSet들 Ready 까지** → OLM uninstall. 나머지 기능 패밀리는 Helm 유지(런타임 동작은 설치 방식과 무관하므로 OLM 재실행은 중복).
- **(선택) RC 게이트:** digest 고정된 실제 릴리즈 번들을 OpenShift에 한 번 설치해 CSV Succeeded 확인 후 certified-operators PR.

"operands Ready까지" 깊이가 핵심: CSV가 생성한 RBAC가 *실제로 충분한지*는 operator가 reconcile를 돌려 operand를 만들어봐야 드러난다.

## 핵심 발견 — RBAC 드리프트는 (지금은) 무해하다

규칙 블록 수(차트 19 vs `config/rbac` 15)는 같은 권한을 더 잘게 쪼갠 것이고, 실제 권한 집합을 비교하면:

| apiGroup | `config/rbac/role.yaml` (→ CSV) | 차트 `clusterrole-controller` |
|---|---|---|
| `apps` | daemonsets, **deployments, replicasets, statefulsets** + `deployments/finalizers` | **daemonsets** 만 |
| 그 외 전부 | 동일 | 동일 |

즉 **CSV RBAC가 차트보다 넓다(superset).** operator는 operand를 전부 DaemonSet으로만 만들므로(런타임에 Deployment/STS/RS 생성 없음) `apps/{deployments,replicasets,statefulsets}`는 kubebuilder 보일러플레이트(미사용)다. 그래서 **양쪽 모두 지금까지 정상 동작**했다.

결론: **현재 깨진 곳은 없다.** 문제는 두 RBAC 소스가 sync 장치 없이 독립 관리돼 **미래에 조용히 갈라질 수 있다**는 것. CSV는 operator-sdk가 `config/rbac`(manager-role)을 자동으로 가져가므로 늘 SoT를 추적한다 — **차트만 SoT를 안 따라간다.**

→ **sync 목표: 권한을 줄이지 않고(안전), 차트도 `config/rbac`을 단방향 생성하게 만들어 드리프트를 0으로.** `make sync-crds`와 동일 철학.

## Operator 레포 작업 (Phase)

### Phase 1 — 차트 RBAC를 `config/rbac`에서 생성 (correctness 선행)
- `make sync-rbac` 추가: 각 차트 RBAC 파일의 **템플릿 헤더는 보존**하고 `rules:` 블록만 대응하는 `config/rbac` 파일에서 갈아끼운다(awk 텍스트 슬라이싱, yq 등 신규 의존성 없음). 매핑:
  - `config/rbac/role.yaml` → `templates/rbac/clusterrole-controller.yaml`
  - `config/rbac/leader_election_role.yaml` → `templates/rbac/role-controller.yaml`
  - `config/rbac/metrics_auth_role.yaml` → `templates/rbac/clusterrole-metrics.yaml`
- `verify-manifests-sync`에 `sync-rbac` 추가 → 기존 `git diff --exit-code -- api config deployments`가 드리프트를 CI에서 차단.
- 1회 실행 효과: 차트가 `apps/{deployments,replicasets,statefulsets}` + `deployments/finalizers`를 **획득**(순수 additive, Helm 동작 불변) → 차트 == CSV.
- **verify:** `make sync-rbac` 후 차트 RBAC == config/rbac; `helm template` 렌더 정상; 기존 unit/e2e 영향 없음.
- (선택, 별도 hygiene) 미사용 `apps/{deployments,replicasets,statefulsets}` 마커를 Go에서 제거하면 CSV·차트 양쪽이 정확한 최소 집합으로 축소 — operator가 deployments를 list/watch하지 않음을 감사한 뒤에만.

### Phase 2 — FBC catalog 빌드 경로 (메인 신규 산출물)
- opm 버전 bump (v1.23.0 → FBC 지원; 인증 파이프라인 버전과 정렬).
- `catalog.Dockerfile`(FBC base + `COPY catalog/` + FBC mediatype 라벨) 추가.
- `make catalog`: `opm render $(BUNDLE_IMAGE)` → `catalog/index.yaml`(olm.package + 채널 entry) → `opm validate catalog/`.
- 레거시 `catalog-build`(`opm index add --mode semver`)는 제거/deprecated — 출하 포맷(FBC)과 일치.
- **verify:** `opm validate catalog/` 통과.

### Phase 3 — dev bundle/catalog 이미지 빌드·푸시 (CI)
- main push CI(`trigger-ci.yaml`의 `dev-image` 패턴)에 step 추가: `make bundle IMAGE=<harbor dev operator> VERSION=<dev>` → `-bundle:dev` 푸시 → `make catalog` → `-catalog:dev` 푸시. release.yaml `olm-bundle`과 동일 make 타깃 공유로 dev↔release 드리프트 방지.
- **verify:** Harbor에 `-bundle:dev`/`-catalog:dev` 존재.

### Phase 4 — 번들 위생 + (선택) PR 정적 게이트
- `alm-examples`(현재 `'[]'`)를 유효한 RBLNClusterPolicy(+RBLNDriver) 예시로 채움(operand 이미지 필드 포함 → 테스트 CR 소스).
- 계약: operand 이미지는 CR-driven, 기본값이 public `:latest` → OLM 테스트 CR은 dev operand 명시.
- (선택) `trigger-pr.yaml`에 정적 OLM 검증 잡.

## 이 레포 밖 (소비처, 참고)
- sentinel: install 패밀리에 OpenShift 한정 OLM 케이스(install→operands Ready→uninstall).
- test-infra matrix: OpenShift 스텝에서 `-bundle:dev`/`-catalog:dev` 소비.

## 리스크 / 오픈 항목
- matrix dev 번들은 `:dev`/태그 기반이라 digest 고정 실제 아티팩트는 아님(플로우/RBAC/구조만 증명) — 실제 아티팩트는 RC 게이트가 담당(선택).
- 웹훅 없음 / installModes(AllNamespaces+OwnNamespace) 준비됨 / CRD 번들 포함됨 → 추가 작업 불필요.
- metrics Service는 차트 전용(번들 미포함) — operator/operand 동작 무관, OLM 스모크엔 불필요.
