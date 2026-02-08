# RBLN Driver Manager / Validator / Container Toolkit 배포 가이드

이 문서는 최근 변경(diff)에 추가된 **driver-manager**, **validator**, **container-toolkit** 구성과
Helm 차트(`deployments/rbln-npu-operator`)로 사용하는 방법을 정리합니다.

## 1) 이번 변경에서 추가된 구성 요약

### 1.1 Driver Manager (RBLNDriver)
- **신규 CRD**: `rebellions.ai/v1alpha1`, `RBLNDriver`
- **역할**: 노드의 OS/커널 조합별로 **Driver Manager DaemonSet을 생성**하여 드라이버를 설치/관리.
- **노드 풀 분리**: NFD 라벨(`feature.node.kubernetes.io/...`)을 기준으로 OS+커널 별 DaemonSet을 생성.
- **호스트 설치 경로**: `/run/rbln/driver` (hostPath)
- **프로세스 및 상태**:
  - driver-manager 컨테이너는 `rbln-k8s-driver-manager` 기반 실행
  - 드라이버 설치 완료 여부를 `/run/rbln/validations/.driver-ctr-ready`로 표시
- **네이밍 변경**
  - DaemonSet 이름: `rbln-k8s-driver-manager` → `rbln-driver`
  - 컨테이너 이름: `rbln-umd-installer` → `rbln-driver-container`

### 1.2 Validator (operator-validator)
- **배포 대상**: `RBLNClusterPolicy`에서 `validator` 섹션 활성화 시 DaemonSet 생성
- **구성**:
  - `driver-validation` initContainer: 드라이버 상태 확인
  - `toolkit-validation` initContainer: CDI 스펙 생성 여부 확인
- **ready 파일**: `/run/rbln/validations/driver-ready`, `/run/rbln/validations/toolkit-ready`
- **CDI 검사**: `/var/run/cdi`에 **rbln** 문자열을 포함한 스펙 파일이 최신인지 확인

### 1.3 Container Toolkit (container-toolkit)
- **배포 대상**: `RBLNClusterPolicy`에서 `containerToolkit` 활성화 시 DaemonSet 생성
- **실행 흐름**:
  1) `/run/rbln/validations/driver-ready` 대기
  2) `driver-ready` 파일 source → `RBLN_CTK_DAEMON_*` 환경 변수 적용
  3) `rbln-ctk-daemon` 실행
- **호스트/런타임 연동**:
  - `/var/run/cdi`에 CDI 스펙 생성
  - 런타임 소켓을 **감지한 런타임(containerd/docker/cri-o)**에 따라 마운트
- **주요 마운트**:
  - `/run/rbln/driver` (hostPath)
  - `/run/rbln` (hostPath)
  - `/var/run/cdi` (hostPath)
  - `/host` (hostPath, readOnly)
  - `/host/usr/local/bin` (hostPath, **RW**, hook 설치용)

---

## 2) Helm 차트로 사용하는 방법

Helm 차트는 `deployments/rbln-npu-operator` 디렉토리에 있습니다.

### 2.1 설치 기본 흐름
```bash
helm install rbln-npu-operator ./deployments/rbln-npu-operator \
  --create-namespace \
  --namespace rbln-system
```

### 2.2 Driver Manager 배포 (RBLNDriver CR)
- Helm에서 `driver.enabled: true`를 설정하면 `RBLNDriver`가 생성됩니다.
- 이미지 및 매니저 설정은 `driver` 및 `driver.manager` 섹션에서 관리됩니다.

```yaml
# values.yaml 예시
driver:
  enabled: true
  image:
    registry: harbor.k8s.rebellions.in
    repository: rebellions-sw/driver
    tag: "3.0.0"
    pullPolicy: IfNotPresent
  imagePullSecrets: []
  nodeSelector: {}
  nodeAffinity: {}
  tolerations: []
  labels: {}
  annotations: {}
  priorityClassName: ""
  resources: {}
  args: []
  env: []
  manager:
    registry: docker.io
    image: rebellions/rbln-k8s-driver-manager
    version: latest
    imagePullPolicy: IfNotPresent
    imagePullSecrets: []
    env: []
```

> **주의**: Driver Manager는 **NFD 라벨**을 사용해 OS/커널 풀을 나눕니다. NFD가 없으면 풀 생성이 실패할 수 있습니다.

### 2.3 Validator 배포
Validator는 `RBLNClusterPolicy`로 관리됩니다.

```yaml
# values.yaml 예시
validator:
  image:
    registry: docker.io
    repository: rebellions/rbln-npu-operator-validator
    tag: latest
    pullPolicy: IfNotPresent
  imagePullSecrets: []
  resources: {}
  args: []
  env: []
  plugin:
    env: []
  toolkit:
    env: []
  driver:
    env: []
  vfioPCI:
    env: []
```

### 2.4 Container Toolkit 배포
Container Toolkit 역시 `RBLNClusterPolicy`로 관리됩니다.

```yaml
# values.yaml 예시
containerToolkit:
  enabled: true
  image:
    registry: docker.io
    repository: rebellions/rbln-container-toolkit
    tag: latest
    pullPolicy: IfNotPresent
  imagePullSecrets: []
  resources: {}
  args: []
  env: []
```

---

## 3) 배포 시 참고 사항

- **driver-manager / validator / container-toolkit 모두 DaemonSet 형태**로 배포됩니다.
- `container-toolkit`은 driver-ready 파일을 source 하므로, **validator가 먼저 정상 동작**해야 합니다.
- CDI 스펙은 `/var/run/cdi`에 생성됩니다.
- 런타임 소켓은 cluster runtime 감지 결과에 따라 자동 선택됩니다.

---

## 4) 샘플 values 파일

차트에 기본 샘플이 포함되어 있습니다:
- `deployments/rbln-npu-operator/sample-values-ContainerWorkload.yaml`
- `deployments/rbln-npu-operator/sample-values-SandboxWorkload.yaml`

컨테이너 워크로드를 사용할 경우:
```bash
helm install rbln-npu-operator ./deployments/rbln-npu-operator \
  -f deployments/rbln-npu-operator/sample-values-ContainerWorkload.yaml \
  --create-namespace \
  --namespace rbln-system
```

---

## 5) 관련 템플릿

Helm 템플릿 경로:
- `deployments/rbln-npu-operator/templates/rblndriver.yaml` → RBLNDriver 생성
- `deployments/rbln-npu-operator/templates/rblnclusterpolicy.yaml` → RBLNClusterPolicy 생성
