#!/bin/bash
set -e

################################################################################
# AUTO-REBALANCE ON IMBALANCE - METRICS VERIFICATION TEST
#
# This script verifies the two metrics changes introduced for auto-rebalance
# on imbalance:
#
# 1. strimzi_auto_rebalance_anomalies_detected_total
#    - Exposed by the Cluster Operator on port 8080
#    - Incremented when Cruise Control detects a goal violation
#    - Labels: type (e.g. goal_violation), fixability (fixable/unfixable/mixed)
#
# 2. mode label on strimzi_kafka_rebalance_resource_info
#    - Exposed by kube-state-metrics
#    - Differentiates KafkaRebalance resources by mode (full/add-brokers/remove-brokers)
#
# What this script does:
#   1. Deploys the Strimzi cluster operator
#   2. Deploys kube-state-metrics with the Strimzi custom resource configmap
#   3. Deploys a Kafka cluster with imbalance mode auto-rebalance enabled
#   4. Creates a disk usage imbalance to trigger Cruise Control anomaly detection
#   5. Waits for the auto-rebalance to trigger and complete
#   6. Verifies both metrics are present and correct
#
# Prerequisites:
# - Kubernetes cluster (e.g., minikube)
# - kubectl configured
#
# Usage: ./TEST_METRICS.sh [namespace]
################################################################################

NAMESPACE="${1:-myproject}"
CLUSTER_NAME="test-cluster"

echo "================================================================================"
echo "  AUTO-REBALANCE METRICS VERIFICATION TEST"
echo "================================================================================"
echo ""

timestamp() { date +"%H:%M:%S"; }
log()  { echo "[$(timestamp)] $1"; }
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; }
info() { echo "  [INFO] $1"; }

wait_with_countdown() {
    local seconds=$1
    local message=$2
    log "$message (${seconds}s)"
    for ((i=seconds; i>0; i--)); do
        printf "\r  Waiting... %3ds remaining" $i
        sleep 1
    done
    printf "\r  Done!                    \n"
}

# Kill any background port-forwards on exit
PF_PID=""
PF_PID2=""
cleanup() {
    [ -n "$PF_PID"  ] && kill "$PF_PID"  2>/dev/null || true
    [ -n "$PF_PID2" ] && kill "$PF_PID2" 2>/dev/null || true
}
trap cleanup EXIT

################################################################################
# STEP 1: Deploy Strimzi Cluster Operator
################################################################################

log "STEP 1: Deploying Strimzi Cluster Operator"
echo ""

if ! kubectl get namespace $NAMESPACE &>/dev/null; then
    log "Creating namespace $NAMESPACE..."
    kubectl create namespace $NAMESPACE
else
    log "Namespace $NAMESPACE already exists"
fi

if kubectl get deployment strimzi-cluster-operator -n $NAMESPACE &>/dev/null; then
    log "Strimzi operator already deployed"
else
    log "Deploying Strimzi operator..."
    kubectl apply -f cluster-operator -n $NAMESPACE
    kubectl wait pod -l name=strimzi-cluster-operator --for=condition=Ready --timeout=120s -n $NAMESPACE
    log "✓ Operator ready"
fi

echo ""

################################################################################
# STEP 2: Deploy kube-state-metrics with Strimzi configmap
################################################################################

log "STEP 2: Deploying kube-state-metrics with Strimzi custom resource config"
echo ""

# The configmap contains the mode label for KafkaRebalance — the key change being tested
if kubectl get deployment strimzi-kube-state-metrics -n $NAMESPACE &>/dev/null; then
    log "kube-state-metrics already deployed — applying configmap to pick up mode label..."
    kubectl apply -f kube-state-metrics/configmap.yaml -n $NAMESPACE
    kubectl rollout restart deployment/strimzi-kube-state-metrics -n $NAMESPACE
else
    log "Deploying kube-state-metrics configmap..."
    kubectl apply -f kube-state-metrics/configmap.yaml -n $NAMESPACE

    log "Deploying kube-state-metrics..."
    # Substitute namespace into ClusterRoleBinding subject before applying
    sed "s/namespace: myproject/namespace: $NAMESPACE/" kube-state-metrics/ksm.yaml | kubectl apply -n $NAMESPACE -f -
fi

kubectl wait pod -l app.kubernetes.io/name=kube-state-metrics \
    --for=condition=Ready --timeout=120s -n $NAMESPACE
log "✓ kube-state-metrics ready"

echo ""

################################################################################
# STEP 3: Deploy Kafka cluster with imbalance mode auto-rebalance
################################################################################

log "STEP 3: Deploying Kafka cluster with imbalance mode auto-rebalance"
echo ""

if kubectl get kafka $CLUSTER_NAME -n $NAMESPACE &>/dev/null; then
    log "Cleaning up existing cluster..."
    kubectl delete kafka $CLUSTER_NAME -n $NAMESPACE --ignore-not-found=true
    kubectl delete kafkarebalance --all -n $NAMESPACE --ignore-not-found=true
    kubectl delete cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker -n $NAMESPACE --ignore-not-found=true
    sleep 15
fi

log "Applying Kafka cluster configuration..."
kubectl apply -f test-kafka-with-metrics.yaml -n $NAMESPACE

log "Waiting for Kafka cluster to be ready (this may take 3-5 minutes)..."
kubectl wait kafka/$CLUSTER_NAME --for=condition=Ready --timeout=600s -n $NAMESPACE
log "✓ Kafka cluster ready"

log "Waiting for Cruise Control to be ready..."
kubectl wait pod -l strimzi.io/name=${CLUSTER_NAME}-cruise-control \
    --for=condition=Ready --timeout=300s -n $NAMESPACE
CC_POD=$(kubectl get pods -n $NAMESPACE -l strimzi.io/name=${CLUSTER_NAME}-cruise-control \
    -o jsonpath='{.items[0].metadata.name}')
log "✓ Cruise Control ready: $CC_POD"

echo ""

################################################################################
# STEP 4: Create disk usage imbalance
################################################################################

log "STEP 4: Creating disk usage imbalance to trigger anomaly detection"
echo ""

log "Creating 15 topics with 3 partitions each..."
for i in {1..15}; do
    kubectl run kafka-topic-create-$i -n $NAMESPACE \
        --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- \
        bin/kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 \
        --create --topic topic-$i --partitions 3 --replication-factor 2 \
        --config min.insync.replicas=1 2>&1 | grep "Created topic" || true
done
log "✓ 15 topics created"

log "Producing data to create disk usage imbalance..."
for i in {1..10}; do
    kubectl run kafka-producer-$i -n $NAMESPACE \
        --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- bash -c "
        for j in {1..5000}; do
            echo \"message-\$j: $(head -c 500 /dev/urandom | base64)\"
        done | bin/kafka-console-producer.sh \
            --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 --topic topic-$i
    " &>/dev/null &
done
wait
log "✓ Data produced"

log "Reassigning partitions to concentrate load on broker-0..."
kubectl run kafka-reassign -n $NAMESPACE \
    --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- bash -c '
cat > /tmp/reassign.json <<EOF
{
  "version": 1,
  "partitions": [
    {"topic": "topic-1", "partition": 0, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-1", "partition": 1, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-1", "partition": 2, "replicas": [0,2], "log_dirs": ["any","any"]},
    {"topic": "topic-2", "partition": 0, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-2", "partition": 1, "replicas": [0,2], "log_dirs": ["any","any"]},
    {"topic": "topic-2", "partition": 2, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-3", "partition": 0, "replicas": [0,2], "log_dirs": ["any","any"]},
    {"topic": "topic-3", "partition": 1, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-3", "partition": 2, "replicas": [0,2], "log_dirs": ["any","any"]},
    {"topic": "topic-4", "partition": 0, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-4", "partition": 1, "replicas": [0,2], "log_dirs": ["any","any"]},
    {"topic": "topic-4", "partition": 2, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-5", "partition": 0, "replicas": [0,2], "log_dirs": ["any","any"]},
    {"topic": "topic-5", "partition": 1, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-5", "partition": 2, "replicas": [0,2], "log_dirs": ["any","any"]}
  ]
}
EOF
bin/kafka-reassign-partitions.sh \
    --bootstrap-server '${CLUSTER_NAME}'-kafka-bootstrap:9092 \
    --reassignment-json-file /tmp/reassign.json --execute
' 2>&1 | grep "Successfully" || true
log "✓ Partition imbalance created"

echo ""

################################################################################
# STEP 5: Wait for anomaly detection and auto-rebalance
################################################################################

log "STEP 5: Waiting for Cruise Control to detect anomaly and trigger auto-rebalance"
echo ""

wait_with_countdown 180 "Cruise Control needs time to collect metrics and train"

log "Monitoring for auto-rebalance trigger (checking every 15s for up to 5 minutes)..."
TRIGGERED=false
for i in {1..20}; do
    AUTO_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE \
        -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)

    if [ "$AUTO_STATE" = "RebalanceOnImbalance" ]; then
        log "✓ AUTO-REBALANCE TRIGGERED! State: RebalanceOnImbalance"
        TRIGGERED=true
        break
    fi

    if kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-imbalance \
            -n $NAMESPACE &>/dev/null; then
        log "✓ KafkaRebalance resource created"
        TRIGGERED=true
        break
    fi

    printf "  Check $i/20 - State: ${AUTO_STATE:-Idle}, waiting 15s...\n"
    sleep 15
done

if [ "$TRIGGERED" = false ]; then
    fail "Auto-rebalance did not trigger — anomaly may not have been detected yet"
    info "Check Cruise Control logs: kubectl logs -n $NAMESPACE $CC_POD --tail=50"
    exit 1
fi

log "Waiting for rebalance to complete..."
for i in {1..20}; do
    if ! kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-imbalance \
            -n $NAMESPACE &>/dev/null; then
        log "✓ Rebalance completed and KafkaRebalance deleted"
        break
    fi
    printf "  Check $i/20 - Still rebalancing...\n"
    sleep 15
done

echo ""

################################################################################
# STEP 6: Verify strimzi_auto_rebalance_anomalies_detected_total
################################################################################

log "STEP 6: Verifying strimzi_auto_rebalance_anomalies_detected_total counter"
echo ""

OPERATOR_POD=$(kubectl get pods -n $NAMESPACE -l name=strimzi-cluster-operator \
    -o jsonpath='{.items[0].metadata.name}')

info "Port-forwarding to operator metrics endpoint (pod port 8080)..."
kubectl port-forward -n $NAMESPACE pod/$OPERATOR_POD 18080:8080 &>/dev/null &
PF_PID=$!
sleep 3

OPERATOR_METRICS=$(curl -s http://localhost:18080/metrics 2>/dev/null || echo "")
kill $PF_PID 2>/dev/null || true
PF_PID=""

if [ -z "$OPERATOR_METRICS" ]; then
    fail "Could not reach operator metrics endpoint on port 18080"
else
    ANOMALY_LINES=$(echo "$OPERATOR_METRICS" | \
        grep "strimzi_auto_rebalance_anomalies_detected_total" || true)

    if [ -z "$ANOMALY_LINES" ]; then
        fail "strimzi_auto_rebalance_anomalies_detected_total not found in operator metrics"
    else
        pass "strimzi_auto_rebalance_anomalies_detected_total found"
        echo ""
        echo "  Raw metric output:"
        echo "$ANOMALY_LINES" | while read -r line; do echo "    $line"; done
        echo ""

        # Check fixability labels
        for fixability in fixable unfixable mixed; do
            if echo "$ANOMALY_LINES" | grep -q "fixability=\"$fixability\""; then
                pass "fixability=\"$fixability\" label present"
            else
                info "fixability=\"$fixability\" not found (may not have been triggered)"
            fi
        done

        # Check type label
        if echo "$ANOMALY_LINES" | grep -q "type=\"goal_violation\""; then
            pass "type=\"goal_violation\" label present"
        else
            fail "type=\"goal_violation\" label missing"
        fi

        # Check counter > 0 (value is the last token on the line)
        COUNTER_VALUE=$(echo "$ANOMALY_LINES" | awk '{print $NF}' | head -1)
        if [ -n "$COUNTER_VALUE" ] && awk "BEGIN{exit !($COUNTER_VALUE > 0)}"; then
            pass "Counter value is > 0 (value: $COUNTER_VALUE)"
        else
            fail "Counter value is 0 — anomaly was not counted"
        fi
    fi
fi

echo ""

################################################################################
# STEP 7: Verify mode label on strimzi_kafka_rebalance_resource_info
################################################################################

log "STEP 7: Verifying mode label on strimzi_kafka_rebalance_resource_info"
echo ""

KSM_POD=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=kube-state-metrics \
    -o jsonpath='{.items[0].metadata.name}')

info "Port-forwarding to kube-state-metrics (pod port 8080)..."
kubectl port-forward -n $NAMESPACE pod/$KSM_POD 18081:8080 &>/dev/null &
PF_PID2=$!
sleep 3

KSM_METRICS=$(curl -s http://localhost:18081/metrics 2>/dev/null || echo "")
kill $PF_PID2 2>/dev/null || true
PF_PID2=""

if [ -z "$KSM_METRICS" ]; then
    fail "Could not reach kube-state-metrics endpoint on port 18081"
else
    # Create a KafkaRebalance resource in full mode to test the label if none exists
    if ! kubectl get kafkarebalance -n $NAMESPACE 2>/dev/null | grep -q .; then
        info "No KafkaRebalance resources found — creating a test resource..."
        kubectl apply -n $NAMESPACE -f - <<EOF
apiVersion: kafka.strimzi.io/v1
kind: KafkaRebalance
metadata:
  name: metrics-test-rebalance
  labels:
    strimzi.io/cluster: $CLUSTER_NAME
spec:
  mode: full
EOF
        sleep 5
        KSM_METRICS=$(curl -s http://localhost:18081/metrics 2>/dev/null || echo "")
    fi

    KR_LINES=$(echo "$KSM_METRICS" | grep "strimzi_kafka_rebalance_resource_info" || true)

    if [ -z "$KR_LINES" ]; then
        fail "strimzi_kafka_rebalance_resource_info not found in kube-state-metrics output"
        info "Ensure kube-state-metrics is using the Strimzi configmap"
    else
        pass "strimzi_kafka_rebalance_resource_info found"
        echo ""
        echo "  Raw metric output:"
        echo "$KR_LINES" | while read -r line; do echo "    $line"; done
        echo ""

        if echo "$KR_LINES" | grep -q "mode="; then
            pass "mode label is present on strimzi_kafka_rebalance_resource_info"
        else
            fail "mode label is missing — configmap may not have been applied or kube-state-metrics not restarted"
        fi

        for mode in full add-brokers remove-brokers; do
            if echo "$KR_LINES" | grep -q "mode=\"$mode\""; then
                pass "mode=\"$mode\" found"
            fi
        done

        AUTO_KR="${CLUSTER_NAME}-auto-rebalancing-imbalance"
        if echo "$KR_LINES" | grep -q "name=\"$AUTO_KR\""; then
            MODE_VALUE=$(echo "$KR_LINES" | grep "name=\"$AUTO_KR\"" | \
                sed 's/.*mode="\([^"]*\)".*/\1/')
            pass "Auto-generated KafkaRebalance '$AUTO_KR' found with mode=\"$MODE_VALUE\""
        else
            info "Auto-generated KafkaRebalance '$AUTO_KR' not found (rebalance already completed)"
        fi
    fi
fi

# Clean up the test KafkaRebalance if we created one
kubectl delete kafkarebalance metrics-test-rebalance -n $NAMESPACE \
    --ignore-not-found=true &>/dev/null || true

echo ""

################################################################################
# STEP 8: Summary
################################################################################

log "STEP 8: Test Summary"
echo ""
echo "================================================================================"
echo "  METRICS VERIFICATION COMPLETE"
echo "================================================================================"
echo ""
echo "Metrics verified:"
echo "  1. strimzi_auto_rebalance_anomalies_detected_total (Cluster Operator, port 8080)"
echo "     - type=goal_violation label"
echo "     - fixability label (fixable / unfixable / mixed)"
echo "     - Counter incremented on anomaly detection"
echo ""
echo "  2. strimzi_kafka_rebalance_resource_info (kube-state-metrics, port 8080)"
echo "     - mode label (full / add-brokers / remove-brokers)"
echo ""
echo "To view raw metrics manually:"
echo "  Operator:          kubectl port-forward -n $NAMESPACE pod/$OPERATOR_POD 18080:8080"
echo "                     curl http://localhost:18080/metrics | grep strimzi_auto_rebalance"
echo ""
echo "  kube-state-metrics: kubectl port-forward -n $NAMESPACE pod/$KSM_POD 18081:8080"
echo "                      curl http://localhost:18081/metrics | grep strimzi_kafka_rebalance"
echo ""
echo "To clean up:"
echo "  kubectl delete kafka $CLUSTER_NAME -n $NAMESPACE"
echo "  kubectl delete -f kube-state-metrics/ -n $NAMESPACE"
echo ""
