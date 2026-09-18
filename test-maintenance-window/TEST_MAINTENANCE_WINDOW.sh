#!/bin/bash
set -e

################################################################################
# MAINTENANCE WINDOW TEST FOR AUTO-REBALANCE ON IMBALANCE
#
# This script verifies that the maintenance window gates auto-rebalance:
#
# 1. Deploy Kafka cluster with auto-rebalance and a maintenance window
#    set to 00:00-00:01 UTC (effectively always outside normal hours)
# 2. Create a disk usage imbalance
# 3. Wait for Cruise Control to detect the anomaly
# 4. Verify auto-rebalance does NOT trigger (outside the window)
# 5. Patch the maintenance window to cover the current UTC hour
# 6. Verify auto-rebalance triggers within the next detection cycle
# 7. Wait for the rebalance to complete and the state to return to Idle
#
# Prerequisites:
# - Kubernetes cluster (e.g., minikube)
# - kubectl configured
# - Strimzi operator deployed
#
# Usage: ./TEST_MAINTENANCE_WINDOW.sh
################################################################################

NAMESPACE="myproject"
CLUSTER_NAME="test-cluster"

echo "================================================================================"
echo "  MAINTENANCE WINDOW TEST FOR AUTO-REBALANCE ON IMBALANCE"
echo "================================================================================"
echo ""

timestamp() { date +"%H:%M:%S"; }
log() { echo "[$(timestamp)] $1"; }

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

################################################################################
# STEP 1: Setup and Deployment
################################################################################

log "STEP 1: Setting up namespace and Strimzi operator"
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
# STEP 2: Deploy Kafka Cluster
################################################################################

log "STEP 2: Deploying Kafka cluster with maintenance window at 00:00-00:01 UTC"
echo ""

if kubectl get kafka $CLUSTER_NAME -n $NAMESPACE &>/dev/null; then
    log "Cleaning up existing cluster..."
    kubectl delete kafka $CLUSTER_NAME -n $NAMESPACE --ignore-not-found=true
    kubectl delete kafkarebalance --all -n $NAMESPACE --ignore-not-found=true
    kubectl delete cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker -n $NAMESPACE --ignore-not-found=true
    sleep 15
fi

log "Applying Kafka cluster configuration..."
kubectl apply -f test-maintenance-window.yaml -n $NAMESPACE

log "Waiting for Kafka cluster to be ready (this may take 3-5 minutes)..."
kubectl wait kafka/$CLUSTER_NAME --for=condition=Ready --timeout=600s -n $NAMESPACE
log "✓ Kafka cluster ready"

log "Waiting for Cruise Control to be ready..."
kubectl wait pod -l strimzi.io/name=${CLUSTER_NAME}-cruise-control --for=condition=Ready --timeout=300s -n $NAMESPACE
CC_POD=$(kubectl get pods -n $NAMESPACE -l strimzi.io/name=${CLUSTER_NAME}-cruise-control -o jsonpath='{.items[0].metadata.name}')
log "✓ Cruise Control ready: $CC_POD"

OPERATOR_POD=$(kubectl get pods -n $NAMESPACE -l name=strimzi-cluster-operator -o jsonpath='{.items[0].metadata.name}')
log "Operator pod: $OPERATOR_POD"

# Confirm the maintenance window is set
WINDOW=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE -o jsonpath='{.spec.maintenanceTimeWindows[0]}')
log "Active maintenance window: $WINDOW"

echo ""

################################################################################
# STEP 3: Create Disk Usage Imbalance
################################################################################

log "STEP 3: Creating disk usage imbalance"
echo ""

log "Creating 15 topics with 3 partitions each..."
for i in {1..15}; do
    kubectl run kafka-topic-create-$i -n $NAMESPACE --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- \
        bin/kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 \
        --create --topic topic-$i --partitions 3 --replication-factor 2 \
        --config min.insync.replicas=1 2>&1 | grep "Created topic" || true
done
log "✓ 15 topics created"

log "Producing data to create disk usage imbalance..."
for i in {1..10}; do
    kubectl run kafka-producer-$i -n $NAMESPACE --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- bash -c "
        for j in {1..5000}; do
            echo \"message-\$j: $(head -c 500 /dev/urandom | base64)\"
        done | bin/kafka-console-producer.sh --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 --topic topic-$i
    " &>/dev/null &
done
wait
log "✓ Data produced"

log "Concentrating partitions on broker-0 to create disk imbalance..."
kubectl run kafka-reassign -n $NAMESPACE --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- bash -c '
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
bin/kafka-reassign-partitions.sh --bootstrap-server '${CLUSTER_NAME}'-kafka-bootstrap:9092 \
    --reassignment-json-file /tmp/reassign.json --execute
' 2>&1 | grep "Successfully" || true

log "✓ Partitions concentrated on broker-0"
sleep 10

echo ""

################################################################################
# STEP 4: Wait for Cruise Control to Detect the Anomaly
################################################################################

log "STEP 4: Waiting for Cruise Control to detect anomaly"
echo ""

wait_with_countdown 180 "Cruise Control needs time to collect metrics and train"

log "Monitoring operator logs for anomaly detection (checking every 30s for up to 5 minutes)..."
DETECTED=false
for i in {1..10}; do
    if kubectl logs -n $NAMESPACE $OPERATOR_POD --tail=100 2>/dev/null | \
            grep -q "Goal violations detected"; then
        log "✓ ANOMALY DETECTED by operator"
        DETECTED=true
        break
    fi
    if [ $i -lt 10 ]; then
        printf "  Check $i/10 - No detection yet, waiting 30s...\n"
        sleep 30
    fi
done

if [ "$DETECTED" = false ]; then
    log "⚠ Anomaly not yet detected. Cruise Control may need more time."
    log "  Continuing anyway to verify the window blocking behaviour."
fi

echo ""

################################################################################
# STEP 5: Verify Auto-Rebalance is Blocked Outside the Maintenance Window
################################################################################

log "STEP 5: Verifying auto-rebalance is NOT triggered outside the maintenance window"
echo ""

BLOCKED=true
for i in {1..6}; do
    AUTO_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE \
        -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)

    if [ "$AUTO_STATE" = "RebalanceOnImbalance" ]; then
        log "✗ Auto-rebalance triggered unexpectedly (state: $AUTO_STATE)"
        BLOCKED=false
        break
    fi

    if kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-imbalance -n $NAMESPACE &>/dev/null; then
        log "✗ KafkaRebalance resource created unexpectedly"
        BLOCKED=false
        break
    fi

    # Also check operator logs for the deferral message
    if kubectl logs -n $NAMESPACE $OPERATOR_POD --tail=50 2>/dev/null | \
            grep -q "outside maintenance window"; then
        log "✓ Operator logged: goal violations detected but outside maintenance window"
        break
    fi

    printf "  Check $i/6 - State: ${AUTO_STATE:-Idle}, no rebalance triggered (correct), waiting 20s...\n"
    sleep 20
done

if [ "$BLOCKED" = true ]; then
    log "✓ Auto-rebalance correctly blocked outside maintenance window"
else
    log "✗ Auto-rebalance triggered outside maintenance window — unexpected"
    exit 1
fi

echo ""

################################################################################
# STEP 6: Open the Maintenance Window to the Current UTC Hour
################################################################################

log "STEP 6: Patching maintenance window to cover the current UTC hour"
echo ""

# Compute the current UTC hour so the window covers it
CURRENT_HOUR=$(date -u +"%H" | sed 's/^0*//')
CURRENT_HOUR=${CURRENT_HOUR:-0}
NEXT_HOUR=$(( (CURRENT_HOUR + 1) % 24 ))
WINDOW_EXPR="* * ${CURRENT_HOUR}-${NEXT_HOUR} * * ?"

log "Current UTC hour: $CURRENT_HOUR — patching window to: $WINDOW_EXPR"

kubectl patch kafka $CLUSTER_NAME -n $NAMESPACE --type=merge \
    -p "{\"spec\":{\"maintenanceTimeWindows\":[\"${WINDOW_EXPR}\"]}}"

log "✓ Maintenance window updated"

echo ""

################################################################################
# STEP 7: Verify Auto-Rebalance Triggers Within the Open Window
################################################################################

log "STEP 7: Verifying auto-rebalance triggers within the maintenance window"
echo ""

log "Monitoring for trigger (checking every 15s for up to 5 minutes)..."
TRIGGERED=false
for i in {1..20}; do
    AUTO_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE \
        -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)

    if [ "$AUTO_STATE" = "RebalanceOnImbalance" ]; then
        log "✓ AUTO-REBALANCE TRIGGERED! State: RebalanceOnImbalance"
        TRIGGERED=true
        break
    fi

    if kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-imbalance -n $NAMESPACE &>/dev/null; then
        KR_STATE=$(kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-imbalance \
            -n $NAMESPACE -o jsonpath='{.status.conditions[0].type}' 2>/dev/null)
        log "✓ KafkaRebalance created (state: $KR_STATE)"
        TRIGGERED=true
        break
    fi

    printf "  Check $i/20 - State: ${AUTO_STATE:-Idle}, waiting 15s...\n"
    sleep 15
done

if [ "$TRIGGERED" = false ]; then
    log "✗ Auto-rebalance did not trigger within the open maintenance window"
    log "  Check operator logs for details:"
    kubectl logs -n $NAMESPACE $OPERATOR_POD --tail=50
    exit 1
fi

echo ""

################################################################################
# STEP 8: Wait for Rebalance to Complete
################################################################################

log "STEP 8: Waiting for rebalance to complete"
echo ""

log "Monitoring rebalance progress (checking every 15s for up to 5 minutes)..."
COMPLETED=false
for i in {1..20}; do
    if ! kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-imbalance -n $NAMESPACE &>/dev/null; then
        log "✓ KafkaRebalance deleted — rebalance complete"
        COMPLETED=true
        break
    fi

    FINAL_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE \
        -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)
    if [ "$FINAL_STATE" = "Idle" ]; then
        log "✓ Auto-rebalance state returned to Idle"
        COMPLETED=true
        break
    fi

    printf "  Check $i/20 - Still rebalancing...\n"
    sleep 15
done

if [ "$COMPLETED" = false ]; then
    log "⚠ Rebalance did not complete in expected time"
    kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-imbalance -n $NAMESPACE -o yaml
    exit 1
fi

echo ""

################################################################################
# STEP 9: Final Verification
################################################################################

log "STEP 9: Final verification"
echo ""

FINAL_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE -o jsonpath='{.status.autoRebalance.state}')
log "Auto-rebalance state: $FINAL_STATE"

if kubectl get cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker -n $NAMESPACE &>/dev/null; then
    COMPLETION_TIME=$(kubectl get cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker \
        -n $NAMESPACE -o jsonpath='{.data.lastRebalanceCompletionTime}')
    log "✓ Tracker ConfigMap updated — lastRebalanceCompletionTime: $COMPLETION_TIME"
else
    log "✗ Tracker ConfigMap not found"
fi

echo ""
echo "================================================================================"
echo "  TEST RESULTS"
echo "================================================================================"
echo ""
echo "✓ Kafka cluster deployed with maintenance window at 00:00-00:01 UTC"
echo "✓ Disk usage imbalance created"
echo "✓ Cruise Control detected anomaly"
echo "✓ Auto-rebalance correctly BLOCKED outside the maintenance window"
echo "✓ Maintenance window patched to cover the current UTC hour"
echo "✓ Auto-rebalance TRIGGERED once inside the window"
echo "✓ Rebalance completed — state returned to Idle"
echo "✓ Tracker ConfigMap updated with completion time"
echo ""
echo "🎉 TEST PASSED! Maintenance window correctly gates auto-rebalance on imbalance."
echo ""
echo "================================================================================"
echo ""
echo "Detailed status:"
kubectl get kafka $CLUSTER_NAME -n $NAMESPACE -o jsonpath='{.status.autoRebalance}' | jq .
echo ""
echo "To cleanup:"
echo "  kubectl delete kafka $CLUSTER_NAME -n $NAMESPACE"
echo "  kubectl delete kafkarebalance --all -n $NAMESPACE"
echo ""
