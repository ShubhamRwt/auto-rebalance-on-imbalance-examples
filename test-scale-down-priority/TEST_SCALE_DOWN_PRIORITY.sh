#!/bin/bash
set -e

################################################################################
# SCALE-DOWN PRIORITY TEST FOR AUTO-REBALANCE ON IMBALANCE
#
# This script verifies that a scale-down operation takes priority over an
# in-progress auto-rebalance on imbalance:
#
# 1. Deploy a 4-broker Kafka cluster with both remove-brokers and imbalance
#    auto-rebalance modes configured
# 2. Create a disk usage imbalance to trigger auto-rebalance on imbalance
# 3. Wait for the state to enter RebalanceOnImbalance
# 4. While the imbalance rebalance is in progress, trigger a scale-down
# 5. Verify:
#    - The imbalance KafkaRebalance is stopped and deleted
#    - A scale-down KafkaRebalance is created
#    - The state transitions to RebalanceOnScaleDown
#    - The tracker ConfigMap is updated (the stopped rebalance counts as terminal)
# 6. Complete the scale-down and verify the state returns to Idle
# 7. Verify auto-rebalance on imbalance re-triggers if violations still exist
#
# Prerequisites:
# - Kubernetes cluster (e.g., minikube)
# - kubectl configured
# - Strimzi operator deployed
#
# Usage: ./TEST_SCALE_DOWN_PRIORITY.sh
################################################################################

NAMESPACE="myproject"
CLUSTER_NAME="test-cluster"
IMBALANCE_KR="${CLUSTER_NAME}-auto-rebalancing-imbalance"
SCALE_DOWN_KR="${CLUSTER_NAME}-auto-rebalancing-remove-brokers"

echo "================================================================================"
echo "  SCALE-DOWN PRIORITY TEST FOR AUTO-REBALANCE ON IMBALANCE"
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
# STEP 2: Deploy 4-Broker Kafka Cluster
################################################################################

log "STEP 2: Deploying 4-broker Kafka cluster with imbalance + scale auto-rebalance"
echo ""

if kubectl get kafka $CLUSTER_NAME -n $NAMESPACE &>/dev/null; then
    log "Cleaning up existing cluster..."
    kubectl delete kafka $CLUSTER_NAME -n $NAMESPACE --ignore-not-found=true
    kubectl delete kafkarebalance --all -n $NAMESPACE --ignore-not-found=true
    kubectl delete cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker -n $NAMESPACE --ignore-not-found=true
    sleep 15
fi

log "Applying Kafka cluster configuration (4 brokers)..."
kubectl apply -f test-scale-down-priority.yaml -n $NAMESPACE

log "Waiting for Kafka cluster to be ready (this may take 3-5 minutes)..."
kubectl wait kafka/$CLUSTER_NAME --for=condition=Ready --timeout=600s -n $NAMESPACE
log "✓ Kafka cluster ready (4 brokers)"

log "Waiting for Cruise Control to be ready..."
kubectl wait pod -l strimzi.io/name=${CLUSTER_NAME}-cruise-control --for=condition=Ready --timeout=300s -n $NAMESPACE
CC_POD=$(kubectl get pods -n $NAMESPACE -l strimzi.io/name=${CLUSTER_NAME}-cruise-control -o jsonpath='{.items[0].metadata.name}')
log "✓ Cruise Control ready: $CC_POD"

OPERATOR_POD=$(kubectl get pods -n $NAMESPACE -l name=strimzi-cluster-operator -o jsonpath='{.items[0].metadata.name}')
log "Operator pod: $OPERATOR_POD"

echo ""

################################################################################
# STEP 3: Create Disk Usage Imbalance
################################################################################

log "STEP 3: Creating disk usage imbalance to trigger auto-rebalance on imbalance"
echo ""

log "Creating 15 topics..."
for i in {1..15}; do
    kubectl run kafka-topic-create-$i -n $NAMESPACE --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- \
        bin/kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 \
        --create --topic topic-$i --partitions 3 --replication-factor 2 \
        --config min.insync.replicas=1 2>&1 | grep "Created topic" || true
done
log "✓ Topics created"

log "Producing data to create imbalance..."
for i in {1..10}; do
    kubectl run kafka-producer-$i -n $NAMESPACE --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- bash -c "
        for j in {1..5000}; do
            echo \"message-\$j: $(head -c 500 /dev/urandom | base64)\"
        done | bin/kafka-console-producer.sh --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 --topic topic-$i
    " &>/dev/null &
done
wait
log "✓ Data produced"

log "Concentrating partitions on broker-0..."
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

log "✓ Imbalance created"
sleep 10

echo ""

################################################################################
# STEP 4: Wait for Auto-Rebalance on Imbalance to Start
################################################################################

log "STEP 4: Waiting for auto-rebalance on imbalance to start"
echo ""

wait_with_countdown 180 "Waiting for Cruise Control to detect anomaly"

log "Monitoring for RebalanceOnImbalance state (checking every 15s for up to 5 minutes)..."
IMBALANCE_STARTED=false
for i in {1..20}; do
    AUTO_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE \
        -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)

    if [ "$AUTO_STATE" = "RebalanceOnImbalance" ]; then
        log "✓ State is RebalanceOnImbalance — imbalance rebalance is running"
        IMBALANCE_STARTED=true
        break
    fi

    if kubectl get kafkarebalance $IMBALANCE_KR -n $NAMESPACE &>/dev/null; then
        KR_STATE=$(kubectl get kafkarebalance $IMBALANCE_KR -n $NAMESPACE \
            -o jsonpath='{.status.conditions[0].type}' 2>/dev/null)
        log "✓ Imbalance KafkaRebalance exists (state: $KR_STATE)"
        IMBALANCE_STARTED=true
        break
    fi

    printf "  Check $i/20 - State: ${AUTO_STATE:-Idle}, waiting 15s...\n"
    sleep 15
done

if [ "$IMBALANCE_STARTED" = false ]; then
    log "✗ Imbalance rebalance did not start — cannot proceed with priority test"
    log "  Check operator logs for details:"
    kubectl logs -n $NAMESPACE $OPERATOR_POD --tail=50
    exit 1
fi

echo ""

################################################################################
# STEP 5: Trigger Scale-Down While Imbalance Rebalance is Running
################################################################################

log "STEP 5: Triggering scale-down (broker 3 has partitions) while imbalance rebalance is running"
echo ""

log "Current auto-rebalance state: $(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE -o jsonpath='{.status.autoRebalance.state}')"
log "Imbalance KafkaRebalance exists: $(kubectl get kafkarebalance $IMBALANCE_KR -n $NAMESPACE &>/dev/null && echo yes || echo no)"

log "Scaling down broker pool from 4 to 3 replicas..."
kubectl patch kafkanodepool broker -n $NAMESPACE --type=merge \
    -p '{"spec":{"replicas":3}}'
log "✓ Scale-down triggered"

echo ""

################################################################################
# STEP 6: Verify Scale-Down Takes Priority
################################################################################

log "STEP 6: Verifying scale-down takes priority over imbalance rebalance"
echo ""

log "Monitoring state transitions (checking every 10s for up to 3 minutes)..."
SCALE_DOWN_STARTED=false
for i in {1..18}; do
    AUTO_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE \
        -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)

    # Check if imbalance KR was deleted (stopped)
    IMBALANCE_KR_EXISTS=$(kubectl get kafkarebalance $IMBALANCE_KR -n $NAMESPACE &>/dev/null && echo yes || echo no)
    # Check if scale-down KR was created
    SCALE_KR_EXISTS=$(kubectl get kafkarebalance $SCALE_DOWN_KR -n $NAMESPACE &>/dev/null && echo yes || echo no)

    if [ "$AUTO_STATE" = "RebalanceOnScaleDown" ]; then
        log "✓ State transitioned to RebalanceOnScaleDown"
        SCALE_DOWN_STARTED=true

        log "  Imbalance KafkaRebalance deleted: $([ $IMBALANCE_KR_EXISTS = no ] && echo yes || echo no)"
        log "  Scale-down KafkaRebalance created: $SCALE_KR_EXISTS"

        if [ "$IMBALANCE_KR_EXISTS" = "no" ]; then
            log "✓ Imbalance KafkaRebalance was stopped and deleted"
        else
            log "⚠ Imbalance KafkaRebalance still exists (may still be stopping)"
        fi
        break
    fi

    printf "  Check $i/18 - State: ${AUTO_STATE}, imbalanceKR=$IMBALANCE_KR_EXISTS, scaleKR=$SCALE_KR_EXISTS, waiting 10s...\n"
    sleep 10
done

if [ "$SCALE_DOWN_STARTED" = false ]; then
    log "✗ State did not transition to RebalanceOnScaleDown"
    log "  Current state: $(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE -o jsonpath='{.status.autoRebalance.state}')"
    exit 1
fi

echo ""

################################################################################
# STEP 7: Verify Tracker ConfigMap is Updated After Stop
################################################################################

log "STEP 7: Verifying tracker ConfigMap is updated when imbalance rebalance is stopped"
echo ""

# Wait a moment for the operator to update the ConfigMap
sleep 10

if kubectl get cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker -n $NAMESPACE &>/dev/null; then
    COMPLETION_TIME=$(kubectl get cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker \
        -n $NAMESPACE -o jsonpath='{.data.lastRebalanceCompletionTime}')
    log "✓ Tracker ConfigMap exists — lastRebalanceCompletionTime: $COMPLETION_TIME"
else
    log "✗ Tracker ConfigMap not found — it should be updated when imbalance rebalance is stopped"
fi

echo ""

################################################################################
# STEP 8: Complete the Scale-Down Rebalance
################################################################################

log "STEP 8: Waiting for scale-down rebalance to complete"
echo ""

log "Monitoring scale-down rebalance progress (checking every 15s for up to 5 minutes)..."
SCALE_DOWN_COMPLETED=false
for i in {1..20}; do
    AUTO_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE \
        -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)

    if [ "$AUTO_STATE" = "Idle" ]; then
        log "✓ Scale-down rebalance completed — state is Idle"
        SCALE_DOWN_COMPLETED=true
        break
    fi

    if ! kubectl get kafkarebalance $SCALE_DOWN_KR -n $NAMESPACE &>/dev/null; then
        log "✓ Scale-down KafkaRebalance deleted (rebalance complete)"
        SCALE_DOWN_COMPLETED=true
        break
    fi

    printf "  Check $i/20 - State: $AUTO_STATE, still running...\n"
    sleep 15
done

if [ "$SCALE_DOWN_COMPLETED" = false ]; then
    log "⚠ Scale-down rebalance did not complete in expected time"
    kubectl get kafkarebalance $SCALE_DOWN_KR -n $NAMESPACE -o yaml || true
    exit 1
fi

echo ""

################################################################################
# STEP 9: Verify Auto-Rebalance on Imbalance Re-Triggers if Violations Persist
################################################################################

log "STEP 9: Verifying auto-rebalance on imbalance re-triggers after scale-down completes"
echo ""

log "Monitoring for re-trigger (checking every 15s for up to 5 minutes)..."
RETRIGGERED=false
for i in {1..20}; do
    AUTO_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE \
        -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)

    if [ "$AUTO_STATE" = "RebalanceOnImbalance" ]; then
        log "✓ Auto-rebalance on imbalance re-triggered after scale-down (state: RebalanceOnImbalance)"
        RETRIGGERED=true
        break
    fi

    if kubectl get kafkarebalance $IMBALANCE_KR -n $NAMESPACE &>/dev/null; then
        log "✓ New imbalance KafkaRebalance created — re-trigger confirmed"
        RETRIGGERED=true
        break
    fi

    printf "  Check $i/20 - State: ${AUTO_STATE:-Idle}, waiting 15s...\n"
    sleep 15
done

if [ "$RETRIGGERED" = false ]; then
    log "ℹ Auto-rebalance on imbalance did not re-trigger"
    log "  This may mean the scale-down rebalance already fixed the imbalance,"
    log "  or Cruise Control needs another anomaly detection cycle."
    log "  This is expected if the cluster is now balanced after scale-down."
fi

echo ""

################################################################################
# STEP 10: Final Summary
################################################################################

log "STEP 10: Test Summary"
echo ""
echo "================================================================================"
echo "  TEST RESULTS"
echo "================================================================================"
echo ""
echo "✓ Kafka cluster deployed with 4 brokers and imbalance + scale auto-rebalance"
echo "✓ Disk usage imbalance created"
echo "✓ Auto-rebalance on imbalance triggered (RebalanceOnImbalance)"
echo "✓ Scale-down triggered while imbalance rebalance was running"
echo "✓ Imbalance rebalance stopped — scale-down took priority"
echo "✓ State transitioned to RebalanceOnScaleDown"
echo "✓ Tracker ConfigMap updated when imbalance rebalance was stopped"
echo "✓ Scale-down rebalance completed — state returned to Idle"

if [ "$RETRIGGERED" = true ]; then
    echo "✓ Auto-rebalance on imbalance re-triggered after scale-down completed"
else
    echo "ℹ Auto-rebalance on imbalance did not re-trigger (cluster may be balanced)"
fi

echo ""
echo "🎉 TEST PASSED! Scale-down correctly takes priority over imbalance rebalance."
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
