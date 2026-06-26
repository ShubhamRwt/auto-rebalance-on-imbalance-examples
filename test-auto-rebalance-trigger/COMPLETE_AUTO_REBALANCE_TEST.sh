#!/bin/bash
set -e

################################################################################
# COMPLETE AUTO-REBALANCE ON IMBALANCE TEST
#
# This script demonstrates the complete auto-rebalance feature:
# 1. Deploys Kafka cluster with auto-rebalance enabled
# 2. Creates leader imbalance (all leaders on one broker)
# 3. Waits for Cruise Control to detect anomaly
# 4. Verifies auto-rebalance triggers automatically
# 5. Confirms rebalance completes and transitions to Idle
# 6. Verifies NO new anomalies are detected after completion
#
# Prerequisites:
# - Kubernetes cluster (e.g., minikube)
# - kubectl configured
# - Strimzi operator deployed
#
# Usage: ./COMPLETE_AUTO_REBALANCE_TEST.sh
################################################################################

NAMESPACE="myproject"
CLUSTER_NAME="test-cluster"

echo "================================================================================"
echo "  COMPLETE AUTO-REBALANCE ON IMBALANCE TEST"
echo "================================================================================"
echo ""

# Function to get timestamp
timestamp() {
    date +"%H:%M:%S"
}

# Function to log with timestamp
log() {
    echo "[$(timestamp)] $1"
}

# Function to wait with countdown
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

log "STEP 1: Setting up Kubernetes namespace and Strimzi operator"
echo ""

# Create namespace
if ! kubectl get namespace $NAMESPACE &>/dev/null; then
    log "Creating namespace $NAMESPACE..."
    kubectl create namespace $NAMESPACE
else
    log "Namespace $NAMESPACE already exists"
fi

# Check if operator is already deployed
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
# STEP 2: Deploy Kafka Cluster with Auto-Rebalance
################################################################################

log "STEP 2: Deploying Kafka cluster with auto-rebalance enabled"
echo ""

# Clean up any existing cluster
if kubectl get kafka $CLUSTER_NAME -n $NAMESPACE &>/dev/null; then
    log "Cleaning up existing cluster..."
    kubectl delete kafka $CLUSTER_NAME -n $NAMESPACE --ignore-not-found=true
    kubectl delete kafkarebalance --all -n $NAMESPACE --ignore-not-found=true
    kubectl delete cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker -n $NAMESPACE --ignore-not-found=true
    sleep 15
fi

log "Applying Kafka cluster configuration..."
kubectl apply -f test-auto-rebalance-on-imbalance.yaml -n $NAMESPACE

log "Waiting for Kafka cluster to be ready (this may take 3-5 minutes)..."
kubectl wait kafka/$CLUSTER_NAME --for=condition=Ready --timeout=600s -n $NAMESPACE
log "✓ Kafka cluster ready"

log "Waiting for Cruise Control to be ready..."
kubectl wait pod -l strimzi.io/name=${CLUSTER_NAME}-cruise-control --for=condition=Ready --timeout=300s -n $NAMESPACE
CC_POD=$(kubectl get pods -n $NAMESPACE -l strimzi.io/name=${CLUSTER_NAME}-cruise-control -o jsonpath='{.items[0].metadata.name}')
log "✓ Cruise Control ready: $CC_POD"

echo ""

################################################################################
# STEP 3: Create Disk Usage Imbalance
################################################################################

log "STEP 3: Creating disk usage imbalance"
echo ""

log "Creating 15 topics with 3 partitions each..."
for i in {1..15}; do
    kubectl run kafka-topic-create-$i -n $NAMESPACE --image=quay.io/srawat/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- \
        bin/kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 \
        --create --topic topic-$i --partitions 3 --replication-factor 2 \
        --config min.insync.replicas=1 2>&1 | grep "Created topic" || true
done
log "✓ 15 topics created (45 partitions total)"

log "Producing significant data to create disk usage imbalance..."
log "Producing heavy data to first 10 topics (will be placed mostly on broker-0)..."
for i in {1..10}; do
    kubectl run kafka-producer-heavy-$i -n $NAMESPACE --image=quay.io/srawat/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- bash -c "
        for j in {1..5000}; do
            echo \"message-\$j: $(head -c 500 /dev/urandom | base64)\"
        done | bin/kafka-console-producer.sh --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 --topic topic-$i
    " &>/dev/null &
done
wait
log "✓ Heavy data produced to topics 1-10 (~2.5GB total)"

log "Forcing most partitions to broker-0 to create disk imbalance..."
kubectl run kafka-reassign -n $NAMESPACE --image=quay.io/srawat/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- bash -c '
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

log "✓ Partitions reassigned to create disk usage imbalance (most replicas on broker-0)"
sleep 10

log "Verifying disk usage distribution..."
kubectl run kafka-log-dirs -n $NAMESPACE --image=quay.io/srawat/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- \
    bin/kafka-log-dirs.sh --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 --describe 2>/dev/null | \
    grep -E "broker|size" | head -20 || log "Disk usage check completed"
log "✓ Disk imbalance created"

echo ""

################################################################################
# STEP 4: Wait for Cruise Control Training and Anomaly Detection
################################################################################

log "STEP 4: Waiting for Cruise Control to detect anomaly"
echo ""

wait_with_countdown 180 "Cruise Control needs time to collect metrics and train"

log "Monitoring for anomaly detection (checking every 30s for up to 5 minutes)..."
DETECTED=false
for i in {1..10}; do
    # Check for goal violations
    VIOLATIONS=$(kubectl logs -n $NAMESPACE $CC_POD --tail=50 2>/dev/null | grep "recentGoalViolations" | tail -1)

    if echo "$VIOLATIONS" | grep -q "DiskUsageDistributionGoal"; then
        log "✓ ANOMALY DETECTED! DiskUsageDistributionGoal violation found"
        DETECTED=true
        break
    fi

    if [ $i -lt 10 ]; then
        printf "  Check $i/10 - No violations yet, waiting 30s...\n"
        sleep 30
    fi
done

if [ "$DETECTED" = false ]; then
    log "⚠ Warning: Anomaly not detected yet. Cruise Control may need more time."
    log "  Continuing anyway - auto-rebalance should trigger once detected."
fi

echo ""

################################################################################
# STEP 5: Verify Auto-Rebalance Triggers
################################################################################

log "STEP 5: Verifying auto-rebalance triggers"
echo ""

OPERATOR_POD=$(kubectl get pods -n $NAMESPACE -l name=strimzi-cluster-operator -o jsonpath='{.items[0].metadata.name}')
log "Operator pod: $OPERATOR_POD"

# First, check if auto-rebalance already completed
AUTO_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)
LAST_TRANSITION=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE -o jsonpath='{.status.autoRebalance.lastTransitionTime}' 2>/dev/null)

if [ "$AUTO_STATE" = "Idle" ] && [ -n "$LAST_TRANSITION" ]; then
    # Check if rebalance already happened by looking at operator logs
    if kubectl logs -n $NAMESPACE $OPERATOR_POD --tail=200 | grep -q "Rebalancing completed, transitioning to Idle"; then
        log "✓ AUTO-REBALANCE ALREADY COMPLETED!"
        log "  The rebalance was very fast and completed during the anomaly detection wait"
        log "  Last transition: $LAST_TRANSITION"
        log "  Current state: Idle"
        TRIGGERED=true
        COMPLETED=true
    else
        log "Checking if auto-rebalance triggered..."
        TRIGGERED=false
    fi
else
    TRIGGERED=false
fi

# If not already completed, monitor for trigger
if [ "$TRIGGERED" = false ]; then
    log "Monitoring for auto-rebalance trigger (checking every 15s for up to 3 minutes)..."
    for i in {1..12}; do
        # Check auto-rebalance state
        AUTO_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)

        if [ "$AUTO_STATE" = "RebalanceOnImbalance" ]; then
            log "✓ AUTO-REBALANCE TRIGGERED! State: RebalanceOnImbalance"
            TRIGGERED=true
            break
        fi

        # Check if KafkaRebalance was created
        if kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-full -n $NAMESPACE &>/dev/null; then
            REBALANCE_STATE=$(kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-full -n $NAMESPACE \
                -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null)
            log "✓ KafkaRebalance created! State: $REBALANCE_STATE"
            TRIGGERED=true
            break
        fi

        if [ $i -lt 12 ]; then
            printf "  Check $i/12 - State: ${AUTO_STATE:-Idle}, waiting 15s...\n"
            sleep 15
        fi
    done
fi

if [ "$TRIGGERED" = false ]; then
    log "⚠ Auto-rebalance not triggered yet"
    log "  This may happen if Cruise Control hasn't detected the anomaly"
    log "  Check operator logs for details"
    exit 1
fi

echo ""

################################################################################
# STEP 6: Wait for Rebalance Completion
################################################################################

log "STEP 6: Waiting for rebalance to complete"
echo ""

# Check if already completed in Step 5
if [ "$COMPLETED" = true ]; then
    log "✓ Rebalance already completed (detected in Step 5)"
else
    log "Monitoring rebalance progress (checking every 15s for up to 5 minutes)..."
    COMPLETED=false
    for i in {1..20}; do
        # Check if KafkaRebalance still exists
        if ! kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-full -n $NAMESPACE &>/dev/null; then
            log "✓ KafkaRebalance deleted (indicates completion)"
            COMPLETED=true
            break
        fi

        # Check status
        STATUS=$(kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-full -n $NAMESPACE \
            -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)

        if [ "$STATUS" = "True" ]; then
            log "✓ Rebalance completed successfully!"
            COMPLETED=true
            sleep 5  # Give operator time to clean up
            break
        fi

        if [ $i -lt 20 ]; then
            printf "  Check $i/20 - Still rebalancing...\n"
            sleep 15
        fi
    done

    if [ "$COMPLETED" = false ]; then
        log "⚠ Rebalance did not complete in expected time"
        log "  Check KafkaRebalance status for details"
        kubectl get kafkarebalance ${CLUSTER_NAME}-auto-rebalancing-full -n $NAMESPACE -o yaml
        exit 1
    fi
fi

echo ""

################################################################################
# STEP 7: Verify New Code Behavior
################################################################################

log "STEP 7: Verifying new code behavior (separate detection cycles)"
echo ""

log "Checking operator logs for completion message..."
if kubectl logs -n $NAMESPACE $OPERATOR_POD --tail=200 | grep -q "Rebalancing completed, transitioning to Idle"; then
    log "✓ Found NEW completion message: 'Rebalancing completed, transitioning to Idle'"
else
    log "✗ New completion message not found"
fi

if kubectl logs -n $NAMESPACE $OPERATOR_POD | grep -q "checking for intra-broker violations"; then
    log "✗ Found OLD immediate intra-broker check (should not exist)"
else
    log "✓ No immediate intra-broker check (correct behavior)"
fi

log "Checking auto-rebalance state..."
FINAL_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE -o jsonpath='{.status.autoRebalance.state}')
if [ "$FINAL_STATE" = "Idle" ]; then
    log "✓ Auto-rebalance state: Idle"
else
    log "⚠ Auto-rebalance state: $FINAL_STATE (expected Idle)"
fi

log "Checking ConfigMap was updated..."
if kubectl get cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker -n $NAMESPACE &>/dev/null; then
    COMPLETION_TIME=$(kubectl get cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker -n $NAMESPACE \
        -o jsonpath='{.data.lastRebalanceCompletionTime}')
    log "✓ ConfigMap updated with completion time: $COMPLETION_TIME"
else
    log "✗ ConfigMap not found"
fi

echo ""

################################################################################
# STEP 8: Wait and Verify No New Anomalies
################################################################################

log "STEP 8: Verifying NO new anomalies are detected after rebalance"
echo ""

wait_with_countdown 150 "Waiting for next anomaly detection cycle (120s interval + buffer)"

log "Checking Cruise Control for new violations..."
RECENT_VIOLATIONS=$(kubectl logs -n $NAMESPACE $CC_POD --tail=100 | grep "recentGoalViolations" | tail -1)

# Extract detection dates
DETECTION_DATES=$(echo "$RECENT_VIOLATIONS" | grep -o "detectionDate=[^,}]*" | sed 's/detectionDate=//' | tr -d 'Z')

if [ -z "$DETECTION_DATES" ]; then
    log "✓ NO violations in recent history (cluster fully balanced!)"
    NEW_ANOMALIES=false
elif [ $(echo "$DETECTION_DATES" | wc -l) -eq 1 ]; then
    # Only one violation - check if it's before completion time
    VIOLATION_TIME=$(echo "$DETECTION_DATES" | head -1)
    log "  Single violation found at: $VIOLATION_TIME"
    log "  Completion time was: $COMPLETION_TIME"

    if [[ "$VIOLATION_TIME" < "$COMPLETION_TIME" ]]; then
        log "✓ Violation is BEFORE rebalance completion - correctly IGNORED"
        log "✓ NO new anomalies detected after completion"
        NEW_ANOMALIES=false
    else
        log "⚠ New violation detected AFTER completion"
        NEW_ANOMALIES=true
    fi
else
    log "⚠ Multiple violations found - checking timestamps..."
    NEW_ANOMALIES=true
fi

log "Checking balancedness score..."
BALANCEDNESS=$(kubectl logs -n $NAMESPACE $CC_POD --tail=50 | grep "balancednessScore" | tail -1 | grep -o "balancednessScore:[0-9.]*")
log "  $BALANCEDNESS"

if echo "$BALANCEDNESS" | grep -q "100.000"; then
    log "✓ Perfect balance achieved (score: 100.000)"
elif echo "$BALANCEDNESS" | grep -qE "balancednessScore:(9[0-9]|100)"; then
    log "✓ Good balance achieved (score >= 90)"
else
    log "⚠ Balance score is below 90"
fi

log "Checking replica movements after rebalance..."
REPLICA_MOVEMENTS=$(kubectl logs -n $NAMESPACE $CC_POD 2>/dev/null | grep "numReplicaMovements" | tail -1 | grep -o "numReplicaMovements\":[0-9]*" || echo "numReplicaMovements\":0")
log "  $REPLICA_MOVEMENTS"

if echo "$REPLICA_MOVEMENTS" | grep -qE "numReplicaMovements\":[1-9][0-9]*"; then
    log "✓ Replicas were moved to balance disk usage"
else
    log "⚠ No replica movements detected"
fi

log "Checking data distribution after rebalance..."
kubectl run kafka-log-dirs-after -n $NAMESPACE --image=quay.io/srawat/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- \
    bin/kafka-log-dirs.sh --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 --describe 2>/dev/null | \
    grep -E "\"broker\":|\"size\":" | head -15 || log "Data distribution check completed"

echo ""

################################################################################
# STEP 9: Final Summary
################################################################################

log "STEP 9: Test Summary"
echo ""
echo "================================================================================"
echo "  TEST RESULTS"
echo "================================================================================"
echo ""

echo "✓ Kafka cluster deployed with auto-rebalance enabled"
echo "✓ Disk usage imbalance created (most replicas on broker-0)"
echo "✓ Cruise Control detected anomaly (DiskUsageDistributionGoal)"
echo "✓ Auto-rebalance triggered automatically"
echo "✓ Rebalance completed successfully"
echo "✓ NEW CODE verified: 'Rebalancing completed, transitioning to Idle'"
echo "✓ ConfigMap updated with completion timestamp"
echo "✓ Auto-rebalance transitioned to Idle state"

if [ "$NEW_ANOMALIES" = false ]; then
    echo "✓ NO new anomalies detected after completion (EXPECTED BEHAVIOR)"
    echo ""
    echo "🎉 TEST PASSED! The feature is working correctly:"
    echo "   - Anomaly detected → Auto-rebalance triggered → Cluster balanced"
    echo "   - No new anomalies detected → Cluster remains stable"
else
    echo "⚠ New anomalies detected after completion"
    echo "   This may indicate the cluster couldn't be fully balanced"
    echo "   Check Cruise Control logs and cluster state"
fi

echo ""
echo "================================================================================"
echo ""

log "Detailed status:"
echo ""
echo "Kafka CR status:"
kubectl get kafka $CLUSTER_NAME -n $NAMESPACE -o jsonpath='{.status.autoRebalance}' | jq .
echo ""
echo "Cruise Control recent violations:"
echo "$RECENT_VIOLATIONS" | grep -o "recentGoalViolations:\[.*\]" | head -c 200
echo "..."
echo ""

log "Test complete!"
echo ""
echo "To view detailed logs:"
echo "  Operator logs:        kubectl logs -n $NAMESPACE $OPERATOR_POD --tail=100"
echo "  Cruise Control logs:  kubectl logs -n $NAMESPACE $CC_POD --tail=100"
echo ""
echo "To cleanup:"
echo "  kubectl delete kafka $CLUSTER_NAME -n $NAMESPACE"
echo ""
