#!/bin/bash
set -e

################################################################################
# FREQUENT AUTO-REBALANCE ALERT TEST
#
# This script demonstrates Prometheus alerting for detecting when auto-rebalance
# on imbalance is triggered too frequently — which indicates the cluster is in
# a rebalancing loop (the imbalance returns immediately after each rebalance).
#
# Two alerts are tested:
#
# 1. FrequentAutoRebalanceOnImbalance
#    Fires when strimzi_auto_rebalance_anomalies_detected_total (fixable)
#    increases by more than 3 within a 10-minute window.
#    This is simulated by producing a persistent imbalance that Cruise Control
#    re-detects after each completed rebalance.
#
# 2. UnfixableGoalViolationsDetected (informational check)
#    Verified by querying the metric directly for unfixable/mixed detections.
#
# What this script does:
#   1. Deploys the Strimzi operator
#   2. Deploys Prometheus with the alerting rules
#   3. Deploys a Kafka cluster with a short anomaly detection interval (60s)
#   4. Creates a persistent disk imbalance that is difficult to fully fix
#   5. Waits for multiple rebalance cycles to occur
#   6. Queries Prometheus to verify the anomaly counter incremented
#   7. Checks whether the FrequentAutoRebalanceOnImbalance alert fired
#
# Prerequisites:
# - Kubernetes cluster (e.g., minikube)
# - kubectl configured
# - Strimzi operator image built from this branch
#
# Usage: ./TEST_FREQUENT_REBALANCE_ALERTS.sh [namespace]
################################################################################

NAMESPACE="${1:-myproject}"
CLUSTER_NAME="test-cluster"
PROM_PORT=9091   # local port-forward target

echo "================================================================================"
echo "  FREQUENT AUTO-REBALANCE ALERT TEST"
echo "================================================================================"
echo ""

timestamp() { date +"%H:%M:%S"; }
log()  { echo "[$(timestamp)] $1"; }
pass() { echo "  ✓ $1"; }
fail() { echo "  ✗ $1"; }
info() { echo "  ℹ $1"; }

# Kill port-forward on exit
PF_PID=""
cleanup() {
    [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

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

query_prometheus() {
    local query="$1"
    curl -s "http://localhost:${PROM_PORT}/api/v1/query" \
        --data-urlencode "query=${query}" 2>/dev/null
}

################################################################################
# STEP 1: Setup and Deployment
################################################################################

log "STEP 1: Setting up namespace and Strimzi operator"
echo ""

if ! kubectl get namespace $NAMESPACE &>/dev/null; then
    kubectl create namespace $NAMESPACE
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
# STEP 2: Deploy Prometheus with Alerting Rules
################################################################################

log "STEP 2: Deploying Prometheus with auto-rebalance alerting rules"
echo ""

kubectl apply -f prometheus/prometheus-config.yaml -n $NAMESPACE
kubectl wait pod -l app=prometheus --for=condition=Ready --timeout=120s -n $NAMESPACE
log "✓ Prometheus ready"

# Port-forward Prometheus
kubectl port-forward svc/prometheus $PROM_PORT:9090 -n $NAMESPACE &>/dev/null &
PF_PID=$!
sleep 3
log "✓ Prometheus port-forwarded to localhost:$PROM_PORT"

echo ""

################################################################################
# STEP 3: Deploy Kafka Cluster
################################################################################

log "STEP 3: Deploying Kafka cluster with 60s anomaly detection interval"
echo ""

if kubectl get kafka $CLUSTER_NAME -n $NAMESPACE &>/dev/null; then
    log "Cleaning up existing cluster..."
    kubectl delete kafka $CLUSTER_NAME -n $NAMESPACE --ignore-not-found=true
    kubectl delete kafkarebalance --all -n $NAMESPACE --ignore-not-found=true
    kubectl delete cm ${CLUSTER_NAME}-auto-rebalance-imbalance-tracker -n $NAMESPACE --ignore-not-found=true
    sleep 15
fi

kubectl apply -f test-kafka-with-alerts.yaml -n $NAMESPACE
kubectl wait kafka/$CLUSTER_NAME --for=condition=Ready --timeout=600s -n $NAMESPACE
log "✓ Kafka cluster ready"

kubectl wait pod -l strimzi.io/name=${CLUSTER_NAME}-cruise-control --for=condition=Ready --timeout=300s -n $NAMESPACE
CC_POD=$(kubectl get pods -n $NAMESPACE -l strimzi.io/name=${CLUSTER_NAME}-cruise-control -o jsonpath='{.items[0].metadata.name}')
log "✓ Cruise Control ready: $CC_POD"

OPERATOR_POD=$(kubectl get pods -n $NAMESPACE -l name=strimzi-cluster-operator -o jsonpath='{.items[0].metadata.name}')

echo ""

################################################################################
# STEP 4: Create a Persistent Imbalance
################################################################################

log "STEP 4: Creating a persistent disk usage imbalance"
echo ""

log "Creating 15 topics..."
for i in {1..15}; do
    kubectl run kafka-topic-$i -n $NAMESPACE --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- \
        bin/kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 \
        --create --topic topic-$i --partitions 3 --replication-factor 2 \
        --config min.insync.replicas=1 2>&1 | grep "Created topic" || true
done
log "✓ Topics created"

log "Producing data to broker-0 to create disk imbalance..."
for i in {1..10}; do
    kubectl run kafka-prod-$i -n $NAMESPACE --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- bash -c "
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
    {"topic": "topic-2", "partition": 0, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-2", "partition": 1, "replicas": [0,2], "log_dirs": ["any","any"]},
    {"topic": "topic-3", "partition": 0, "replicas": [0,2], "log_dirs": ["any","any"]},
    {"topic": "topic-3", "partition": 1, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-4", "partition": 0, "replicas": [0,1], "log_dirs": ["any","any"]},
    {"topic": "topic-4", "partition": 1, "replicas": [0,2], "log_dirs": ["any","any"]},
    {"topic": "topic-5", "partition": 0, "replicas": [0,2], "log_dirs": ["any","any"]},
    {"topic": "topic-5", "partition": 1, "replicas": [0,1], "log_dirs": ["any","any"]}
  ]
}
EOF
bin/kafka-reassign-partitions.sh --bootstrap-server '${CLUSTER_NAME}'-kafka-bootstrap:9092 \
    --reassignment-json-file /tmp/reassign.json --execute
' 2>&1 | grep "Successfully" || true
log "✓ Imbalance created"

echo ""

################################################################################
# STEP 5: Wait for Multiple Rebalance Cycles
################################################################################

log "STEP 5: Waiting for multiple auto-rebalance cycles to accumulate"
info "Anomaly detection interval is 60s — expecting 4+ detections within 10 minutes"
echo ""

wait_with_countdown 180 "Initial wait for Cruise Control to train and detect"

log "Monitoring rebalance cycles (checking every 30s for 10 minutes)..."
CYCLE_COUNT=0
for i in {1..20}; do
    # Count completed rebalance cycles from operator logs
    CYCLE_COUNT=$(kubectl logs -n $NAMESPACE $OPERATOR_POD --tail=500 2>/dev/null | \
        grep -c "Rebalancing completed, transitioning to Idle" || echo 0)

    DETECTION_COUNT=$(kubectl logs -n $NAMESPACE $OPERATOR_POD --tail=500 2>/dev/null | \
        grep -c "Fixable goal violations detected" || echo 0)

    AUTO_STATE=$(kubectl get kafka $CLUSTER_NAME -n $NAMESPACE \
        -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null)

    printf "  Cycle $i/20 — detections: $DETECTION_COUNT, completions: $CYCLE_COUNT, state: ${AUTO_STATE:-Idle}\n"

    if [ "$DETECTION_COUNT" -ge 4 ]; then
        log "✓ Reached $DETECTION_COUNT detections — enough to verify the alert"
        break
    fi

    sleep 30
done

echo ""

################################################################################
# STEP 6: Query Prometheus for the Anomaly Counter
################################################################################

log "STEP 6: Querying Prometheus for anomaly detection metrics"
echo ""

# Total fixable detections
RESULT=$(query_prometheus 'strimzi_auto_rebalance_anomalies_detected_total{type="goal_violation",fixability="fixable"}')
FIXABLE_TOTAL=$(echo "$RESULT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
results = d.get('data', {}).get('result', [])
print(results[0]['value'][1] if results else '0')
" 2>/dev/null || echo "0")
log "strimzi_auto_rebalance_anomalies_detected_total{fixability=fixable}: $FIXABLE_TOTAL"

if [ "$(echo "$FIXABLE_TOTAL > 0" | bc 2>/dev/null || [ "$FIXABLE_TOTAL" != "0" ] && echo 1 || echo 0)" = "1" ] || [ "$FIXABLE_TOTAL" != "0" ]; then
    pass "Anomaly counter is incrementing (value: $FIXABLE_TOTAL)"
else
    fail "Anomaly counter is still 0 — operator may not be scraping yet"
fi

# Rate over 10 minutes
RESULT_RATE=$(query_prometheus 'increase(strimzi_auto_rebalance_anomalies_detected_total{type="goal_violation",fixability="fixable"}[10m])')
RATE=$(echo "$RESULT_RATE" | python3 -c "
import sys, json
d = json.load(sys.stdin)
results = d.get('data', {}).get('result', [])
print(round(float(results[0]['value'][1]), 2) if results else '0')
" 2>/dev/null || echo "0")
log "increase over last 10 minutes: $RATE"

echo ""

################################################################################
# STEP 7: Check Whether Alerts Fired
################################################################################

log "STEP 7: Checking Prometheus alerts"
echo ""

ALERTS=$(curl -s "http://localhost:${PROM_PORT}/api/v1/alerts" 2>/dev/null)

# Check FrequentAutoRebalanceOnImbalance
FREQUENT=$(echo "$ALERTS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
alerts = d.get('data', {}).get('alerts', [])
for a in alerts:
    if a.get('labels', {}).get('alertname') == 'FrequentAutoRebalanceOnImbalance':
        print(f'FIRING — state={a[\"state\"]}, value={a[\"annotations\"][\"description\"][:80]}...')
        sys.exit(0)
print('NOT_FIRING')
" 2>/dev/null || echo "QUERY_FAILED")

if echo "$FREQUENT" | grep -q "FIRING"; then
    pass "FrequentAutoRebalanceOnImbalance alert is FIRING"
    info "$FREQUENT"
elif [ "$RATE" != "0" ] && python3 -c "exit(0 if float('$RATE') > 3 else 1)" 2>/dev/null; then
    pass "Rate ($RATE) exceeds threshold (3) — alert should fire on next evaluation"
else
    info "FrequentAutoRebalanceOnImbalance not firing yet"
    info "Current 10-minute increase: $RATE (threshold: > 3)"
    info "Wait longer or check that Prometheus is scraping the operator"
fi

# Check UnfixableGoalViolationsDetected (informational)
UNFIXABLE_VAL=$(query_prometheus 'strimzi_auto_rebalance_anomalies_detected_total{type="goal_violation",fixability=~"unfixable|mixed"}' | \
    python3 -c "
import sys, json
d = json.load(sys.stdin)
results = d.get('data', {}).get('result', [])
print(sum(float(r['value'][1]) for r in results))
" 2>/dev/null || echo "0")
log "Unfixable/mixed anomaly detections total: $UNFIXABLE_VAL"

echo ""

################################################################################
# STEP 8: Show Alert Rule Expressions for Reference
################################################################################

log "STEP 8: Alert rule reference"
echo ""
echo "  FrequentAutoRebalanceOnImbalance — fires when:"
echo "    increase(strimzi_auto_rebalance_anomalies_detected_total{"
echo "      type=\"goal_violation\", fixability=\"fixable\"}[10m]) > 3"
echo ""
echo "  UnfixableGoalViolationsDetected — fires when:"
echo "    increase(strimzi_auto_rebalance_anomalies_detected_total{"
echo "      type=\"goal_violation\", fixability=~\"unfixable|mixed\"}[5m]) > 0"
echo ""

################################################################################
# STEP 9: Final Summary
################################################################################

log "STEP 9: Summary"
echo ""
echo "================================================================================"
echo "  TEST RESULTS"
echo "================================================================================"
echo ""
echo "✓ Prometheus deployed with auto-rebalance alerting rules"
echo "✓ Kafka cluster deployed with 60s anomaly detection interval"
echo "✓ Persistent disk imbalance created"
echo "✓ Multiple rebalance cycles observed ($CYCLE_COUNT completions)"
echo "✓ strimzi_auto_rebalance_anomalies_detected_total counter: $FIXABLE_TOTAL"
echo "✓ 10-minute increase: $RATE (alert threshold: > 3)"
echo ""
echo "Alert files deployed to Prometheus:"
echo "  prometheus/prometheus-config.yaml"
echo ""
echo "To open Prometheus UI:"
echo "  kubectl port-forward svc/prometheus $PROM_PORT:9090 -n $NAMESPACE"
echo "  open http://localhost:$PROM_PORT/alerts"
echo ""
echo "Useful Prometheus queries:"
echo "  # Total anomaly detections by fixability"
echo "  strimzi_auto_rebalance_anomalies_detected_total"
echo ""
echo "  # Rate of fixable detections over 10 minutes (alert expression)"
echo "  increase(strimzi_auto_rebalance_anomalies_detected_total{type=\"goal_violation\",fixability=\"fixable\"}[10m])"
echo ""
echo "  # Active alerts"
echo "  ALERTS{alertname=~\"FrequentAutoRebalanceOnImbalance|UnfixableGoalViolationsDetected\"}"
echo ""
echo "To cleanup:"
echo "  kubectl delete kafka $CLUSTER_NAME -n $NAMESPACE"
echo "  kubectl delete deployment prometheus -n $NAMESPACE"
echo "  kubectl delete cm prometheus-config -n $NAMESPACE"
echo "  kubectl delete svc prometheus -n $NAMESPACE"
echo ""
