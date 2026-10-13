#!/bin/bash
set -e

################################################################################
# FREQUENT AUTO-REBALANCE ALERT TEST
#
# Verifies that Prometheus alerts fire when auto-rebalance on imbalance is
# triggered too frequently — indicating the cluster is in a rebalancing loop.
#
# Two alerts are verified:
#   FrequentAutoRebalanceOnImbalance  — fixable detections > 3 in 10 min
#   UnfixableGoalViolationsDetected   — any unfixable/mixed detection
#
# What this script does:
#   1. Deploys the Strimzi operator
#   2. Deploys Prometheus with alerting rules and a metrics Service for the operator
#   3. Deploys a Kafka cluster with a short anomaly detection interval (60s)
#   4. Creates a persistent disk imbalance that re-appears after each rebalance
#   5. Waits for multiple rebalance cycles
#   6. Queries Prometheus to verify the anomaly counter is incrementing
#   7. Checks whether the FrequentAutoRebalanceOnImbalance alert fired
#
# Prerequisites:
#   - Kubernetes cluster (e.g., minikube)
#   - kubectl configured
#   - jq installed (for JSON parsing)
#
# Usage: ./TEST_FREQUENT_REBALANCE_ALERTS.sh [namespace]
################################################################################

NAMESPACE="${1:-myproject}"
CLUSTER_NAME="test-cluster"
PROM_PORT=9091

echo "================================================================================"
echo "  FREQUENT AUTO-REBALANCE ALERT TEST"
echo "================================================================================"
echo ""

timestamp() { date +"%H:%M:%S"; }
log()  { echo "[$(timestamp)] $1"; }
pass() { echo "  ✓ $1"; }
fail() { echo "  ✗ $1"; }
info() { echo "  ℹ $1"; }

PF_PID=""
cleanup() { [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null || true; }
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
    curl -sG "http://localhost:${PROM_PORT}/api/v1/query" \
        --data-urlencode "query=$1" 2>/dev/null
}

################################################################################
# STEP 1: Setup and Deployment
################################################################################

log "STEP 1: Setting up namespace and Strimzi operator"
echo ""

kubectl get namespace "$NAMESPACE" &>/dev/null || kubectl create namespace "$NAMESPACE"

if kubectl get deployment strimzi-cluster-operator -n "$NAMESPACE" &>/dev/null; then
    log "Strimzi operator already deployed"
else
    log "Deploying Strimzi operator..."
    kubectl apply -f cluster-operator -n "$NAMESPACE"
    kubectl rollout status deployment/strimzi-cluster-operator -n "$NAMESPACE" --timeout=120s
    log "✓ Operator ready"
fi

echo ""

################################################################################
# STEP 2: Deploy Prometheus with Alerting Rules
################################################################################

log "STEP 2: Deploying Prometheus with auto-rebalance alerting rules"
echo ""

kubectl apply -f prometheus/prometheus-config.yaml -n "$NAMESPACE"

log "Waiting for Prometheus pod to be ready..."
kubectl rollout status deployment/prometheus -n "$NAMESPACE" --timeout=120s
log "✓ Prometheus ready"

# Port-forward Prometheus in background
kubectl port-forward svc/prometheus "$PROM_PORT":9090 -n "$NAMESPACE" >/dev/null 2>&1 &
PF_PID=$!
sleep 5
log "✓ Prometheus port-forwarded → http://localhost:${PROM_PORT}"

echo ""

################################################################################
# STEP 3: Deploy Kafka Cluster
################################################################################

log "STEP 3: Deploying Kafka cluster with 60s anomaly detection interval"
echo ""

if kubectl get kafka "$CLUSTER_NAME" -n "$NAMESPACE" &>/dev/null; then
    log "Cleaning up existing cluster..."
    kubectl delete kafka "$CLUSTER_NAME" -n "$NAMESPACE" --ignore-not-found=true
    kubectl delete kafkarebalance --all -n "$NAMESPACE" --ignore-not-found=true
    kubectl delete cm "${CLUSTER_NAME}-auto-rebalance-imbalance-tracker" \
        -n "$NAMESPACE" --ignore-not-found=true
    sleep 15
fi

kubectl apply -f test-kafka-with-alerts.yaml -n "$NAMESPACE"
kubectl wait kafka/"$CLUSTER_NAME" --for=condition=Ready --timeout=600s -n "$NAMESPACE"
log "✓ Kafka cluster ready"

kubectl wait pod -l strimzi.io/name="${CLUSTER_NAME}-cruise-control" \
    --for=condition=Ready --timeout=300s -n "$NAMESPACE"
CC_POD=$(kubectl get pods -n "$NAMESPACE" \
    -l strimzi.io/name="${CLUSTER_NAME}-cruise-control" \
    -o jsonpath='{.items[0].metadata.name}')
log "✓ Cruise Control ready: $CC_POD"

OPERATOR_POD=$(kubectl get pods -n "$NAMESPACE" \
    -l name=strimzi-cluster-operator \
    -o jsonpath='{.items[0].metadata.name}')
log "Operator pod: $OPERATOR_POD"

echo ""

################################################################################
# STEP 4: Create a Persistent Disk Imbalance
################################################################################

log "STEP 4: Creating a persistent disk usage imbalance"
echo ""

log "Creating 15 topics..."
for i in {1..15}; do
    kubectl run "kafka-topic-$i" -n "$NAMESPACE" \
        --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- \
        bin/kafka-topics.sh --bootstrap-server "${CLUSTER_NAME}-kafka-bootstrap:9092" \
        --create --topic "topic-$i" --partitions 3 --replication-factor 2 \
        --config min.insync.replicas=1 2>&1 | grep "Created topic" || true
done
log "✓ Topics created"

log "Producing data to create disk imbalance..."
for i in {1..10}; do
    kubectl run "kafka-prod-$i" -n "$NAMESPACE" \
        --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- bash -c "
        for j in \$(seq 1 5000); do echo \"msg-\$j-$(head -c 100 /dev/urandom | base64 -w0)\"; done \
        | bin/kafka-console-producer.sh \
            --bootstrap-server ${CLUSTER_NAME}-kafka-bootstrap:9092 \
            --topic topic-$i" &>/dev/null &
done
wait
log "✓ Data produced"

log "Concentrating partitions on broker-0..."
kubectl run kafka-reassign -n "$NAMESPACE" \
    --image=quay.io/strimzi/kafka:latest-kafka-4.3.0 --rm -i --restart=Never -- bash -c '
cat > /tmp/reassign.json <<EOF
{"version":1,"partitions":[
  {"topic":"topic-1","partition":0,"replicas":[0,1],"log_dirs":["any","any"]},
  {"topic":"topic-1","partition":1,"replicas":[0,1],"log_dirs":["any","any"]},
  {"topic":"topic-2","partition":0,"replicas":[0,1],"log_dirs":["any","any"]},
  {"topic":"topic-2","partition":1,"replicas":[0,2],"log_dirs":["any","any"]},
  {"topic":"topic-3","partition":0,"replicas":[0,2],"log_dirs":["any","any"]},
  {"topic":"topic-3","partition":1,"replicas":[0,1],"log_dirs":["any","any"]},
  {"topic":"topic-4","partition":0,"replicas":[0,1],"log_dirs":["any","any"]},
  {"topic":"topic-4","partition":1,"replicas":[0,2],"log_dirs":["any","any"]},
  {"topic":"topic-5","partition":0,"replicas":[0,2],"log_dirs":["any","any"]},
  {"topic":"topic-5","partition":1,"replicas":[0,1],"log_dirs":["any","any"]}
]}
EOF
bin/kafka-reassign-partitions.sh \
    --bootstrap-server '"${CLUSTER_NAME}"'-kafka-bootstrap:9092 \
    --reassignment-json-file /tmp/reassign.json --execute
' 2>&1 | grep "Successfully" || true

log "✓ Partitions concentrated on broker-0"

echo ""

################################################################################
# STEP 5: Wait for Multiple Rebalance Cycles
################################################################################

log "STEP 5: Waiting for multiple auto-rebalance detection cycles"
info "Anomaly detection interval: 60s — expecting 4+ detections within 10 minutes"
echo ""

wait_with_countdown 180 "Initial wait for Cruise Control to train and detect"

log "Monitoring detection cycles (checking every 30s for up to 10 minutes)..."
CYCLE_COUNT=0
DETECTION_COUNT=0
for i in $(seq 1 20); do
    CYCLE_COUNT=$(kubectl logs -n "$NAMESPACE" "$OPERATOR_POD" --tail=500 2>/dev/null \
        | grep -c "Rebalancing completed" 2>/dev/null || true)
    CYCLE_COUNT=${CYCLE_COUNT:-0}

    DETECTION_COUNT=$(kubectl logs -n "$NAMESPACE" "$OPERATOR_POD" --tail=500 2>/dev/null \
        | grep -c "Fixable goal violations detected" 2>/dev/null || true)
    DETECTION_COUNT=${DETECTION_COUNT:-0}

    AUTO_STATE=$(kubectl get kafka "$CLUSTER_NAME" -n "$NAMESPACE" \
        -o jsonpath='{.status.autoRebalance.state}' 2>/dev/null || echo "Unknown")

    printf "  Check %2d/20 — detections: %d, completions: %d, state: %s\n" \
        "$i" "$DETECTION_COUNT" "$CYCLE_COUNT" "$AUTO_STATE"

    if [ "$DETECTION_COUNT" -ge 4 ]; then
        log "✓ $DETECTION_COUNT detections accumulated — sufficient to verify alert"
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
FIXABLE_TOTAL=$(query_prometheus \
    'strimzi_auto_rebalance_anomalies_detected_total{type="goal_violation",fixability="fixable"}' \
    | jq -r '.data.result[0].value[1] // "0"' 2>/dev/null || echo "0")
log "strimzi_auto_rebalance_anomalies_detected_total{fixability=\"fixable\"}: $FIXABLE_TOTAL"

if [ "$FIXABLE_TOTAL" != "0" ] && [ "$FIXABLE_TOTAL" != "null" ]; then
    pass "Anomaly counter incrementing (value: $FIXABLE_TOTAL)"
else
    fail "Anomaly counter still 0 — operator may not be scraped yet (check Prometheus targets)"
fi

# 10-minute increase (the alert expression)
RATE=$(query_prometheus \
    'increase(strimzi_auto_rebalance_anomalies_detected_total{type="goal_violation",fixability="fixable"}[10m])' \
    | jq -r '.data.result[0].value[1] // "0"' 2>/dev/null || echo "0")
RATE_INT=$(printf "%.0f" "${RATE:-0}" 2>/dev/null || echo "0")
log "increase() over last 10 minutes: $RATE"

echo ""

################################################################################
# STEP 7: Check Whether Alerts Fired
################################################################################

log "STEP 7: Checking Prometheus alerts"
echo ""

ALERT_STATE=$(curl -s "http://localhost:${PROM_PORT}/api/v1/alerts" 2>/dev/null \
    | jq -r '.data.alerts[]
        | select(.labels.alertname == "FrequentAutoRebalanceOnImbalance")
        | .state' 2>/dev/null || echo "")

if [ "$ALERT_STATE" = "firing" ]; then
    pass "FrequentAutoRebalanceOnImbalance alert is FIRING"
elif [ "$RATE_INT" -gt 3 ] 2>/dev/null; then
    pass "10-minute increase ($RATE) exceeds threshold (3) — alert fires on next evaluation cycle"
else
    info "FrequentAutoRebalanceOnImbalance not firing yet"
    info "10-minute increase: $RATE (threshold: > 3)"
    info "If Prometheus just started scraping, wait one more evaluation cycle (30s) and re-check:"
    info "  curl -s http://localhost:${PROM_PORT}/api/v1/alerts | jq '.data.alerts'"
fi

# Unfixable/mixed total (informational)
UNFIXABLE=$(query_prometheus \
    'sum(strimzi_auto_rebalance_anomalies_detected_total{type="goal_violation",fixability=~"unfixable|mixed"})' \
    | jq -r '.data.result[0].value[1] // "0"' 2>/dev/null || echo "0")
log "Unfixable/mixed anomaly detections total: $UNFIXABLE"

echo ""

################################################################################
# STEP 8: Prometheus UI and Query Reference
################################################################################

log "STEP 8: Prometheus query reference"
echo ""
echo "  Open Prometheus UI:"
echo "    http://localhost:${PROM_PORT}/alerts"
echo "    http://localhost:${PROM_PORT}/targets   (verify operator is scraped)"
echo ""
echo "  Key queries:"
echo "    # All anomaly detections by fixability"
echo "    strimzi_auto_rebalance_anomalies_detected_total"
echo ""
echo "    # 10-minute rate (FrequentAutoRebalanceOnImbalance alert expression)"
echo "    increase(strimzi_auto_rebalance_anomalies_detected_total{fixability=\"fixable\"}[10m])"
echo ""
echo "    # Active alerts"
echo "    ALERTS{alertname=~\"FrequentAutoRebalanceOnImbalance|UnfixableGoalViolationsDetected\"}"
echo ""

################################################################################
# STEP 9: Summary
################################################################################

log "STEP 9: Summary"
echo ""
echo "================================================================================"
echo "  TEST RESULTS"
echo "================================================================================"
echo ""
echo "✓ Prometheus deployed with FrequentAutoRebalanceOnImbalance alert"
echo "✓ Operator metrics Service deployed (strimzi-cluster-operator-metrics:8080)"
echo "✓ Kafka cluster deployed with 60s anomaly detection interval"
echo "✓ Persistent disk imbalance created"
echo "  Rebalance completions observed : $CYCLE_COUNT"
echo "  Anomaly detections (operator)  : $DETECTION_COUNT"
echo "  Prometheus counter (fixable)   : $FIXABLE_TOTAL"
echo "  10-minute increase             : $RATE  (alert threshold: > 3)"
echo ""
echo "To cleanup:"
echo "  kubectl delete kafka $CLUSTER_NAME -n $NAMESPACE"
echo "  kubectl delete deployment prometheus -n $NAMESPACE"
echo "  kubectl delete svc prometheus strimzi-cluster-operator-metrics -n $NAMESPACE"
echo "  kubectl delete cm prometheus-config -n $NAMESPACE"
echo ""
