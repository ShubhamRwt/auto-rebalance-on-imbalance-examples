#!/bin/bash
# Cleans up all resources created by the auto-rebalance example scripts.
# Run from the repo root or any example sub-directory.
#
# Usage: ./cleanup.sh [--namespace <ns>] [--delete-namespace]
#
#   --namespace <ns>      Namespace to clean up (default: myproject)
#   --delete-namespace    Also delete the namespace itself

set -euo pipefail

NAMESPACE="myproject"
DELETE_NAMESPACE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --namespace|-n) NAMESPACE="$2"; shift 2 ;;
        --delete-namespace) DELETE_NAMESPACE=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Resolve script dir so relative paths to manifests work regardless of cwd
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

timestamp() { date +"%H:%M:%S"; }
log()  { echo "[$(timestamp)] $1"; }
skip() { echo "  (skipped — not found)"; }

if ! kubectl get namespace "$NAMESPACE" &>/dev/null; then
    log "Namespace '$NAMESPACE' does not exist — nothing to clean up."
    exit 0
fi

log "Cleaning up auto-rebalance examples in namespace: $NAMESPACE"
echo ""

################################################################################
# Kafka resources (all examples)
################################################################################

log "Deleting KafkaRebalance resources..."
if kubectl get kafkarebalance -n "$NAMESPACE" --no-headers 2>/dev/null | grep -q .; then
    kubectl delete kafkarebalance --all -n "$NAMESPACE" --ignore-not-found=true
else
    skip
fi

log "Deleting Kafka cluster 'test-cluster'..."
if kubectl get kafka test-cluster -n "$NAMESPACE" &>/dev/null; then
    kubectl delete kafka test-cluster -n "$NAMESPACE" --ignore-not-found=true
else
    skip
fi

log "Deleting auto-rebalance tracker ConfigMap..."
if kubectl get cm test-cluster-auto-rebalance-imbalance-tracker -n "$NAMESPACE" &>/dev/null; then
    kubectl delete cm test-cluster-auto-rebalance-imbalance-tracker -n "$NAMESPACE" --ignore-not-found=true
else
    skip
fi

################################################################################
# test-frequent-rebalance-alerts: Prometheus stack
################################################################################

PROM_DIR="$SCRIPT_DIR/test-frequent-rebalance-alerts/prometheus"
log "Deleting Prometheus resources (test-frequent-rebalance-alerts)..."
if kubectl get deployment prometheus -n "$NAMESPACE" &>/dev/null; then
    kubectl delete -f "$PROM_DIR/prometheus-config.yaml" -n "$NAMESPACE" --ignore-not-found=true
else
    skip
fi

################################################################################
# test-metrics: kube-state-metrics
################################################################################

KSM_DIR="$SCRIPT_DIR/test-metrics/kube-state-metrics"
log "Deleting kube-state-metrics resources (test-metrics)..."
if kubectl get deployment kube-state-metrics -n "$NAMESPACE" &>/dev/null; then
    kubectl delete -f "$KSM_DIR/" -n "$NAMESPACE" --ignore-not-found=true
else
    skip
fi

################################################################################
# Strimzi cluster operator (all examples share the same operator deployment)
################################################################################

OPERATOR_DIR="$SCRIPT_DIR/test-auto-rebalance-trigger/cluster-operator"
log "Deleting Strimzi cluster operator..."
if kubectl get deployment strimzi-cluster-operator -n "$NAMESPACE" &>/dev/null; then
    kubectl delete -f "$OPERATOR_DIR" -n "$NAMESPACE" --ignore-not-found=true
else
    skip
fi

################################################################################
# Optional: delete the namespace itself
################################################################################

echo ""
if [ "$DELETE_NAMESPACE" = true ]; then
    log "Deleting namespace '$NAMESPACE'..."
    kubectl delete namespace "$NAMESPACE" --ignore-not-found=true
    log "Done."
else
    log "Done. Namespace '$NAMESPACE' retained (pass --delete-namespace to remove it)."
fi
