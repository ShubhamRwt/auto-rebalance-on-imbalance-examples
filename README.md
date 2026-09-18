# auto-rebalance-on-imbalance-examples

Examples for testing the auto-rebalance on imbalance feature for Strimzi.

## Examples

### test-auto-rebalance-trigger

Tests the core auto-rebalance on imbalance flow:
1. Deploy a Kafka cluster with the `imbalance` auto-rebalance mode enabled
2. Create a disk usage imbalance by concentrating partitions on one broker
3. Wait for Cruise Control to detect the anomaly
4. Verify auto-rebalance triggers automatically
5. Verify the rebalance completes and the cluster returns to `Idle`

**Usage:**
```bash
cd test-auto-rebalance-trigger
./COMPLETE_AUTO_REBALANCE_TEST.sh
```

---

### test-maintenance-window

Tests that the maintenance window correctly gates auto-rebalance on imbalance:
1. Deploy a Kafka cluster with the `imbalance` mode and a maintenance window set to 00:00–00:01 UTC (outside normal hours)
2. Create a disk usage imbalance
3. Wait for Cruise Control to detect the anomaly
4. Verify auto-rebalance is **NOT** triggered outside the window
5. Patch the window to cover the current UTC hour
6. Verify auto-rebalance **does** trigger within the open window
7. Verify the rebalance completes and the cluster returns to `Idle`

**Usage:**
```bash
cd test-maintenance-window
./TEST_MAINTENANCE_WINDOW.sh
```

---

### test-scale-down-priority

Tests that a scale-down operation takes priority over an in-progress auto-rebalance on imbalance:
1. Deploy a 4-broker Kafka cluster with `remove-brokers` and `imbalance` auto-rebalance modes
2. Create a disk usage imbalance and wait for `RebalanceOnImbalance` to start
3. Trigger a scale-down while the imbalance rebalance is running
4. Verify the imbalance KafkaRebalance is stopped and deleted
5. Verify the state transitions to `RebalanceOnScaleDown`
6. Verify the tracker ConfigMap is updated when the imbalance rebalance is stopped
7. Wait for the scale-down rebalance to complete and the state to return to `Idle`
8. Verify auto-rebalance on imbalance re-triggers if violations persist after the scale-down

**Usage:**
```bash
cd test-scale-down-priority
./TEST_SCALE_DOWN_PRIORITY.sh
```

---

### test-frequent-rebalance-alerts

Tests Prometheus alerting for detecting when auto-rebalance on imbalance is triggered too frequently, indicating a rebalancing loop:
1. Deploy Prometheus with two alerting rules:
   - **`FrequentAutoRebalanceOnImbalance`** — fires when more than 3 fixable anomaly detections occur within 10 minutes
   - **`UnfixableGoalViolationsDetected`** — fires when unfixable/mixed violations block auto-rebalance
2. Deploy a Kafka cluster with a 60-second anomaly detection interval
3. Create a persistent disk imbalance
4. Wait for multiple rebalance cycles to accumulate
5. Query Prometheus to verify the `strimzi_auto_rebalance_anomalies_detected_total` counter is incrementing
6. Verify the `FrequentAutoRebalanceOnImbalance` alert fires

**Usage:**
```bash
cd test-frequent-rebalance-alerts
./TEST_FREQUENT_REBALANCE_ALERTS.sh
```

**Alert rules are in:** `prometheus/prometheus-config.yaml`

---

### test-metrics

Tests that the anomaly detection metrics are exposed correctly:
- Verifies `strimzi_auto_rebalance_anomalies_detected_total` counter increments on detection
- Uses kube-state-metrics for additional KafkaRebalance resource metrics

**Usage:**
```bash
cd test-metrics
./TEST_METRICS.sh
```

## Prerequisites

- Kubernetes cluster (e.g., minikube or kind)
- `kubectl` configured to point at the cluster
- Strimzi operator image built from this branch
