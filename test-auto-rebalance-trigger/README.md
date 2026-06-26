# Auto-Rebalance on Imbalance Test

This test demonstrates the complete auto-rebalance feature in Strimzi Kafka, including anomaly detection, automatic rebalance triggering, and verification of cluster stability.

## Prerequisites

Before running the test script, ensure you have:

1. **Kubernetes cluster** (e.g., minikube, kind, or a cloud provider cluster)
2. **kubectl** configured and connected to your cluster

## Files Included

This test directory contains:
- `COMPLETE_AUTO_REBALANCE_TEST.sh` - Main test script
- `test-auto-rebalance-on-imbalance.yaml` - Kafka cluster configuration with auto-rebalance enabled
- `cluster-operator/` - Strimzi operator installation files
- `README.md` - This file
- `TEST_RESULTS.md` - Example test results from a successful run

## Running the Test

### Step 1: Navigate to the test directory

```bash
cd test-auto-rebalance-trigger
```

### Step 2: Make the script executable

```bash
chmod +x COMPLETE_AUTO_REBALANCE_TEST.sh
```

### Step 3: Run the test

```bash
./COMPLETE_AUTO_REBALANCE_TEST.sh
```

**Note:** The script automatically uses the files in this directory:
- `test-auto-rebalance-on-imbalance.yaml` for the Kafka cluster configuration
- `cluster-operator/` for Strimzi operator installation

The script will automatically:

1. Create a Kubernetes namespace (`myproject`) and deploy the Strimzi operator
2. Deploy a Kafka cluster with auto-rebalance enabled (using `test-auto-rebalance-on-imbalance.yaml`)
3. Create a disk usage imbalance by placing most partitions on broker-0
4. Wait for Cruise Control to detect the anomaly
5. Verify auto-rebalance triggers automatically
6. Monitor the rebalance until completion
7. Verify no new anomalies are detected after rebalancing

**Expected Runtime:** 10-15 minutes

**Note on Fast Completion:** In some cases, the auto-rebalance may complete very quickly (within seconds) during the Cruise Control training period. The script has been updated to detect and report this scenario as a success.

### Step 3: Review the results

The script provides detailed output at each step and a final summary. Look for:

- ✓ marks indicating successful completion of each phase
- Final test results showing the feature working correctly
- Detailed status of the Kafka cluster and Cruise Control

## What Gets Tested

- **Anomaly Detection**: Cruise Control detects `DiskUsageDistributionGoal` violations
- **Auto-Trigger**: The operator automatically creates a `KafkaRebalance` resource
- **Rebalancing**: Partitions are redistributed to balance disk usage across brokers
- **State Transition**: The auto-rebalance state correctly transitions to `Idle` after completion
- **Stability**: No new anomalies are detected after successful rebalancing

## Cleanup

To remove the test cluster and resources:

```bash
kubectl delete kafka test-cluster -n myproject
kubectl delete namespace myproject
```

## Troubleshooting

If the test fails, check:

- **Operator logs**: `kubectl logs -n myproject deployment/strimzi-cluster-operator --tail=100`
- **Cruise Control logs**: `kubectl logs -n myproject -l strimzi.io/name=test-cluster-cruise-control --tail=100`
- **Kafka status**: `kubectl get kafka test-cluster -n myproject -o yaml`

The script includes detailed error messages and suggested commands to investigate issues.
