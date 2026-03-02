# CS 223 Winter 2026 — Multi-Threaded Transaction Processing System

A multi-threaded transaction processing layer built on top of RocksDB, implementing two concurrency control protocols: **Optimistic Concurrency Control (OCC)** and **Conservative Two-Phase Locking (Conservative 2PL)**.

## Prerequisites

- **Java 21** (JDK 21 or higher)
- **Maven** (included with IntelliJ IDEA, or install separately)

## Build

```bash
cd 223Winter
mvn clean compile
```

## Run

```bash
mvn exec:java -Dexec.mainClass="org.cs223.Main" -Dexec.args="<arguments>"
```

Or run `org.cs223.Main` directly from IntelliJ IDEA with program arguments.

### Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--workload` | Workload number (1 or 2) | 1 |
| `--protocol` | Concurrency control protocol (`occ` or `2pl`) | occ |
| `--threads` | Number of worker threads | 4 |
| `--contention` | Contention level (0.0 to 1.0) | 0.5 |
| `--hotset` | Number of hot keys | 10 |
| `--transactions` | Total number of transactions to execute | 1000 |

### Examples

```bash
# Workload 1 (bank transfer), OCC, 4 threads, medium contention
mvn exec:java -Dexec.mainClass="org.cs223.Main" \
  -Dexec.args="--workload 1 --protocol occ --threads 4 --contention 0.5 --hotset 10 --transactions 1000"

# Workload 1, Conservative 2PL, 8 threads, high contention
mvn exec:java -Dexec.mainClass="org.cs223.Main" \
  -Dexec.args="--workload 1 --protocol 2pl --threads 8 --contention 0.8 --hotset 10 --transactions 1000"

# Workload 2 (TPC-C style), OCC, 4 threads, low contention
mvn exec:java -Dexec.mainClass="org.cs223.Main" \
  -Dexec.args="--workload 2 --protocol occ --threads 4 --contention 0.2 --hotset 10 --transactions 1000"

# Workload 2, Conservative 2PL, 16 threads
mvn exec:java -Dexec.mainClass="org.cs223.Main" \
  -Dexec.args="--workload 2 --protocol 2pl --threads 16 --contention 0.5 --hotset 10 --transactions 1000"
```

## Workloads

### Workload 1: Bank Transfer
- **Data**: 500 accounts (`Data/workload1/input1.txt`)
- **Transaction**: Transfer $1 from one account to another (2 keys per transaction)

### Workload 2: TPC-C Style
- **Data**: 8 warehouses, 80 districts, 8000 customers, 100 items, 800 stocks (`Data/workload2/input2.txt`)
- **Transactions**:
  - **NewOrder**: Read 1 district + 3 stocks, update order ID and stock quantities (4 keys)
  - **Payment**: Read 1 warehouse + 1 district + 1 customer, update balances and payment counts (3 keys)
- Transactions run in equal proportions (50% NewOrder, 50% Payment)

## Concurrency Control Protocols

### Optimistic Concurrency Control (OCC)
- Transactions execute without locks during the read/write phase
- Writes are buffered privately
- At commit time, sequential validation checks for conflicts with concurrent transactions
- Validation checks: (1) no committed transaction wrote a key we read, (2) no in-progress validated transaction writes a key we also write
- Write phase executes outside the validation lock
- On validation failure, the transaction aborts and retries with exponential backoff

### Conservative Two-Phase Locking (Conservative 2PL)
- Transactions declare all required keys upfront
- All locks (exclusive) must be acquired before execution begins
- If any lock is unavailable, all held locks are released immediately
- Keys are sorted before acquisition to reduce conflicts
- Livelock prevention via exponential backoff with random jitter
- All locks are released after transaction commits

## Project Structure

```
src/main/java/org/cs223/
├── Main.java                 # CLI entry point, argument parsing
├── Database.java             # RocksDB wrapper with map serialization
├── Transaction.java          # Transaction with read set, write buffer
├── TransactionManager.java   # Thread pool, workload execution, stats
├── LockManager.java          # Conservative 2PL lock manager
├── OCCValidator.java         # OCC validation with finished/validated sets
├── parser/
│   └── InsertParser.java     # Parses INSERT data files into RocksDB
├── template/
│   ├── TransactionTemplate.java   # Interface for transaction logic
│   ├── TransferTemplate.java      # Workload 1: bank transfer
│   ├── NewOrderTemplate.java      # Workload 2: new order
│   └── PaymentTemplate.java       # Workload 2: payment
└── test/
    ├── TestDatabase.java
    ├── TestLockManager.java
    ├── TestOCC.java
    ├── TestTransaction.java
    └── TestTransactionManager.java
```

## Output

Each run prints:
- Total committed transactions
- Total retries and retry rate
- Throughput (transactions/second)
- Average response time (ms)
- Per-template breakdown (for Workload 2)

## Dependencies

- [RocksDB](https://rocksdb.org/) via `rocksdbjni 9.6.1` (managed by Maven)
