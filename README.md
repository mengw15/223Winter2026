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
- **Data**: 500 accounts with initial balances (`Data/workload1/input1.txt`)
- **Transaction**: Transfer $1 from one randomly selected account to another
- Each transaction reads 2 keys and writes 2 keys
- Contention arises when multiple threads select overlapping accounts

### Workload 2: TPC-C Style
- **Data**: 8 warehouses, 80 districts, 8000 customers, 100 items, 800 stocks (`Data/workload2/input2.txt`)
- **Transactions** (run in equal proportions, 50/50):
  - **NewOrder** (4 keys): Reads a district to generate an order ID (`next_o_id + 1`), then reads and updates 3 stock items (decrement `qty`, increment `ytd` and `order_cnt`)
  - **Payment** (3 keys): Updates warehouse `ytd`, district `ytd`, and customer `balance`/`ytd_payment`/`payment_cnt`
- Cross-transaction contention: both NewOrder and Payment write to district keys, creating natural conflicts between different transaction types

## Concurrency Control Protocols

### Optimistic Concurrency Control (OCC)

**Execution flow:**
1. **Begin**: Snapshot the set of finished transactions (`ignore set`) — these completed before we started and are safe to ignore during validation
2. **Read/Write phase**: Execute transaction logic without any locks. Reads go to RocksDB (or the private write buffer if the key was already written). Writes are buffered privately in a `HashMap`, not applied to the database
3. **Validation phase** (sequential — one transaction validates at a time):
   - Enter a `synchronized` validation method (ensures sequential validation — one transaction at a time)
   - **Check 1**: For every transaction that was validated after we started (`validated - ignore`), verify that its write set does not overlap with our read set (`RS(Tj) ∩ WS(Ti) = ∅`). This ensures we did not read stale data
   - **Check 2**: For every such transaction that has not yet finished its write phase, verify that its write set does not overlap with our write set (`WS(Tj) ∩ WS(Ti) = ∅`). This prevents concurrent write conflicts
   - If both checks pass: add this transaction to the `validated` set and return
   - If either check fails: return failure and abort
4. **Write phase**: Apply the private write buffer to RocksDB. This happens **outside** the validation lock, allowing other transactions to validate concurrently
5. **Finish**: Mark the transaction as `finished` so future transactions can add it to their ignore set

**On abort**: The transaction retries immediately with newly selected keys. Unlike Conservative 2PL, OCC does not use backoff because there is no risk of livelock — conflicts are detected at validation time and the failing transaction simply restarts

### Conservative Two-Phase Locking (Conservative 2PL)

**Execution flow:**
1. **Declare keys**: Each transaction declares all keys it will access before execution (known from the transaction template)
2. **Acquire all locks**:
   - Keys are **sorted alphabetically** before acquisition to ensure a consistent global ordering, reducing the chance of mutual blocking
   - Use `tryLock()` (non-blocking) on each key in order
   - If **any** lock cannot be acquired: immediately **release all** locks already held and return failure. This is the key property — no transaction ever holds locks while waiting, making **deadlocks impossible**
3. **Execute**: With all locks held, read from RocksDB, perform logic, buffer writes
4. **Commit**: Apply the write buffer to RocksDB
5. **Release all locks**

**Livelock prevention**: Since transactions release all locks on failure and retry, there is a risk of livelock — multiple transactions repeatedly failing and retrying at the same time, never making progress. We address this with:
- **Exponential backoff**: Each retry waits longer (`2^attempt` ms, capped at `2^10 = 1024` ms)
- **Random jitter**: Each wait adds a random component (`random(0-4)` ms) so that competing transactions wake up at different times and don't collide again
- **Max retries**: A safety cap of 100 retries prevents infinite loops in pathological cases

**Lock type**: All accesses (both reads and writes) use exclusive locks for simplicity. This is more conservative than using shared/exclusive locks but avoids the complexity of lock upgrades.

## Contention Model

Contention is controlled by two parameters: `--contention` (probability `p`) and `--hotset` (size `h`):

- With probability `p`, a key is selected from the **hotset** (the first `h` keys in each key pool)
- With probability `1 - p`, a key is selected uniformly from the **full keyspace**

| Contention | Behavior |
|------------|----------|
| `p = 0.0` | All keys selected uniformly — minimal conflicts |
| `p = 0.5` | Half the selections target hot keys — moderate conflicts |
| `p = 1.0` | All selections from hotset — maximum conflicts |

Shrinking the hotset size or increasing `p` both increase contention. For example, `--contention 0.8 --hotset 5` creates very high contention as 80% of accesses target just 5 keys.

## Running Experiments

### Single Run

Use `org.cs223.Main` with command-line arguments to run a single experiment (see examples above).

Each run automatically exports results to the `results/` directory:
- `results/summary.csv` — one row appended per run with throughput, retry rate, avg response time
- `results/rt_w1_OCC_t4_c0.50.csv` — per-transaction response times for that run

### Batch Run (All Combinations)

Run `org.cs223.ExperimentRunner` to automatically execute all parameter combinations:

- **Workloads**: 1 and 2
- **Protocols**: OCC and Conservative 2PL
- **Thread counts**: 1, 2, 4, 8
- **Contention levels**: 0, 0.2, 0.5, 0.8, 1.0

This runs **400 experiments** sequentially (2 × 2 × 4 × 5 × 5). Each experiment creates a fresh database, loads the workload data, executes 10,000 transactions, and exports results.

To run from IntelliJ: right-click `ExperimentRunner.java` → Run.

Configuration constants at the top of `ExperimentRunner.java`:
```java
static final int[] THREAD_COUNTS = {1, 2, 4, 8};
static final double[] CONTENTION_LEVELS = {0, 0.2, 0.5, 0.8, 1.0};
static final int[] HOTSET_SIZES = {5, 10, 20, 50, 100};
static final int NUM_TRANSACTIONS = 10000;
```

### Exported Results

All results are written to the `results/` directory:

| File | Contents |
|------|----------|
| `summary.csv` | One row per experiment: workload, protocol, threads, contention, hotset, transactions, committed, retries, retry_rate, throughput, avg_response_time |
| `rt_w{N}_{PROTO}_t{T}_c{C}_h{H}.csv` | Per-transaction response times for a specific run, with columns: template, response_time_ms |

`summary.csv` example:
```
workload,protocol,threads,contention,hotset,transactions,committed,retries,retry_rate,throughput,avg_response_time
1,OCC,4,0.50,10,10000,10000,330,3.19,19942.81,0.0366
1,TWO_PL,4,0.50,10,10000,10000,120,1.18,18500.00,0.0410
```

These CSV files can be read directly by Python/matplotlib for plotting.

## Project Structure

```
src/main/java/org/cs223/
├── Main.java                 # CLI entry point, argument parsing, workload setup
├── Database.java             # RocksDB wrapper with map serialization/deserialization
├── Transaction.java          # Transaction state: read set, write buffer, timing
├── TransactionManager.java   # Thread pool execution, retry logic, stats collection
├── LockManager.java          # Conservative 2PL: per-key ReentrantLocks, acquire-all-or-release-all
├── OCCValidator.java         # OCC: synchronized validation, validated/finished sets
├── Stats.java                # CSV export: summary.csv and per-transaction response times
├── ExperimentRunner.java     # Batch runner: sweeps all parameter combinations
├── parser/
│   └── InsertParser.java     # Parses INSERT data files, loads into RocksDB
├── template/
│   ├── TransactionTemplate.java   # Interface: getNumKeys(), execute(), getName()
│   ├── TransferTemplate.java      # Workload 1: read 2 accounts, transfer $1
│   ├── NewOrderTemplate.java      # Workload 2: read district + 3 stocks, update
│   └── PaymentTemplate.java       # Workload 2: read warehouse + district + customer, update
└── test/
    ├── TestDatabase.java          # RocksDB CRUD operations
    ├── TestLockManager.java       # Lock acquire/release, multi-thread contention
    ├── TestOCC.java               # Validation pass, conflict detection, abort
    ├── TestTransaction.java       # Read/write buffer, apply writes
    └── TestTransactionManager.java # End-to-end: OCC vs 2PL with multiple threads
```

## Output

Each run prints:
- Total committed transactions
- Total retries and retry rate (percentage of attempts that failed)
- Throughput (committed transactions per second)
- Average response time (ms, from transaction begin to commit)
- Per-template breakdown for Workload 2 (NewOrder vs Payment stats)

## Dependencies

- [RocksDB](https://rocksdb.org/) via `rocksdbjni 9.6.1` (managed by Maven, no manual installation needed)
