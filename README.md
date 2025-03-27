# SimpleDB — Lightweight Relational Database

SimpleDB is a fully functional, educational relational database system built from scratch in Java, designed to simulate the core components of modern RDBMS systems. This project was developed as part of the **University of Washington’s CSE444** course and showcases a complete backend architecture including query processing, memory management, concurrency control, transaction logging, and more.

> **Note:** This implementation is educational in nature and heavily inspired by the coursework and architecture outlined in the CSE444 curriculum. All credit for the project structure and conceptual design goes to the University of Washington Paul G. Allen School of Computer Science & Engineering.

------------------------------------------------------------------------

## 🔧 Core Features

### 🧠 Query Processing & Execution

-   Accepts SQL-like input via command line.
-   Parses logical plans using clause-based construction (`FROM`, `WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`).
-   Transforms logical plans into optimized physical plans using **cost estimation** and **join order selection**.
-   Supports core operations: selection, projection, joins (nested loop & hash), aggregation, and sorting.

### 🧮 Buffer Management

-   Implements a **Buffer Pool Manager** that caches pages in memory and mediates between disk and operators.
-   Enforces **NO STEAL** (dirty pages cannot be evicted until commit) for atomicity.
-   Applies the **FORCE** policy (flush dirty pages on commit) for durability.

### 🔒 Concurrency Control

-   Enforces **strict Two-Phase Locking (2PL)** with:
    -   Shared and Exclusive locks.
    -   Deadlock detection via wait-for graph & timeouts.
-   Ensures **serializability** and **isolation** for concurrent transactions.

### 🔁 Transaction Management & Logging

-   Uses **Write-Ahead Logging (WAL)** for recovery.
    -   Logs both before- and after-images for all page updates.
    -   Ensures **atomicity and durability** through Undo/Redo recovery procedures.

### 📊 Cost-Based Optimizer

-   Computes **table statistics** including I/O cost estimates and cardinality.
-   Optimizes physical plan generation based on cost models.

------------------------------------------------------------------------

## 📁 System Architecture

### 🧱 Main Components

-   **Query Processor:** Handles parsing, logical/physical plan generation, and execution.
-   **Buffer Manager:** Manages in-memory page caching and eviction policies.
-   **Lock Manager:** Manages locks for read/write operations with 2PL.
-   **Log Manager:** Ensures fault tolerance with WAL.
-   **Access Manager:** Maintains catalog metadata and file structure.

### 🗂 Architecture Flow

![](https://lh7-rt.googleusercontent.com/docsz/AD_4nXdsXK6cu01gDz_Q8NYkSrzi7dUNfZ1Gccw2ogHRYGJ2Drt58gEoaK5GnIm6WTCxbCUyBC3m3763J1ZQPVpYV5uWi1ZlnMh-aRL8FsyCd_XM0bq2AZHq6UUuoSBKuIrvj9DwlWuhKw?key=9nlZhEyWeKIUueLOu-EVd2om){width="464"}

### 🚀 Highlights from Execution Pipeline

-   Parser.main() kicks off schema loading and interactive query processing.
-   Logical Plan is constructed from parsed SQL input.
-   Physical Plan is generated with cost-based optimizations.
-   Execution Engine opens, iterates, and closes operator trees to return results.
-   Transactions are tracked and recovered through WAL and 2PL.

### 📈 Performance Discussion

SimpleDB performs efficiently under basic to intermediate workloads, maintaining ACID properties and minimizing disk I/O through buffering and strict locking. However, scalability becomes limited with large or complex queries.

### 💡 Potential Improvements If further time and resources were available, I would explore:

-   Bushy Join Trees for greater join flexibility and performance gains.
-   Smarter Eviction Policies such as LRU-K or Clock to reduce buffer pool pressure.
-   Improved Selectivity Estimations using multi-dimensional histograms for more accurate query optimization.

### 🛠 Technical Skills Demonstrated Java backend development and object-oriented design.

-   Relational database internals and query execution pipelines.
-   Memory and buffer management systems.
-   Concurrency control (locking protocols, deadlock detection).
-   Logging and recovery techniques (WAL, undo/redo).
-   Cost-based query optimization.
-   Data structures and systems-level programming concepts.

📚 Attribution This project was developed as part of the University of Washington CSE444. Much of the system architecture and learning objectives were provided by the course instructors and staff. While the implementation is my own, it builds upon the framework and guidance of the CSE444 curriculum. Full credit to the University of Washington for the design and scaffolding of this educational project.
