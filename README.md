# Order Matching Engine

A high-performance order matching engine implemented in C++ for simulating high-frequency trading systems. This project demonstrates multi-threaded order processing, efficient memory management, and real-time trade matching.

## Features
- Multi-threaded order submission and matching with 4 worker threads.
- Custom `OrderPool` for efficient memory allocation and deallocation.
- `OrderBook` implementation with price-time priority matching.
- Benchmarking support to measure throughput and latency.
- Signal handling for graceful shutdown.

## Performance
- **Benchmark**: Processed **10,000 orders in 30,405.7 µs (30.4 ms)**.
- **Latency**: **3,040 ns (3.04 µs) per order**.
- **Throughput**: **328,886 orders/sec**.
- **Trades Generated**: **4,997**.
- **Total Runtime**: **416 ms** (including initialization, tests, and shutdown).
- **Note**: Optimization is ongoing! Planning to integrate lock-free queues and sharding to target millions of orders/sec.

## Requirements
- C++17 compiler (e.g., g++ 13.3.0)
- Boost library (version 1.65+)
- CMake for building
- POSIX-compliant system (for `sched_setaffinity`)

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/hft-matching-engine.git
   cd hft-matching-engine

## Run 
  rm -rf build/* 
  ./run.sh
