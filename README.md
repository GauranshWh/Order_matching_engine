# Order Matching Engine

A high-performance order matching engine implemented in C++ for simulating high-frequency trading systems. This project demonstrates multi-threaded order processing, efficient memory management, and real-time trade matching.

## Features
- Multi-threaded order submission and matching with 4 worker threads.
- Custom `OrderPool` for efficient memory allocation and deallocation.
- `OrderBook` implementation with price-time priority matching.
- Benchmarking support to measure throughput and latency.
- Signal handling for graceful shutdown.

## Performance
- **Benchmark**: Processed 1,000,000 orders in 32,353.6 µs.
- **Latency**: 3,235 ns per order.
- **Throughput**: 309,085 orders/sec.
- **Trades Generated**: 5,263.
- **Total Runtime**: 185 ms (including initialization and shutdown).

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
