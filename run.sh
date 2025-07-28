#!/bin/bash

# Create build directory
mkdir -p build
cd build

# Run CMake to configure the build
cmake ..

# Build the project
make

# Run the executable with a timeout
timeout 30 ./matching_engine