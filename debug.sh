#!/bin/bash

echo "Building with DEBUG=1..."
make DEBUG=1
if [ $? -ne 0 ]; then
    echo "Make failed, exiting..."
    exit 1
fi

PID=$(pidof simpledb)
if [ -n "$PID" ]; then
    echo "Killing existing simpledb (PID: $PID)..."
    kill -9 $PID
else
    echo "No simpledb process running."
fi

echo "Cleaning up data directory..."
rm -rf ~/data/*

echo "Starting GDB..."
gdb src/simpledb

