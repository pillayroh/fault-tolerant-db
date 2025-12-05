#!/bin/bash

# Test script to verify Zookeeper and Cassandra connectivity

echo "========================================="
echo "Testing Zookeeper and Cassandra Setup"
echo "========================================="
echo ""

# Test Zookeeper
echo "1. Testing Zookeeper on localhost:2181..."
if echo "ruok" | nc -w 2 localhost 2181 > /dev/null 2>&1; then
    echo "   ✓ Zookeeper is running and accessible"
else
    echo "   ✗ Zookeeper connection failed"
    exit 1
fi

# Test Cassandra
echo "2. Testing Cassandra on localhost:9042..."
if cqlsh localhost 9042 -e "DESCRIBE KEYSPACES;" > /dev/null 2>&1; then
    echo "   ✓ Cassandra is running and accessible"
    echo ""
    echo "   Available keyspaces:"
    cqlsh localhost 9042 -e "DESCRIBE KEYSPACES;" 2>/dev/null | grep -E "^\s+\w" | sed 's/^/      /'
else
    echo "   ✗ Cassandra connection failed"
    exit 1
fi

echo ""
echo "3. Checking required keyspaces..."
REQUIRED_KEYS="server0 server1 server2"
for keyspace in $REQUIRED_KEYS; do
    if cqlsh localhost 9042 -e "USE $keyspace; DESCRIBE TABLES;" > /dev/null 2>&1; then
        echo "   ✓ Keyspace '$keyspace' exists"
    else
        echo "   ⚠ Keyspace '$keyspace' does not exist (will be created by servers)"
    fi
done

echo ""
echo "========================================="
echo "All connectivity tests passed!"
echo "========================================="
echo ""
echo "Configuration Summary:"
echo "  - Zookeeper: localhost:2181"
echo "  - Cassandra: localhost:9042"
echo "  - Server keyspaces: server0, server1, server2"
echo ""
echo "You can now run the tests with:"
echo "  java -cp \"bin:lib/*\" GraderFaultTolerance"

