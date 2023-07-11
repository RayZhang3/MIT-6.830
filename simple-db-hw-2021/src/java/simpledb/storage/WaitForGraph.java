package simpledb.storage;

import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertTrue;

public class WaitForGraph {
    private final Map<TransactionId, Set<TransactionId>> edges = new ConcurrentHashMap<>();
    public boolean addEdge(TransactionId from, TransactionId to) {
        if (!edges.containsKey(from)) {
            edges.put(from, new HashSet<>());
        }
        Set<TransactionId> dependsOn = edges.get(from);
        dependsOn.add(to);
        if (detectDeadLock(to, new HashSet<>())) {
            // Need to abort this transaction
            return false;
        }
        return true;
    }
    public void removeEdge(TransactionId from, TransactionId to) {
        if (edges.containsKey(from)) {
            Set<TransactionId> dependsOn = edges.get(from);
            dependsOn.remove(to);
            if (dependsOn.isEmpty()) {
                edges.remove(from);
            }
        }
    }

    public boolean detectDeadLock(TransactionId start, Set<TransactionId> visited) {
        if (!visited.add(start)) {
            return true;
        }
        Set<TransactionId> adjNodes = edges.get(start);
        if (adjNodes != null) {
            for (TransactionId neighbor: adjNodes) {
                if (detectDeadLock(neighbor, visited)) {
                    return true;
                }
            }
        }
        visited.remove(start);// Backtrack since we are done exploring this path.
        return false;
    }

    public Set<TransactionId> getTransactionNoNeedToWait() {
        // TODO: get Transaction without out edge.
        return null;
    }
}