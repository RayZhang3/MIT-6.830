package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class WaitForGraph {
    Map<TransactionId, List<TransactionId>> Graph;
    Map<TransactionId, List<TransactionId>> GraphCopy;
    public WaitForGraph() {
        this.Graph = new ConcurrentHashMap<>();
    }
    // t1 request a lock t2 holds, add an edge t1-->t2
    public synchronized void insertEdge(TransactionId t1, TransactionId t2) {
        if (!Graph.containsKey(t2)) {
            Graph.put(t2, new LinkedList<>());
        }

        if (!Graph.containsKey(t1)) {
            List<TransactionId> adjNodes = new LinkedList<>();
            adjNodes.add(t2);
            Graph.put(t1, adjNodes);
        } else {
            if (Graph.get(t1).contains(t2)) {
                return;
            }
            this.Graph.get(t1).add(t2);
        }
    }

    //remove an edge from t1 to t2
    public synchronized boolean removeEdge(TransactionId t1, TransactionId t2) {
        if (!Graph.containsKey(t1)) {
            return false;
        }
        List<TransactionId> adjNodes = Graph.get(t1);
        Iterator<TransactionId> it = adjNodes.iterator();
        while (it.hasNext()) {
            TransactionId t = it.next();
            if (t.equals(t2)) {
                it.remove();
                return true;
            }
        }
        return false;
    }
    // We can only remove the vertex with out-degree == 0
    //
    public synchronized boolean removeVertex(TransactionId t1) {
        if (!Graph.containsKey(t1)) {
            return false;
        }
        if (Graph.get(t1).size() > 0) {
            return false;
        }
        Graph.remove(t1);
        for (Map.Entry<TransactionId, List<TransactionId>> entrys: Graph.entrySet()) {
            List<TransactionId> adjNodes = entrys.getValue();
            Iterator<TransactionId> it = adjNodes.iterator();
            while (it.hasNext()) {
                TransactionId t = it.next();
                if (t.equals(t1)) {
                    it.remove();
                }
            }
        }
        return true;
    }

    public synchronized boolean removeSourceNode(TransactionId t) {
        if (!Graph.containsKey(t)) {
            return false;
        }
        Graph.remove(t);
        return true;
    }

    public Deque<TransactionId> IndegreeCalculate(){
        Map<TransactionId, Integer> indegree = new HashMap<>();
        Deque<TransactionId> sourceNodes = new LinkedList<>();
        for (TransactionId t: GraphCopy.keySet()) {
            indegree.put(t, 0);
        }
        for (Map.Entry<TransactionId, List<TransactionId>> entrys: GraphCopy.entrySet()) {
            TransactionId node = entrys.getKey();
            List<TransactionId> adjNodes = entrys.getValue();
            Iterator<TransactionId> it = adjNodes.iterator();
            while (it.hasNext()) {
                TransactionId t = it.next();
                indegree.put(t, indegree.get(t) + 1);
            }
        }
        for (Map.Entry<TransactionId, Integer> entry: indegree.entrySet()) {
            if (entry.getValue() == 0) {
                sourceNodes.add(entry.getKey());
            }
        }
        return sourceNodes;
    }

    // t1 request a lock t2 holds, add an edge t1-->t2
    public void insertGraphCopyEdge(TransactionId t1, TransactionId t2) {
        if (!GraphCopy.containsKey(t1)) {
            List<TransactionId> adjNodes = new LinkedList<>();
            adjNodes.add(t2);
            GraphCopy.put(t1, adjNodes);
        } else {
            GraphCopy.get(t1).add(t2);
        }
        if (!GraphCopy.containsKey(t2)) {
            GraphCopy.put(t2, new LinkedList<>());
        }
    }

    public synchronized void upDateGraphCopy() {
        this.GraphCopy = new HashMap<>(Graph);
    }

    public synchronized boolean hasCycle() {
        int count = 0;
        int cycle = 0;
        while (cycle < Graph.size() && count < Graph.size()) {
            Deque<TransactionId> deque = IndegreeCalculate();
            for (TransactionId t: deque) {
                GraphCopy.remove(t);
                count += 1;
            }
            cycle += 1;
        }
        return count == Graph.size();
    }
}
