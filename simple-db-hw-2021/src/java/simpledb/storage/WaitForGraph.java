package simpledb.storage;

import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;

import static org.junit.Assert.assertTrue;

public class WaitForGraph {
    Map<Long, List<Long>> Graph;
    public WaitForGraph() {
        this.Graph = new HashMap<>();
    }
    // t1 request a lock t2 holds, add an edge t1-->t2
    public synchronized void insertEdge(TransactionId t1, TransactionId t2) throws TransactionAbortedException {
        Long t1Id = t1.getId();
        Long t2Id = t2.getId();
        if (!Graph.containsKey(t2Id)) {
            Graph.put(t2Id, new LinkedList<>());
        }
        if (!Graph.containsKey(t1Id)) {
            List<Long> adjNodes = new LinkedList<>();
            adjNodes.add(t2Id);
            Graph.put(t1Id, adjNodes);
        } else {
            if (Graph.get(t1Id).contains(t2Id)) {
                return;
            } else {
                Graph.get(t1Id).add(t2Id);
            }
        }
        if (hasCycle()) {
            System.out.println();
            System.out.println();
            System.out.println("Detect cycle");
            System.out.println();
            throw new TransactionAbortedException();
        }
        assertTrue(Graph.containsKey(t1Id));
        assertTrue(Graph.containsKey(t2Id));
    }
    // t1 request a lock t2 holds, add an edge t1-->t2

    public synchronized void insertEdgetoGraphCopy(TransactionId t1, TransactionId t2, Map<Long,List<Long>> graph) {
        Long t1Id = t1.getId();
        Long t2Id = t2.getId();
        if (!graph.containsKey(t2Id)) {
            graph.put(t2Id, new LinkedList<>());
        }
        if (!graph.containsKey(t1Id)) {
            List<Long> adjNodes = new LinkedList<>();
            adjNodes.add(t2Id);
            graph.put(t1Id, adjNodes);
        } else {
            if (graph.get(t1Id).contains(t2Id)) {
                return;
            } else {
                graph.get(t1Id).add(t2Id);
            }
        }
    }
    //remove an edge from t1 to t2

    public synchronized boolean removeEdge(TransactionId transactionId1, TransactionId transactionId2) {
        Long t1 = transactionId1.getId();
        Long t2 = transactionId2.getId();
        if (!Graph.containsKey(t1)) {
            return false;
        }
        List<Long> adjNodes = Graph.get(t1);
        Iterator<Long> it = adjNodes.iterator();
        while (it.hasNext()) {
            Long t = it.next();
            if (t.equals(t2)) {
                it.remove();
                return true;
            }
        }
        return false;
    }

    // We can only remove the vertex with out-degree == 0
    //
    public synchronized boolean removeVertex(TransactionId transactionId1) {
        Long t1 = transactionId1.getId();
        if (!Graph.containsKey(t1)) {
            return false;
        }
        if (Graph.get(t1).size() > 0) {
            return false;
            //throw new IllegalArgumentException("Try to delete a node with out-degree > 0");
            //throw new TransactionAbortedException();
        }
        Graph.remove(t1);
        for (Map.Entry<Long, List<Long>> entries: Graph.entrySet()) {
            List<Long> adjNodes = entries.getValue();
            Iterator<Long> it = adjNodes.iterator();
            while (it.hasNext()) {
                Long t = it.next();
                if (t.equals(t1)) {
                    it.remove();
                }
            }
        }
        return true;
    }
    /*
    public synchronized boolean removeSourceNode(TransactionId transactionId) {
        Long t = transactionId.getId();
        if (!Graph.containsKey(t)) {
            return false;
        }
        Graph.remove(t);
        return true;
    }
     */
    public synchronized Deque<Long> IndegreeCalculate(Map<Long,List<Long>> graph){
        //String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        //System.out.println(processName);
        Map<Long, Integer> indegree = new HashMap<>();
        Deque<Long> sourceNodes = new LinkedList<>();
        for (Long t: graph.keySet()) {
            indegree.put(t, 0);
        }
        assert (indegree.size() == graph.size());

        for (Map.Entry<Long, List<Long>> entrys: graph.entrySet()) {
            //String thisprocessName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            //System.out.println(thisprocessName);

            Long node = entrys.getKey();
            List<Long> adjNodes = entrys.getValue();
            Iterator<Long> it = adjNodes.iterator();
            while (it.hasNext()) {
                long t = it.next();
                if (!indegree.containsKey(t)) {
                    System.out.println(t);
                    System.out.println("indegree.size()" + indegree.size());
                    System.out.println("GraphCopy.entrysize()" + graph.entrySet().size());
                }
                indegree.put(t, indegree.get(t) + 1);
            }
        }
        for (Map.Entry<Long, Integer> entry: indegree.entrySet()) {
            if (entry.getValue() == 0) {
                sourceNodes.add(entry.getKey());
            }
        }
        return sourceNodes;
    }

    public synchronized void updateGraph(List<List<TransactionId>> edges) throws TransactionAbortedException {
        for (int i = 0; i < edges.size(); i += 1) {
            List<TransactionId> list = edges.get(i);
            insertEdge(list.get(0), list.get(1));
        }
        if (hasCycle()) {
            System.out.println();
            System.out.println();
            System.out.println("Detect cycle");
            System.out.println();
        }
    }

    public synchronized boolean DetectCycleWithEdges(List<List<TransactionId>> edges) {
        Set<Long> newEdgeTransaction = new HashSet<>();
        Map<Long, List<Long>> graphCopy = copyGraph();
        for (int i = 0; i < edges.size(); i += 1) {
            List<TransactionId> list = edges.get(i);
            insertEdgetoGraphCopy(list.get(0), list.get(1),graphCopy);
        }
        int VertexNum = graphCopy.size();
        int count = 0;
        int cycle = 0;
        while (cycle < VertexNum && count < VertexNum) {
            Deque<Long> deque = IndegreeCalculate(graphCopy);
            for (Long t: deque) {
                graphCopy.remove(t);
                count += 1;
            }
            cycle += 1;
        }
        /*
        if (graphCopy.size() == 0) {
            return null;
        }

         */
        return graphCopy.size() != 0;
        /*
        List<Long> cycleTransactions = new ArrayList();
        Long newTransactionId = edges.get(0).get(0).getId();
        for (Long t: graphCopy.keySet()) {
            if (t != newTransactionId) {
                cycleTransactions.add(t);
            }
        }
        return cycleTransactions;
         */
    }
    public synchronized boolean hasCycle() {
        Map<Long, List<Long>> graphCopy = copyGraph();
        int VertexNum = graphCopy.size();
        int count = 0;
        int cycle = 0;
        while (cycle < VertexNum && count < VertexNum) {
            Deque<Long> deque = IndegreeCalculate(graphCopy);
            for (Long t: deque) {
                graphCopy.remove(t);
                count += 1;
            }
            cycle += 1;
        }
        return graphCopy.size() != 0;
    }
    public Map<Long,List<Long>> copyGraph() {
        Map<Long, List<Long>> graphCopy = new HashMap<>();
        for (Map.Entry<Long, List<Long>> entry: this.Graph.entrySet()) {
            List<Long> items = new LinkedList<>();
            for (Long item: entry.getValue()) {
                items.add(item.longValue());
            }
            long key = entry.getKey();
            graphCopy.put(key, items);
        }
        return graphCopy;
    }

}
