package simpledb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PageIterator implements Iterator<Tuple> {
    private HeapPage hp;
    private int slotId = -1;

    public PageIterator(HeapPage hp) {
        this.hp = hp;
    }
    @Override
    public boolean hasNext() {
        while(true) {
            int nextId = slotId + 1;
            if (nextId >= hp.numSlots) {
                return false;
            }
            if (hp.isSlotUsed(nextId)) {
                return true;
            }
            slotId += 1;
        }
    }


    @Override
    public Tuple next() {
        while(true) {
            int nextId = slotId + 1;
            if (nextId >= hp.numSlots) {
                throw new NoSuchElementException();
            }
            if (hp.isSlotUsed(nextId)) {
                slotId = nextId;
                return hp.tuples[slotId];
            }
            slotId += 1;
        }
    }
}
