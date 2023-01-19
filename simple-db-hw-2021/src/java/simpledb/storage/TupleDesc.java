package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private final TDItem[] items;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldType = t;
            this.fieldName = n;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return Arrays.stream(this.items).iterator();
    }

    private static final long serialVersionUID = 1L;
    public TupleDesc(TDItem[] items) {
        this.items = items;
    }
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        TDItem[] items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i += 1) {
            items[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
        this.items = items;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        TDItem[] items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i += 1) {
            items[i] = new TDItem(typeAr[i], null);
        }
        this.items = items;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.items.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (this.items == null || this.items.length == 0 || i >= this.items.length) {
            throw new NoSuchElementException();
        } else {
            return this.items[i].fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (this.items == null || this.items.length == 0 || i >= this.items.length) {
            throw new NoSuchElementException();
        } else {
            return this.items[i].fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (this.items == null || this.items.length == 0) {
            throw new NoSuchElementException();
        }
        if (name == null) {
            throw new NoSuchElementException();
        }
        for (int i = 0; i < this.items.length; i += 1) {
            if (name.equals(this.items[i].fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        if (this.items == null || this.items.length == 0) {
            return 0;
        } else {
            int length = 0;
            for (TDItem item: items){
                length += item.fieldType.getLen();
            }
            return length;
        }
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TDItem[] newItems = new TDItem[td1.items.length + td2.items.length];
        int i = 0;
        for (TDItem item : td1.items) {
            newItems[i++] = item;
        }
        for (TDItem item: td2.items) {
            newItems[i++] = item;
        }
        return new TupleDesc(newItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == this) {
            return true;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc other = (TupleDesc) o;
        if (this.numFields() != other.numFields()) {
            return false;
        }
        for (int i = 0; i < this.numFields(); i += 1) {
            if (!this.items[i].fieldType.equals(other.items[i].fieldType)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return toString().hashCode();
        //throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuffer sb = new StringBuffer();
        for (TDItem item: items) {
            sb.append(item.toString());
        }
        return sb.toString();
    }
}
