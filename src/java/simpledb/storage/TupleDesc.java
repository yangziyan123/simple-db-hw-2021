// 参考：https://blog.csdn.net/weixin_45938441/article/details/127949453
package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    CopyOnWriteArrayList<TDItem> tdItems;

    public CopyOnWriteArrayList<TDItem> getTdItems() {
        return tdItems;
    }

    @Override
    public TupleDesc clone() {
        try {
            return (TupleDesc) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

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
            this.fieldName = n;
            this.fieldType = t;
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
        // some code goes here
        if (tdItems == null) {
            return null;
        }
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr. length fields with fields of the
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
        this.tdItems = new CopyOnWriteArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            this.tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr. length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.tdItems = new CopyOnWriteArrayList<>();
        for (Type type : typeAr) {
            tdItems.add(new TDItem(type, null));
        }
    }

    public TupleDesc(){
        tdItems = new CopyOnWriteArrayList<>();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.size();
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
        if(i < 0 || i >= tdItems.size()){
            throw new NoSuchElementException("i is not a valid field reference.");
        }
        return tdItems.get(i).fieldName;
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
        if(i < 0 || i >= tdItems.size()){
            throw new NoSuchElementException("i is not a valid field reference.");
        }
        return tdItems.get(i).fieldType;
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
        // some code goes here
        if(name == null){
            throw new NoSuchElementException("no field with a matching name is found.");
        }
        String altName = name.substring(name.lastIndexOf(".")+1);
        // 因为合并后的元组可能得不到别名因此去掉.前面的名字
        for (int i = 0; i < tdItems.size(); i++) {
            if(name.equals(getFieldName(i)) || altName.equals(getFieldName(i)) ){
                return i;
            }
        }
        throw new NoSuchElementException("no field with a matching name is found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : tdItems) {
            size += item.fieldType.getLen();
        }
        return size;
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
        if (td1 == null) {
            return td2;
        }
        if (td2 == null) {
            return td1;
        }
        TupleDesc tupleDesc = new TupleDesc();
        for(int i = 0 ; i < td1.numFields() ; i++){
            tupleDesc.tdItems.add(td1.tdItems.get(i));
        }
        for (int i = 0; i < td2.numFields(); i++) {
            tupleDesc.tdItems.add(td2.tdItems.get(i));
        }

        return tupleDesc;
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
        if (!(o instanceof TupleDesc)) {
            return false;
        }

        TupleDesc other = (TupleDesc) o;
        if (other.getSize() != this.getSize() || other.numFields() != this.numFields()) {
            return false;
        }

        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getFieldType(i).equals(other.getFieldType(i))) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int result = 0;
        for (TDItem item : tdItems) {
            result += item.toString().hashCode() * 41 ;
        }
        return result;
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
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < this.numFields(); i++) {
            TDItem tdItem =  tdItems.get(i);
            stringBuilder.append(tdItem.fieldType.toString())
                    .append("(").append(tdItem.fieldName).append("),");
        }
        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        return stringBuilder.toString();
    }
}
/*
    public int getSizeInBytes() {
        int totalSize = 0;
        for (Type type : typeAr) {
            totalSize += type.getSizeInBytes();
        }
        return totalSize;
    }
 */