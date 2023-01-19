package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


import java.util.Map;
import java.util.HashMap;
/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
    //Stores a list of all tables in the database
    private static class Table{
        private DbFile file;
        private String name;
        private String pKeyField;

        public Table(DbFile file, String name, String pKeyField) {
            this.file = file;
            this.name = name;
            this.pKeyField = pKeyField;
        }
    }

    private Map<Integer, Table> tablesMap;
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        this.tablesMap = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // Duplicate name, remove the old one
        Iterator<Map.Entry<Integer, Table>> iter = tablesMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer, Table> entry = iter.next();
            if (name.equals(entry.getValue().name)) {
                iter.remove();
            }
        }
        //Don't need to worry about Duplicate id
        tablesMap.put(file.getId(), new Table(file, name, pkeyField));
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        if (name == null || this.tablesMap == null) {
            throw new NoSuchElementException();
        }
        for (Map.Entry<Integer, Table> entry : tablesMap.entrySet()) {
            if (name.equals(entry.getValue().name)) {
                return entry.getKey();
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        if (this.tablesMap == null || !tablesMap.containsKey(tableid)) {
            throw new NoSuchElementException();
        }
        return tablesMap.get(tableid).file.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        if (this.tablesMap == null || !tablesMap.containsKey(tableid)) {
            throw new NoSuchElementException();
        }
        return tablesMap.get(tableid).file;
    }

    public String getPrimaryKey(int tableid) {
        if (this.tablesMap == null || !tablesMap.containsKey(tableid)) {
            throw new NoSuchElementException();
        }
        return tablesMap.get(tableid).pKeyField;
    }

    public Iterator<Integer> tableIdIterator() {
        return tablesMap.keySet().iterator();
    }

    public String getTableName(int id) {
        if (tablesMap == null || !tablesMap.containsKey(id)) {
            throw new NoSuchElementException();
        }
        return tablesMap.get(id).name;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        this.tablesMap = new HashMap<Integer, Table>();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

