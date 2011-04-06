package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import columnar.*;

import java.io.*;

/**
 * Index Scan iterator will directly access the required tuple using the
 * provided key. It will also perform selections and projections. information
 * about the tuples and the index are passed to the constructor, then the user
 * calls <code>get_next()</code> to get the tuples.
 */
public class ColumnIndexScan extends Iterator {

	
	public ColumnIndexScan()
	{}
	/**
	 * class constructor. set up the index scan.
	 * 
	 * @param index
	 *            type of the index (B_Index, Hash)
	 * @param relName
	 *            name of the input relation
	 * @param indName
	 *            name of the input index
	 * @param types
	 *            array of types in this relation
	 * @param str_sizes
	 *            array of string sizes (for attributes that are string)
	 * @param noInFlds
	 *            number of fields in input tuple
	 * @param noOutFlds
	 *            number of fields in output tuple
	 * @param outFlds
	 *            fields to project
	 * @param selects
	 *            conditions to apply, first one is primary
	 * @param fldNum
	 *            field number of the indexed field
	 * @param indexOnly
	 *            whether the answer requires only the key or the tuple
	 * @exception IndexException
	 *                error from the lower layer
	 * @exception InvalidTypeException
	 *                tuple type not valid
	 * @exception InvalidTupleSizeException
	 *                tuple size not valid
	 * @exception UnknownIndexTypeException
	 *                index type unknown
	 * @exception IOException
	 *                from the lower layer
	 */
	public ColumnIndexScan(IndexType index, final String relName,
			final String indName, AttrType types[], short str_sizes[],
			int noInFlds, int noOutFlds, FldSpec outFlds[], CondExpr selects[],
			final int fldNum, final boolean indexOnly) throws IndexException,
			InvalidTypeException, InvalidTupleSizeException,
			UnknownIndexTypeException, IOException {
		_fldNum = fldNum;
		_noInFlds = noInFlds;
		_types = types;
		_s_sizes = str_sizes;

		_selects = selects;
		perm_mat = outFlds;
		_noOutFlds = noOutFlds;
		_fldNum = fldNum;
		tuple1 = new Tuple();

        iscan = new IndexScan(new IndexType(IndexType.B_Index), relName, indName, _types, _s_sizes, 1, 1, perm_mat, selects, 1, true);
	}

	/**
	 * returns the next tuple. if <code>index_only</code>, only returns the key
	 * value (as the first field in a tuple) otherwise, retrive the tuple and
	 * returns the whole tuple
	 * 
	 * @return the tuple
	 * @exception IndexException
	 *                error from the lower layer
	 * @exception UnknownKeyTypeException
	 *                key type unknown
	 * @exception IOException
	 *                from the lower layer
	 */
	public Tuple get_next() throws IndexException, UnknownKeyTypeException,
			IOException {
		Tuple t = iscan.get_next();
		return t;
	}

	public RID get_next_rid() throws IndexException, UnknownKeyTypeException,
	IOException, ScanIteratorException {
		RID rid = iscan.get_next_rid();
		return rid;
	}
	
	public boolean Delete(KeyClass key, RID rid) throws IndexException, UnknownKeyTypeException,
	IOException, ScanIteratorException, DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException, KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException, RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException, ConstructPageException, DeleteRecException, IndexSearchException {
		this.iscan.Delete(key, rid);
		return true;
	}
	/**
	 * Cleaning up the index scan, does not remove either the original relation
	 * or the index from the database.
	 * 
	 * @exception IndexException
	 *                error from the lower layer
	 * @exception IOException
	 *                from the lower layer
	 */
	public void close() throws IOException, IndexException {
		iscan.close();
	}
	
	public boolean scanBTreeIndex(ColumnarFile cf, int columnNo, CondExpr selects[]) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
	    FldSpec[] projlist = new FldSpec[1];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    
	    AttrType[] tempTypes =  new AttrType[1];
	    tempTypes[0] = new AttrType(cf.type[columnNo-1].attrType);
	   
	    short[] strSizes = new short[1];
	    strSizes[0] = cf.attrSize[columnNo-1];
	
	    
	    // start index scan
	    ColumnIndexScan iscan = null;
        //IndexScan iscan = null;
 	    //iscan = new IndexScan(new IndexType(IndexType.B_Index), this.name + "." + (columnNo-1), this.name + "_Btree." + (columnNo-1), tempTypes, strSizes, 1, 1, projlist, selects, 1, true);
	    iscan = new ColumnIndexScan(new IndexType(IndexType.B_Index), cf.heapFileNames[columnNo-1], cf.name + "_Btree." + (columnNo-1), tempTypes, strSizes, 1, 1, projlist, selects, 1, true);
	    
        Tuple t = new Tuple();
		RID rid = new RID();
	    String outStrVal = null;
	    Integer outIntVal = null;
	    Integer position = null;
	    PositionData newPositionData = new PositionData();
		
	    rid = iscan.get_next_rid();
	    
	    while (rid != null) 
	    {
	    	position = cf.columnFiles[columnNo-1].getPositionForRID(rid);
	    	newPositionData.position = position;
	    	TID tid = new TID(cf.numColumns);
		    tid = tid.constructTIDfromPosition(newPositionData, cf.name, cf.numColumns);
		    
		    t = cf.getTuple(tid);
    		t.print(cf.type);

			rid = iscan.get_next_rid();
		    
	    }

	    iscan.close();
	    System.out.println("BTreeIndex Scan Completed.\n"); 
	    
	    return true;
		
	}

	public boolean deleteTuple(ColumnarFile cf, int columnNo, CondExpr selects[]) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
	    FldSpec[] projlist = new FldSpec[1];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    
	    AttrType[] tempTypes =  new AttrType[1];
	    tempTypes[0] = new AttrType(cf.type[columnNo-1].attrType);
	   
	    short[] strSizes = new short[1];
	    strSizes[0] = cf.attrSize[columnNo-1];
	
	    
	    // start index scan
	    ColumnIndexScan iscan = null;
        //IndexScan iscan = null;
 	    //iscan = new IndexScan(new IndexType(IndexType.B_Index), this.name + "." + (columnNo-1), this.name + "_Btree." + (columnNo-1), tempTypes, strSizes, 1, 1, projlist, selects, 1, true);
	    iscan = new ColumnIndexScan(new IndexType(IndexType.B_Index), cf.heapFileNames[columnNo-1], cf.name + "_Btree." + (columnNo-1), tempTypes, strSizes, 1, 1, projlist, selects, 1, true);
	    
        Tuple t = new Tuple();
		RID rid = new RID();
	    String outStrVal = null;
	    Integer outIntVal = null;
	    Integer position = null;
	    PositionData newPositionData = new PositionData();
		Integer count=0;
	    rid = iscan.get_next_rid();
	    
	    while (rid != null) 
	    {
	    	position = cf.columnFiles[columnNo-1].getPositionForRID(rid);
	    	newPositionData.position = position;
			
    		rid = iscan.get_next_rid();

	    	TID tid = new TID(cf.numColumns);
		    tid = tid.constructTIDfromPosition(newPositionData, cf.name, cf.numColumns);
		    if (cf.markTupleDeleted(tid, cf))
    			count++; 
    		else
    			System.out.println("Error: markTupleDeleted() returned null" + "\n"); 
	    }
		System.out.println("Total Number of Records Deleted:" + count); 

	    
	    System.out.println("Delete Completed.\n"); 
	    iscan.close();
	    
	    return true;
		
	}

	
	public FldSpec[] perm_mat;
	private IndexFile indFile; // BitMapFile extends IndexFile??
	private IndexFileScan indScan; // BMFileScan extends IndexFileScan??
	private IndexScan iscan;
	private AttrType[] _types;
	private short[] _s_sizes;
	private CondExpr[] _selects;
	private int _noInFlds;
	private int _noOutFlds;
	private ColumnarFile f;
	private Tuple tuple1;
	private Tuple Jtuple;
	private int t1_size;
	private int _fldNum;
	private boolean index_only;

}