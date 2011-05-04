package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import columnar.*;

import java.io.*;
import java.util.Scanner;

/**
 * Index Scan iterator will directly access the required tuple using the
 * provided key. It will also perform selections and projections. information
 * about the tuples and the index are passed to the constructor, then the user
 * calls <code>get_next()</code> to get the tuples.
 */
public class ColumnarIndexScan extends Iterator implements GlobalConst{

	
	public ColumnarIndexScan()
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
	 * @throws Exception 
	 * @throws InvalidSlotNumberException 
	 */
	public ColumnarIndexScan(final String relName, int[] fldNum, String[] indexTypes,
			final String[] indName, AttrType[] type, short[] str_sizes,
			int noInFlds, int noOutFlds, FldSpec[] outFlds, CondExpr[] selects,
			final boolean indexOnly) throws InvalidSlotNumberException, Exception {
		_fldNum = fldNum;
		_noInFlds = noInFlds;
		_types = type;
		_s_sizes = str_sizes;

		_selects = selects;
		perm_mat = outFlds;
		_noOutFlds = noOutFlds;
		_fldNum = fldNum;
		tuple1 = new Tuple();
        // iscan = new IndexScan(new IndexType(IndexType.B_Index), relName, indName, _types, _s_sizes, 1, 1, perm_mat, selects, 1, true);
		
		ColumnarFile cf = new ColumnarFile(relName, type.length, type);
		
		Heapfile newHeap = new Heapfile(cf.heapFileNames[1]);
		MarkEliminate newEliminate = new MarkEliminate(relName, newHeap.getRecCnt());
		
		//System.out.println("------------ "+newHeap.getRecCnt());
		FldSpec [] Sprojection = new FldSpec[fldNum.length];	//create the projection array
		for(int i = 0; i < fldNum.length; i++)
		{
			Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), (i + 1));
		}

		boolean isFirst = true;
		int numConditions = selects.length;
		for (int i=0; i<numConditions ; i++)
		{
			
			CondExpr[] expr2 = new CondExpr[2];
	    	Mark delete = new Mark();
		    expr2[0] = new CondExpr();
	    	expr2[0].op = new AttrOperator(selects[i].op.attrOperator);
	    	expr2[0].next  = null;
	    	expr2[0].type1 = new AttrType(AttrType.attrSymbol);
	    	expr2[0].type2 = new AttrType(type[fldNum[i]-1].attrType);
	    	expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
	    	if(expr2[0].type2.attrType == AttrType.attrString)
		    {
		    	expr2[0].operand2.string = selects[i].operand2.string;
		    }
		    else if (expr2[0].type2.attrType == AttrType.attrInteger)
		    {
		    	expr2[0].operand2.integer = selects[i].operand2.integer;
		    }
	    	expr2[1] = null;
	    	
	    	if(indexTypes[i].equals("btree"))
	    	{
	    		System.out.println("\n***** BTree ******");
	    		
	    		
    	    	cf.createBTreeIndex(fldNum[i]);
    			ColumnIndexScan dummy = new ColumnIndexScan();																																				
    			//System.out.println(cf.name +" - Btree : Field Num - " +fldNum[i] );
    			dummy.scanColumnarBTreeIndex(cf, fldNum[i], expr2, type, isFirst, newEliminate);
    			cf.btreeIndexFiles[fldNum[i]-1].destroyFile();
    	    	
	    	}
	    	else if(indexTypes[i].equals("bitmap"))
	    	{
	    		System.out.println("\n***** Bitmap ******");
	    		
	    	}
	    	else if(indexTypes[i].equals("filescan"))
	    	{
			    System.out.println("\n***** FileScan ******");
	    		
	    		ColumnarFileScan cs = new ColumnarFileScan(relName, type, str_sizes ,(short) type.length, (short)Sprojection.length,Sprojection, expr2);
			    int position = 0; int k=0;
			    Tuple newT = new Tuple();
			    while((newT=cs.get_next())!=null)
			    {	
			    	if (!delete.isDeleted(relName, cs.position_counter))
			    	{
			    		if(isFirst)
			    			newEliminate.setEliminated(relName, position);
			    		else
			    			newEliminate.testAndSetEliminated(relName, position);
				    	
			    		//System.out.println(relName + " " + cs.position_counter);
			    	    //printProjectionData(newT, targetColumnNames, type);
			    		k++;
			    	}
			    }System.out.println("Number of records displayed is: " + k);
	    	}
	    	else if(indexTypes[i].equals("columnscan"))
	    	{
	    		System.out.println("\n***** ColumnScan ******");
	    		
			    FileScanByColnPos fscp = new FileScanByColnPos(relName, type, str_sizes, (short)type.length, (short)Sprojection.length, Sprojection, expr2, fldNum[i]);
		    	PositionData newPD = new PositionData();
		    	newPD = null;int k=0;
		    	while((newPD=fscp.get_next_PositionData())!=null)
		    	{
		    		if(!delete.isDeleted(relName, newPD.position))
			    	{
		    			//System.out.print(newPD.position + ": ");
		    			if(isFirst)
			    			newEliminate.setEliminated(relName, newPD.position);
			    		else
			    			newEliminate.testAndSetEliminated(relName, newPD.position);
				    	
		    			k++;
		    		}
		    	} System.out.println("Number of records displayed is: " + k);

	    	}
	    isFirst = false;
		}
       	System.out.println("Read count: " + PCounter.rcounter);
   		System.out.println("Write count: " + PCounter.wcounter);
   		newEliminate.close(relName);
	}
	
	
	
	
	public void Close() throws IndexException, IOException 
	{
	//	iscan.close();
		
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
	
	public boolean scanBTreeIndex(ColumnarFile cf, int columnNo, CondExpr selects[],  String targetColumnNames, AttrType[] type) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
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
	    int count = 0;
	    Mark delete = new Mark();
	    while (rid != null) 
	    {
	    	position = cf.columnFiles[columnNo-1].getPositionForRID(rid);
	    	if(!delete.isDeleted(cf.name, position))
	    	{	
		    	count++;
	    		newPositionData.position = position;
		    	TID tid = new TID(cf.numColumns);
			    tid = tid.constructTIDfromPosition(newPositionData, cf.name, cf.numColumns);
			    
			    t = cf.getTuple(tid);
			    printProjectionData(cf.name,t,targetColumnNames, type);
			    //	    		t.print(cf.type);
	    	}
			rid = iscan.get_next_rid();
		    
	    }

	    iscan.close();
	    System.out.println("Number of records displayed is: " + count); 
	    System.out.println("BTreeIndex Scan Completed\n" ); 
	    
	    return true;
		
	}

	public boolean checkIndex(ColumnarFile cf, int columnNo, CondExpr selects[]) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
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
	    int count = 0;
	    Mark delete = new Mark();
	    while (rid != null) 
	    {
	    	position = cf.columnFiles[columnNo-1].getPositionForRID(rid);
	    	if(!delete.isDeleted(cf.name, position))
	    	{	
		    	count++;
	    		newPositionData.position = position;
		    	TID tid = new TID(cf.numColumns);
			    tid = tid.constructTIDfromPosition(newPositionData, cf.name, cf.numColumns);
			    
			    t = cf.getTuple(tid);
			    t.print(cf.type);
	    	}
			rid = iscan.get_next_rid();
		    
	    }

	    iscan.close();
	    System.out.println("Number of records displayed is: " + count); 
	    System.out.println("BTreeIndex Scan Completed\n" ); 
	    
	    return true;
		
	}


	public boolean checkColumnarIndex(ColumnarFile cf, int columnNo, CondExpr selects[]) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
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
	    int count = 0;
	    Mark delete = new Mark();
	    while (rid != null) 
	    {
	    	position = cf.columnFiles[columnNo-1].getPositionForRID(rid);
	    	if(!delete.isDeleted(cf.name, position))
	    	{	
		    	count++;
	    		newPositionData.position = position;
		    	TID tid = new TID(cf.numColumns);
			    tid = tid.constructTIDfromPosition(newPositionData, cf.name, cf.numColumns);
			    
			    t = cf.getTuple(tid);
			    t.print(cf.type);
	    	}
			rid = iscan.get_next_rid();
		    
	    }

	    iscan.close();
	    System.out.println("Number of records displayed is: " + count); 
	    System.out.println("BTreeIndex Scan Completed\n" ); 
	    
	    return true;
		
	}

	private static void printProjectionData(String columnFile, Tuple t, String targetColumnNames, AttrType[] type) throws FieldNumberOutOfBoundException, IOException 
	{
		String[] columnNamesToProject = targetColumnNames.split(" ");
		System.out.print("[");
		for(int i = 0; i < columnNamesToProject.length; i++)
		{
			int columnNumber = getVictimColumnNumber(columnFile, columnNamesToProject[i]);
			if(type[columnNumber - 1].attrType == AttrType.attrInteger)
			{
				System.out.print(t.getIntFld(columnNumber));
			}
			if(type[columnNumber - 1].attrType == AttrType.attrString)
			{
				System.out.print(t.getStrFld(columnNumber));
			}
			if(i != (columnNamesToProject.length - 1))
			{
				System.out.print(", ");
			}
		}
		System.out.println("]");
	}

	private static int getVictimColumnNumber(String columnFile, String victimColumnName) throws FileNotFoundException 
	{
		Scanner s = new Scanner(new FileInputStream(DIRPATH + columnFile + "_schema.txt"));
		while(s.hasNext())
		{
			String[] colsInSchema = s.nextLine().split("\t");
			if(colsInSchema[0].equals(victimColumnName.toLowerCase()))
			{
				return Integer.parseInt(colsInSchema[1]);
			}
		}
		return -1;
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
	private IndexScan iscan;
	private AttrType[] _types;
	private short[] _s_sizes;
	private CondExpr[] _selects;
	private int _noInFlds;
	private int _noOutFlds;
	private ColumnarFile columnarFile;
	private Tuple tuple1;
	private Tuple Jtuple;
	private int t1_size;
	private int[] _fldNum;
	private boolean index_only;

}