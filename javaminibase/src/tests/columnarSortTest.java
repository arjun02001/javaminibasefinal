package tests;

import columnar.ColumnarFile;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import iterator.ColumnarFileScan;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;
import global.*;

public class columnarSortTest implements GlobalConst {

	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		//has to be modified
		
		int NUM_RECORDS = 10;
		int SORTPGNUM = 12;
		System.out.println("------------------------ TEST 1 --------------------------");
	    
	    boolean status = true;
	    
	    AttrType[] attrType = new AttrType[4];
	    attrType[0] = new AttrType(AttrType.attrString);
	    attrType[1] = new AttrType(AttrType.attrString);
	    attrType[2] = new AttrType(AttrType.attrInteger);
	    attrType[3] = new AttrType(AttrType.attrInteger);
	    
	    short[] attrSize = new short[2];
	    attrSize[0] =  STRINGSIZE;
	    attrSize[1] = STRINGSIZE;
	    
	    TupleOrder[] order = new TupleOrder[2];
	    order[0] = new TupleOrder(TupleOrder.Ascending);
	    order[1] = new TupleOrder(TupleOrder.Descending);
	    
	    // create a tuple of appropriate size
	    Tuple t = new Tuple();
	    try {
	      t.setHdr((short) attrType.length, attrType, attrSize);
	    }
	    catch (Exception e) {
	      status = false;
	      e.printStackTrace();
	    }

	    int size = t.size();
	    
	    // Create unsorted data file "test1.in"
	    //RID             rid;
	    //Heapfile        f = null;
	    TID tid;
	    ColumnarFile cf;
	    try {
	      //f = new Heapfile("test1.in");
	    	cf = new ColumnarFile("columnarFileForSort", attrType.length, attrType);
	    }
	    catch (Exception e) {
	      status = false;
	      e.printStackTrace();
	    }
	  // Do not think this bit is needed cos columnarFile gets created while batchinsert itself. 
	  /*  t = new Tuple(size);
	    try {
	      t.setHdr((short) attrType.length, attrType, attrSize);
	    }
	    catch (Exception e) {
	      status = false;
	      e.printStackTrace();
	    }
	    
	    for (int i=0; i<NUM_RECORDS; i++) {
	      try {
		t.setStrFld(1, data1[i]);
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	      
	      try {
		rid = f.insertRecord(t.returnTupleByteArray());
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	    }
*/
	    // create an iterator by open a file scan
	    FldSpec[] projlist = new FldSpec[4];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    projlist[1] = new FldSpec(rel, 2);
	    projlist[2] = new FldSpec(rel, 3);
	    projlist[3] = new FldSpec(rel, 4);
	    
	    //FileScan fscan = null;
	    ColumnarFileScan cs = null;
	    try {
	      //fscan = new FileScan("test1.in", attrType, attrSize, (short) 2, 2, projlist, null);
	    	
	      cs = new ColumnarFileScan("columnarFileForSort", attrType, attrSize ,
		    		(short) attrType.length, (short)projlist.length,projlist, null);
	    }
	    catch (Exception e) {
	      status = false;
	      e.printStackTrace();
	    }

	    // Sort "test1.in" 
	    Sort sort = null;
	    try {
	      sort = new Sort(attrType, (short) 2, attrSize, cs, 1, order[0], STRINGSIZE, SORTPGNUM);
	    }
	    catch (Exception e) {
	      status = false;
	      e.printStackTrace();
	    }
	    

	    int count = 0;
	    t = null;
	    String outval = null;
	    
	    try {
	      t = sort.get_next();
	    }
	    catch (Exception e) {
	      status = false;
	      e.printStackTrace(); 
	    }

	    boolean flag = true;
	    //not needed for us since this is comparison with sorted data 2
	   /* 
	    while (t != null) {
	      if (count >= NUM_RECORDS) {
		System.err.println("Test1 -- OOPS! too many records");
		status = false;
		flag = false; 
		break;
	      }
	      
	      try {
		outval = t.getStrFld(1);
	      }
	      catch (Exception e) {
		status = false;
		e.printStackTrace();
	      }
	      
	      if (outval.compareTo(data2[count]) != 0) {
		System.err.println("outval = " + outval + "\tdata2[count] = " + data2[count]);
		
		System.err.println("Test1 -- OOPS! test1.out not sorted");
		status = FAIL;
	      }
	      count++;

	      try {
		t = sort.get_next();
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	    }
	    if (count < NUM_RECORDS) {
		System.err.println("Test1 -- OOPS! too few records");
		status = FAIL;
	    }
	    else if (flag && status) {
	      System.err.println("Test1 -- Sorting OK");
	    }
	    */

	    // clean up
	    try {
	      sort.close();
	    }
	    catch (Exception e) {
	      status = false;
	      e.printStackTrace();
	    }
	    
	    System.err.println("------------------- TEST 1 completed ---------------------\n");
	    
	  //  return status;

	}

}
