package tests;

import java.io.File;
import java.io.IOException;

import columnar.ColumnarFile;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import iterator.ColumnarFileScan;
import iterator.ColumnarSort;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;
import global.*;

public class ColumnarSortTest implements GlobalConst 
{
	public static void main(String[] args) throws Exception 
	{			
		initDB("mydb");	 
		int SORTPGNUM = 12;
		
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
	    
	    FldSpec []  Sprojection = {
	    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 2),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 3),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 4),
	    	    }; 
	    
	    ColumnarFileScan cfS = new ColumnarFileScan("myfile", attrType, attrSize, (short) attrType.length, Sprojection.length, Sprojection, null);
	    
	    ColumnarSort cSort = null;
	    try
	    {
	    	cSort = new ColumnarSort(attrType,(short)attrType.length,attrSize,"myfile",1,order[0],STRINGSIZE, SORTPGNUM, cfS);
	    }
	    catch (Exception e) 
	    {      
		      e.printStackTrace();
		}
	    
	    Tuple t = new Tuple();
	    try 
	    {
	        t = cSort.get_next();
	    }
	    catch (Exception e) 
	    { 
	        e.printStackTrace(); 
	    }
	    
	    while(t!=null)
	    {
	    	  t.print(attrType);
	    	  
	    	  try 
	    	  {
	    			t = cSort.get_next();
	    	  }
	    	  catch (Exception e) 
	    	  {    	  
	    		  e.printStackTrace();
	    	  }
	    }
	    
	    try 
	    {
	    	  cSort.close();
		}
		catch (Exception e) 
		{
		      e.printStackTrace();
		}
	}
	
	static void initDB(String columnDBName)
	{

		 String dbpath = DIRPATH + System.getProperty("user.name") + ".minibase." + columnDBName;
		 SystemDefs sysdef = new SystemDefs( dbpath, 1000, 100, "Clock" );
	}

}
