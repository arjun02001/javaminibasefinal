package tests;

import java.io.IOException;

import diskmgr.PCounter;

import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Tuple;
import iterator.ColumnarDuplElim;
import iterator.ColumnarFileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.RelSpec;
import iterator.TupleUtilsException;
import global.AttrType;
import global.GlobalConst;
import global.SystemDefs;
import global.TupleOrder;

public class ColumnarDuplElimTest implements GlobalConst
{

	
	public static void main(String[] args) throws FileScanException, TupleUtilsException, InvalidRelation, HFException, HFBufMgrException, HFDiskMgrException, IOException 
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
	    
	    FldSpec []  Sprojection = {
	    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 2),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 3),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 4),
	    	    }; 
	    
	    ColumnarFileScan cfs = new ColumnarFileScan("myfile", attrType, attrSize, (short) attrType.length, Sprojection.length, Sprojection, null);
	    
	    ColumnarDuplElim cde = null;
	    try
	    {
	    	cde = new ColumnarDuplElim(attrType, (short)attrType.length, attrSize, cfs, SORTPGNUM, false);
	    }
	    catch(Exception ex)
	    {
	    	ex.printStackTrace();
	    }
	    
	    Tuple t = new Tuple();
	    try 
	    {
	        t = cde.get_next();
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
	    			t = cde.get_next();
	    	  }
	    	  catch (Exception e) 
	    	  {    	  
	    		  e.printStackTrace();
	    	  }
	    }
	    
	    try 
	    {
	    	  cde.close();
		}
		catch (Exception e) 
		{
		      e.printStackTrace();
		}
		System.out.println("Read count: " + PCounter.rcounter);
		System.out.println("Write count: " + PCounter.wcounter);
	}
	
	static void initDB(String columnDBName)
	{
		 String dbpath = DIRPATH + System.getProperty("user.name") + ".minibase." + columnDBName;
		 SystemDefs sysdef = new SystemDefs( dbpath, 1000, 100, "Clock" );
	}

}
