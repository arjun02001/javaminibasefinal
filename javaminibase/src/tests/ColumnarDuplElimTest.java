package tests;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

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
	static String columnFile;
	
	public static void main(String[] args) throws FileScanException, TupleUtilsException, InvalidRelation, HFException, HFBufMgrException, HFDiskMgrException, IOException 
	{
		String dbname = args[0];
		columnFile = args[1];
		
		initDB(dbname);
		int SORTPGNUM = 12;
		
		Scanner s1 = new Scanner(new FileInputStream(DIRPATH + columnFile + "_schema.txt"));
		int numColumns = 0;
		while(s1.hasNextLine())	//count the no. of lines in schema file
		{
			s1.nextLine();
			numColumns++;
		}
		s1.close();
		
		Scanner s2 = new Scanner(new FileInputStream(DIRPATH + columnFile + "_schema.txt"));
		AttrType[] type = new AttrType[numColumns];
		
		int j = 0;
		int strCount = 0;
		while(s2.hasNextLine())	//construct the type[]
		{
			String dataType = s2.nextLine().split("\t")[2].toLowerCase();
			if(dataType.equals("int"))
			{
				type[j++] = new AttrType(AttrType.attrInteger);
			}
			if(dataType.equals("char"))
			{
				type[j++] = new AttrType(AttrType.attrString);
				strCount++;
			}
		}
		
		short[] strSizes = new short[strCount];
		Arrays.fill(strSizes, (short)STRINGSIZE);
		
		FldSpec [] Sprojection = new FldSpec[numColumns];	//create the projection array
		for(int i = 0; i < numColumns; i++)
		{
			Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), (i + 1));
		}
		
	    ColumnarFileScan cfs = new ColumnarFileScan("myfile", type, strSizes, (short) type.length, Sprojection.length, Sprojection, null);
	    
	    ColumnarDuplElim cde = null;
	    try
	    {
	    	cde = new ColumnarDuplElim(type, (short)type.length, strSizes, cfs, SORTPGNUM, false);
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
	    	  t.print(type);
	    	  
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
