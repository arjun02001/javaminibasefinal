package tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import columnar.ColumnarFile;
import diskmgr.PCounter;
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
	static String columnFile;
	static int victimColumnNumber;
	public static void main(String[] args) throws Exception 
	{			
		String dbname = args[0];
		columnFile = args[1];
		String victimColumnName = args[2];
		String sortOrder = args[3];
		
		initDB(dbname);	 
		int SORTPGNUM = 12;
		
		victimColumnNumber = getVictimColumnNumber(victimColumnName);
		
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
			    
		TupleOrder[] order = new TupleOrder[1];
		if(sortOrder.toLowerCase().contains("asc"))
		{
			order[0] = new TupleOrder(TupleOrder.Ascending);
		}
		else
		{
			order[0] = new TupleOrder(TupleOrder.Descending);
		}
	    
	    ColumnarFileScan cfS = new ColumnarFileScan(columnFile, type, strSizes, (short) type.length, Sprojection.length, Sprojection, null);
	    
	    ColumnarSort cSort = null;
	    try
	    {
	    	cSort = new ColumnarSort(type, (short)type.length, strSizes, columnFile, victimColumnNumber, order[0], STRINGSIZE, SORTPGNUM, cfS);
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
	    	  t.print(type);
	    	  
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
		System.out.println("Read count: " + PCounter.rcounter);
		System.out.println("Write count: " + PCounter.wcounter);
	}
	
	private static int getVictimColumnNumber(String victimColumnName) throws FileNotFoundException 
	{
		Scanner s = new Scanner(new FileInputStream(DIRPATH + columnFile + "_schema.txt"));
		while(s.hasNext())
		{
			String[] colsInSchema = s.nextLine().split("\t");
			if(colsInSchema[0].equals(victimColumnName.toLowerCase()))
			{
				victimColumnNumber= Integer.parseInt(colsInSchema[1]);
				return victimColumnNumber;
			}
		}
		return -1;
	}
	
	static void initDB(String columnDBName)
	{

		 String dbpath = DIRPATH + System.getProperty("user.name") + ".minibase." + columnDBName;
		 SystemDefs sysdef = new SystemDefs( dbpath, 1000, 100, "Clock" );
	}

}
