package tests;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import columnar.ColumnarFile;
import diskmgr.PCounter;

import bufmgr.PageNotReadException;

import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.PositionData;
import heap.Tuple;
import index.ColumnIndexScan;
import index.IndexException;
import iterator.*;
import global.*;

public class Index implements GlobalConst {

	static String columnFile;
	static int victimColumnNumber;
	/**
	 * @param args
	 * @throws Exception 
	 * @throws UnknownKeyTypeException 
	 * @throws LowMemException 
	 * @throws SortException 
	 * @throws IndexException 
	 */
	public static void main(String[] args) throws IndexException, SortException, LowMemException, UnknownKeyTypeException, Exception 
	{
		
		String dbname = args[0];
		columnFile = args[1];
		String targetColumnNames = args[2];
		String indexType = args[3];
		
		victimColumnNumber = getVictimColumnNumber(targetColumnNames); // column on which we want to query
		
		initDB(dbname, 1000);
		
		Scanner s1 = new Scanner(new FileInputStream("/tmp/" + columnFile + "_schema.txt"));
		int numColumns = 0;
		while(s1.hasNextLine())	//count the no. of lines in schema file
		{
			s1.nextLine();
			numColumns++;
		}
		s1.close();
		
		Scanner s2 = new Scanner(new FileInputStream("/tmp/" + columnFile + "_schema.txt"));
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
	      
	    if(indexType.toLowerCase().equals("btree"))
	    {
	    	ColumnarFile cf = new ColumnarFile(columnFile, numColumns, type);
			cf.createBTreeIndex(victimColumnNumber);
			ColumnIndexScan dummy = new ColumnIndexScan();
			dummy.scanBTreeIndex(cf, victimColumnNumber, null);
					
			System.out.println("Created B-Tree successfully");
	    	
	    }
	    
	    if(indexType.toLowerCase().equals("bitmap"))
	    {
	    		throw new Exception("Please use RunMakeBitMap.java from tests package.");
	    }
	    System.out.println("Read count: " + PCounter.rcounter);
	    System.out.println("Write count: " + PCounter.wcounter);
	}
	
	private static int getVictimColumnNumber(String victimColumnName) throws FileNotFoundException 
	{
		Scanner s = new Scanner(new FileInputStream("/tmp/" + columnFile + "_schema.txt"));
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
	
	static void initDB(String dbname, int numBuf)
	{
		 String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase."+dbname;
		 SystemDefs sysdef = new SystemDefs( dbpath, 1000, numBuf, "Clock" );
	}
	
}
