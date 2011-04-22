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
import deliverables.MakeBitMap;
import deliverables.RunQueryOnBitMap;

public class QQuery implements GlobalConst {

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
		String valueConstraint = args[3];
		String numBuf = args[4];
		String indexType = args[5];
		
		String victimColumnName = valueConstraint.split(" ")[0].toLowerCase(); //assuming value constraint is "a = South_Dakota"
		String operator = valueConstraint.split(" ")[1];
		String value = valueConstraint.split(" ")[2];
	    
		victimColumnNumber = getVictimColumnNumber(victimColumnName); // column on which we want to query
		String[] arr = new String[3];
		
		arr[0] = args[0];
		arr[1] = args[1];
		
		initDB(dbname, 1000);
		
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
	      
	    
	    if(indexType.toLowerCase().equals("btree"))
	    {
	    	CondExpr[] expr2 = new CondExpr[2];
	    	Mark delete = new Mark();
		    expr2[0] = new CondExpr();
	    	expr2[0].op = new AttrOperator(returnOp(operator));
	    	expr2[0].next  = null;
	    	expr2[0].type1 = new AttrType(AttrType.attrSymbol);
	    	expr2[0].type2 = new AttrType(type[victimColumnNumber - 1].attrType);
	    	expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
	    	if(expr2[0].type2.attrType == AttrType.attrString)
		    {
		    	expr2[0].operand2.string = value;
		    }
		    else if (expr2[0].type2.attrType == AttrType.attrInteger)
		    {
		    	expr2[0].operand2.integer = Integer.parseInt(value);
		    }
	    	expr2[1] = null;
	    			System.out.println(columnFile +" "+ numColumns);
	    			ColumnarFile cf = new ColumnarFile(columnFile, numColumns, type);
	    	    	cf.createBTreeIndex(victimColumnNumber);
	    			ColumnIndexScan dummy = new ColumnIndexScan();																																				
	    			dummy.scanBTreeIndex(cf, victimColumnNumber, expr2, targetColumnNames, type);
	                
	    			cf.btreeIndexFiles[victimColumnNumber-1].destroyFile();
	                        
	                //projectBTreeIndex(cf, victimColumnNumber, expr2, targetColumnNames, type);
	    	System.out.println("B-Tree Scan Completed");
           	System.out.println("Read count: " + PCounter.rcounter);
       		System.out.println("Write count: " + PCounter.wcounter);
	                
	      }
	    if(indexType.toLowerCase().equals("filescan"))
	    {
	    	
	    	CondExpr[] expr1 = new CondExpr[2];
	    	Mark delete = new Mark();
		    expr1[0] = new CondExpr();
			expr1[0].op = new AttrOperator(returnOp(operator));
			expr1[0].next  = null;
		    expr1[0].type1 = new AttrType(AttrType.attrSymbol); 
		    expr1[0].type2 = new AttrType(type[victimColumnNumber - 1].attrType);
		    expr1[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),victimColumnNumber);
		    if(expr1[0].type2.attrType == AttrType.attrString)
		    {
		    	expr1[0].operand2.string = value;
		    }
		    else if (expr1[0].type2.attrType == AttrType.attrInteger)
		    {
		    	expr1[0].operand2.integer = Integer.parseInt(value);
		    }
		    expr1[1] = null;
		    PCounter.rcounter = PCounter.wcounter = 0;
		    ColumnarFileScan cs = new ColumnarFileScan(columnFile, type, strSizes ,(short) type.length, (short)Sprojection.length,Sprojection, expr1);
		    int i=0, position = 0;
		    Tuple newT = new Tuple();
		    while((newT=cs.get_next())!=null)
		    {	
		    	if (!delete.isDeleted(columnFile, cs.position_counter))
		    	{
		    		System.out.print(columnFile + " " + cs.position_counter + ": ");
		    	    		printProjectionData(newT, targetColumnNames, type);
		    		i++;
		    	}
		    }System.out.println("Number of records displayes is: " + i);
		
		    System.out.println("Read count: " + PCounter.rcounter);
		    System.out.println("Write count: " + PCounter.wcounter);
		
	    }
	    if(indexType.toLowerCase().equals("columnscan"))
	    {
	    	CondExpr[] expr2 = new CondExpr[2];
	    	Mark delete = new Mark();
		    expr2[0] = new CondExpr();
	    	expr2[0].op = new AttrOperator(returnOp(operator));
	    	expr2[0].next  = null;
	    	expr2[0].type1 = new AttrType(AttrType.attrSymbol);
	    	expr2[0].type2 = new AttrType(type[victimColumnNumber - 1].attrType);
	    	expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
	    	if(expr2[0].type2.attrType == AttrType.attrString)
		    {
		    	expr2[0].operand2.string = value;
		    }
		    else if (expr2[0].type2.attrType == AttrType.attrInteger)
		    {
		    	expr2[0].operand2.integer = Integer.parseInt(value);
		    }
	    	expr2[1] = null;
	    	
	    	PCounter.rcounter = PCounter.wcounter = 0;
		    FileScanByColnPos fscp = new FileScanByColnPos(columnFile, type, strSizes, (short)type.length, (short)Sprojection.length, Sprojection, expr2, victimColumnNumber);
	    	PositionData newPD = new PositionData();
	    	newPD = null;int i=0;
	    	while((newPD=fscp.get_next_PositionData())!=null)
	    	{
	    		if(!delete.isDeleted(columnFile, newPD.position))
		    	{
	    			System.out.print(newPD.position + ": ");
	    			TID tid = new TID(type.length);
	    			tid = tid.constructTIDfromPosition(newPD, columnFile, type.length);
	    			ColumnarFile cf = new ColumnarFile(columnFile, type.length, type);
	    			Tuple t = new Tuple();
	    			t = cf.getTuple(tid);
	    			printProjectionData(t, targetColumnNames, type);
	    			i++;
	    		}
	    	} System.out.println("Number of records displayed is: " + i);
    	System.out.println("Read count: " + PCounter.rcounter);
		System.out.println("Write count: " + PCounter.wcounter);
		}

	    if(indexType.toLowerCase().equals("bitmap"))
	    {
				String[] arr2 = new String[6];
				
				arr2[0] = args[0];
				arr2[1] = args[1];
				arr2[2] = args[2];
				arr2[3] = args[3];
				arr2[4] = args[4];
				arr2[5] = args[5];
			
				PCounter.rcounter = PCounter.wcounter = 0;
			    	
				try {
				RunQueryOnBitMap.queryOnBitMap(arr2);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Read count: " + PCounter.rcounter);
				System.out.println("Write count: " + PCounter.wcounter);
	    }
	}
	
	public static boolean projectBTreeIndex(ColumnarFile cf, int columnNo, CondExpr selects[], String targetColumnNames, AttrType[] type) throws HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		    FldSpec[] projlist = new FldSpec[1];
		    RelSpec rel = new RelSpec(RelSpec.outer); 
		    projlist[0] = new FldSpec(rel, 1);
		    
		    AttrType[] tempTypes =  new AttrType[1];
		    tempTypes[0] = new AttrType(cf.type[columnNo-1].attrType);
		   
		    short[] strSizes = new short[1];
		    //strSizes[0] = cf.attrSize[columnNo-1];
		    strSizes[0] = STRINGSIZE;
		
		    
		    // start index scan
		    ColumnIndexScan iscan = null;
		    PCounter.rcounter = PCounter.wcounter = 0;
		    iscan = new ColumnIndexScan(new IndexType(IndexType.B_Index), cf.heapFileNames[columnNo-1], cf.name + "122_Btree." + (columnNo-1), tempTypes, strSizes, 1, 1, projlist, selects, 1, true);
		    
	        Tuple t = new Tuple();
			RID rid = new RID();
		    String outStrVal = null;
		    Integer outIntVal = null;
		    Integer position = null;
		    PositionData newPositionData = new PositionData();
		    																														
		    
		    //rid = iscan.get_next_rid();
		    int i = 0;
		    Mark delete = new Mark();
		    while ((rid = iscan.get_next_rid()) != null) 
		    {    	
		    	position = cf.columnFiles[columnNo-1].getPositionForRID(rid);
		    	if(!delete.isDeleted(cf.name, position))
		    	{
		    		System.out.print(position + ": ");
		    		newPositionData.position = position;
		    	
			    	TID tid = new TID(cf.numColumns);
			    	tid = new TID(cf.numColumns);
				    tid = tid.constructTIDfromPosition(newPositionData, cf.name, cf.numColumns);
				    t = cf.getTuple(tid);
				    //System.out.println("*************** Is Delete: " + position);
				    printProjectionData(t,targetColumnNames, type);
				    ++i;
		    	}//rid = iscan.get_next_rid();
		    }
		    System.out.println("Number of records displayed is: " + i);
		    iscan.close();
		    System.out.println("BTreeIndex Scan Completed");
		    return true;
	}

	private static void printProjectionData(Tuple t, String targetColumnNames, AttrType[] type) throws FieldNumberOutOfBoundException, IOException 
	{
		String[] columnNamesToProject = targetColumnNames.split(" ");
		System.out.print("[");
		for(int i = 0; i < columnNamesToProject.length; i++)
		{
			int columnNumber = getVictimColumnNumber(columnNamesToProject[i]);
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
	
	static void initDB(String dbname, int numBuf)
	{
		 String dbpath = DIRPATH + System.getProperty("user.name") + ".minibase."+dbname;
		 SystemDefs sysdef = new SystemDefs( dbpath, 1000, numBuf, "Clock" );
	}
	static int returnOp(String operator)
	{ 
		//aopEQ   = 0; aopLT   = 1; aopGT   = 2; aopNE   = 3; aopLE   = 4; aopGE   = 5; aopNOT  = 6; aopNOP  = 7;	opRANGE = 8; 
		
		if(operator.equals("="))
			return 0;
		else if(operator.equals("<"))
			return 1;
		else if (operator.equals(">"))
			return 2;
		else if(operator.equals("<>"))
			return 3;
		else if (operator.equals("<="))
			return 4;
		else if (operator.equals(">="))
			return 5;
		else if (operator.equals("<>"))
			return 6;
		return -1;
	}

	
}
