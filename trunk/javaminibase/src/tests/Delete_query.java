package tests;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import columnar.ColumnarFile;

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

public class Delete_query implements GlobalConst {

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
		String accessType = args[5];
		
		String victimColumnName = valueConstraint.split(" ")[0].toLowerCase(); //assuming value constraint is "a = South_Dakota"
		String operator = valueConstraint.split(" ")[1];
		String value = valueConstraint.split(" ")[2];
		
		victimColumnNumber = getVictimColumnNumber(victimColumnName); // column on which we want to query
		
		initDB(dbname, Integer.parseInt(numBuf));
		
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
	      
	    
	    if(accessType.toLowerCase().equals("filescan"))
	    {
	    	
	    	CondExpr[] expr1 = new CondExpr[2];
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
		    ColumnarFileScan cs = new ColumnarFileScan(columnFile, type, strSizes ,(short) type.length, (short)Sprojection.length,Sprojection, expr1);
		    Tuple newT = new Tuple();
		    newT = null;int i=0;
		    while((newT=cs.get_next())!=null)
		    {	
	    	    printProjectionData(newT, targetColumnNames, type);
	    	    i++;
		    }
		    System.out.println("Number of records displayes is: " + i);
	    }
	    
	    if(accessType.toLowerCase().equals("columnscan"))
	    {
	    	CondExpr[] expr2 = new CondExpr[2];
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
	    	
	    	
	    	FileScanByColnPos fscp = new FileScanByColnPos(columnFile, type, strSizes, (short)type.length, (short)Sprojection.length, Sprojection, expr2, victimColumnNumber);
	    	PositionData newPD = new PositionData();
	    	newPD = null;int i=0;
	    	while((newPD=fscp.get_next_PositionData())!=null)
	    	{
	    		TID tid = new TID(type.length);
	    		tid = tid.constructTIDfromPosition(newPD, columnFile, type.length);
	    		ColumnarFile cf = new ColumnarFile(columnFile, type.length, type);
	    		Tuple t = new Tuple();
	    		t = cf.getTuple(tid);
	    		printProjectionData(t, targetColumnNames, type);
	    		i++;
	    	}
	    	
	    	System.out.println("Number of records displayed is: " + i);
	    }
	    
	    if(accessType.toLowerCase().equals("btree"))
	    {
	    	CondExpr[] expr = new CondExpr[2];
			expr[0] = new CondExpr();
			expr[0].op = new AttrOperator(AttrOperator.aopEQ);
			expr[0].next = null;
			expr[0].type1 = new AttrType(AttrType.attrSymbol);
			expr[0].type2 = new AttrType(type[victimColumnNumber - 1].attrType);
			expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), victimColumnNumber);
			expr[0].operand2.string = value;
			expr[1] = null;
			
			ColumnarFile cf = new ColumnarFile(columnFile, numColumns, type);
			
			cf.createBTreeIndex(victimColumnNumber);
			ColumnIndexScan dummy = new ColumnIndexScan();
			dummy.checkIndex(cf, victimColumnNumber, expr);
			
			dummy.deleteTuple(cf, victimColumnNumber, expr);

			cf.purgeAllDeletedTuples();
			
			System.out.println("Finished purging records");
	    	
	    }
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
	
	static void initDB(String dbname, int numBuf)
	{
		 String dbpath = DIRPATH + System.getProperty("user.name") + ".minibase."+dbname;
		 SystemDefs sysdef = new SystemDefs( dbpath, 1000, numBuf, "Clock" );
	}
	
}
