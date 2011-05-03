package tests;

import java.io.IOException;

import bufmgr.PageNotReadException;
import columnar.ColumnarFile;
import columnar.TupleScan;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.SystemDefs;
import global.TupleOrder;
import index.*;
import iterator.*;
import heap.*;

public class ColumnarNestedLoopJoinTest implements GlobalConst 
{
	static AttrType[] c1Type = null;
	static AttrType[] c2Type = null;
	static short[] c1sizes = null;
	static short[] c2sizes = null;
	static iterator.Iterator am = null;
	static String innerFile = null;
	static String outerFile = null;
	
	public static void main(String[] args) throws Exception 
	{
		//String[] nlj1 = {"/host/nljdata1.txt", "mydb", "myfile1", "4"};
		//String[] nlj2 = {"/host/nljdata2.txt", "mydb", "myfile2", "4"};
		//BatchInsert.main(nlj1);
		//BatchInsert.main(nlj2);
		
		String dbName = "mydb";
		outerFile = "myfile1";
		innerFile = "myfile2";
		
		initDB(dbName, 1000);
		    
		//type for relation 1
		c1Type = new AttrType[4];
		c1Type[0] = new AttrType(AttrType.attrString); 
	    c1Type[1] = new AttrType(AttrType.attrString);
	    c1Type[2] = new AttrType(AttrType.attrInteger); 
	    c1Type[3] = new AttrType(AttrType.attrInteger);
		
	    //string size for relation 1
		c1sizes = new short[2];
		c1sizes[0] = STRINGSIZE;
		c1sizes[1] = STRINGSIZE;
		    
		//type for relation 2
		c2Type = new AttrType[4];
		c2Type[0] = new AttrType(AttrType.attrString);
		c2Type[1] = new AttrType(AttrType.attrString); 
		c2Type[2] = new AttrType(AttrType.attrInteger); 
		c2Type[3] = new AttrType(AttrType.attrInteger);
		    	    
		//string size for relation 2
		c2sizes = new short[2];
   	    c2sizes[0] = STRINGSIZE;
   	    c2sizes[1] = STRINGSIZE;
		
   	    FldSpec []  proj1 = {
   	    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
   	    	       new FldSpec(new RelSpec(RelSpec.outer), 2),
   	    	       new FldSpec(new RelSpec(RelSpec.outer), 3),
   	    	       new FldSpec(new RelSpec(RelSpec.outer), 4),
   	    	    }; 
   	       	       	  
       try 
       {   	 
    	   	am = new ColumnarFileScan(outerFile, c1Type, c1sizes ,(short) c1Type.length, c1Type.length, proj1, null);   	  
       }
    	    
       catch (Exception e) 
       {
    	   System.err.println ("*** Error creating scan for ColumnIndexScan");
    	   System.err.println (""+e);
    	   Runtime.getRuntime().exit(1);
       }
       
       String test = args[0];
       if(test.toLowerCase().equals("test1"))
       {
    	   test1();
       }
       else if(test.toLowerCase().equals("test2"))
       {
    	   test2();
       }
       else if (test.toLowerCase().equals("test3"))
       {
    	   test3();
       }
       
	}
	
	static void test1() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception
	{
		System.out.println("select c1.A, c2.A, c2.B");
		System.out.println("from myfile1 c1, myfile2 c2");
		System.out.println("where c1.B = c2.B and c2.B = 'Nevada'");
		System.out.println("-----------------------------------------");
		CondExpr [] outFilter2 = new CondExpr[3];
		outFilter2[0] = new CondExpr();
		outFilter2[1] = new CondExpr();
		outFilter2[2] = new CondExpr();
		    
		outFilter2[0].next  = null;
		outFilter2[0].op    = new AttrOperator(AttrOperator.aopEQ); 
		outFilter2[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter2[0].type2 = new AttrType(AttrType.attrSymbol);   
		outFilter2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		outFilter2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
		    
		outFilter2[1].op   = new AttrOperator(AttrOperator.aopEQ);
		outFilter2[1].next = null;
		outFilter2[1].type1 = new AttrType(AttrType.attrSymbol);
		outFilter2[1].type2 = new AttrType(AttrType.attrString);
		outFilter2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
		outFilter2[1].operand2.string = "Nevada";
		 
		outFilter2[2] = null;
		
		FldSpec []  proj1 = {
	    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
	    	       new FldSpec(new RelSpec(RelSpec.innerRel), 1),
	    	       new FldSpec(new RelSpec(RelSpec.innerRel), 2),
	    	       //new FldSpec(new RelSpec(RelSpec.innerRel), 4),
	    	    }; 
		
		AttrType[] outType = {
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrString),
		};
		
		ColumnarNestedLoopsJoins nlj = null;
    	try 
    	{
    	      nlj = new ColumnarNestedLoopsJoins (c1Type, c1Type.length, c1sizes, c2Type, c2Type.length, c2sizes, 10, am, innerFile, outFilter2, null, proj1, proj1.length);
    	}
    	catch (Exception e) 
    	{
    	      System.err.println ("*** Error preparing for nested_loop_join");
    	      System.err.println (""+e);
    	      e.printStackTrace();
    	      Runtime.getRuntime().exit(1);
    	}
    	Tuple t = new Tuple();
    	int count = 0;
    	while((t = nlj.get_next()) != null)
    	{
    		t.print(outType);
    		count++;
    	}
    	System.out.println("Total records: " + count); 
    	System.out.println("Read count: " + PCounter.rcounter);
    	System.out.println("Write count: " + PCounter.wcounter);
	}
	
	static void test2() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception
	{
		System.out.println("select c1.A, c2.A, c2.B");
		System.out.println("from myfile1 c1, myfile2 c2");
		System.out.println("where c1.A = c2.A");
		System.out.println("-----------------------------------------");
		
		CondExpr [] outFilter2 = new CondExpr[2];
		outFilter2[0] = new CondExpr();
		outFilter2[1] = new CondExpr();
		    
		outFilter2[0].next  = null;
		outFilter2[0].op    = new AttrOperator(AttrOperator.aopEQ); 
		outFilter2[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter2[0].type2 = new AttrType(AttrType.attrSymbol);   
		outFilter2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), 1);
		outFilter2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), 1);
		    
		outFilter2[1] = null;
		
		FldSpec []  proj1 = {
	    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
	    	       new FldSpec(new RelSpec(RelSpec.innerRel), 1),
	    	       new FldSpec(new RelSpec(RelSpec.innerRel), 2),
	    	       //new FldSpec(new RelSpec(RelSpec.innerRel), 4),
	    	    }; 
		
		AttrType[] outType = {
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrString),
		};
		
		ColumnarNestedLoopsJoins nlj = null;
    	try 
    	{
    	      nlj = new ColumnarNestedLoopsJoins (c1Type, c1Type.length, c1sizes, c2Type, c2Type.length, c2sizes, 10, am, innerFile, outFilter2, null, proj1, proj1.length);
    	}
    	catch (Exception e) 
    	{
    	      System.err.println ("*** Error preparing for nested_loop_join");
    	      System.err.println (""+e);
    	      e.printStackTrace();
    	      Runtime.getRuntime().exit(1);
    	}
    	Tuple t = new Tuple();
    	int count = 0;
    	while((t = nlj.get_next()) != null)
    	{
    		t.print(outType);
    		count++;
    	}
    	System.out.println("Total records: " + count); 
    	System.out.println("Read count: " + PCounter.rcounter);
    	System.out.println("Write count: " + PCounter.wcounter);
	}
	
	static void test3() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception
	{
		System.out.println("select c1.A, c1.B, c1.C");
		System.out.println("from myfile1 c1, myfile2 c2");
		System.out.println("where c1.B = c2.B and c1.C > '2'");
		System.out.println("order by c1.A");
		System.out.println("-----------------------------------------");
		
		CondExpr [] outFilter2 = new CondExpr[3];
		outFilter2[0] = new CondExpr();
		outFilter2[1] = new CondExpr();
		outFilter2[2] = new CondExpr();
		    
		outFilter2[0].next  = null;
		outFilter2[0].op    = new AttrOperator(AttrOperator.aopEQ); 
		outFilter2[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter2[0].type2 = new AttrType(AttrType.attrSymbol);   
		outFilter2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		outFilter2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
		    
		outFilter2[1].op   = new AttrOperator(AttrOperator.aopGT);
		outFilter2[1].next = null;
		outFilter2[1].type1 = new AttrType(AttrType.attrSymbol);
		outFilter2[1].type2 = new AttrType(AttrType.attrInteger);
		outFilter2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
		outFilter2[1].operand2.integer = 2;
		 
		outFilter2[2] = null;
		
		FldSpec []  proj1 = {
	    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 2),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 3)
	    	    }; 
		
		AttrType[] outType = {
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger)
		};
		
		short[] outsizes = new short[2];
		outsizes[0] = STRINGSIZE;
		outsizes[1] = STRINGSIZE;
		
		ColumnarNestedLoopsJoins nlj = null;
    	try 
    	{
    	      nlj = new ColumnarNestedLoopsJoins (c1Type, c1Type.length, c1sizes, c2Type, c2Type.length, c2sizes, 10, am, innerFile, outFilter2, null, proj1, proj1.length);
    	}
    	catch (Exception e) 
    	{
    	      System.err.println ("*** Error preparing for nested_loop_join");
    	      System.err.println (""+e);
    	      e.printStackTrace();
    	      Runtime.getRuntime().exit(1);
    	}
    	
    	Sort sort = new Sort(outType, (short) outType.length, outsizes, nlj, 1, new TupleOrder(TupleOrder.Ascending), STRINGSIZE, 10);
    	Tuple t = new Tuple();
    	int count = 0;
    	while((t = sort.get_next()) != null)
    	{
    		t.print(outType);
    		count++;
    	}
    	System.out.println("Total records: " + count); 
    	System.out.println("Read count: " + PCounter.rcounter);
    	System.out.println("Write count: " + PCounter.wcounter);
	}
	
	static void initDB(String dbname, int numBuf)
	{
		 String dbpath = DIRPATH + System.getProperty("user.name") + ".minibase."+dbname;
		 SystemDefs sysdef = new SystemDefs( dbpath, 1000, numBuf, "Clock" );
	}
}
