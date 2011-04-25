package tests;

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

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception 
	{
		//String[] nlj1 = {"/host/nljdata1.txt", "mydb", "myfile1", "4"};
		//String[] nlj2 = {"/host/nljdata2.txt", "mydb", "myfile2", "4"};
		//BatchInsert.main(nlj1);
		//BatchInsert.main(nlj2);
		
		initDB("mydb", 1000);
		
		/*
		 * Select c1.A, c1.B
		 * From myfile1 c1, myfile2 c2
		 * where c1.B = c2.B and c2.B = "Neveda" 
		 * */
		
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
		    
		//c1
		AttrType [] c1Type = {
		    	      new AttrType(AttrType.attrString), 
		    	      new AttrType(AttrType.attrString), 
		    	      new AttrType(AttrType.attrInteger), 
		    	      new AttrType(AttrType.attrInteger)
		    	    };
		short []   c1sizes = new short[2];
		c1sizes[0] = STRINGSIZE;
		c1sizes[1] = STRINGSIZE;
		    
		//c2	    
		AttrType [] c2Type = {
					new AttrType(AttrType.attrString), 
		    	    new AttrType(AttrType.attrString), 
		    	    new AttrType(AttrType.attrInteger), 
		    	    new AttrType(AttrType.attrInteger)
		    	    };
		short [] c2sizes = new short[2];
   	    c2sizes[0] = STRINGSIZE;
   	    c2sizes[1] = STRINGSIZE;
		
   	    FldSpec []  proj1 = {
   	    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
   	    	       new FldSpec(new RelSpec(RelSpec.outer), 2),
   	    	       new FldSpec(new RelSpec(RelSpec.outer), 3),
   	    	       new FldSpec(new RelSpec(RelSpec.outer), 4),
   	    	    }; 

   	   iterator.Iterator am = null;
   	       	       	  
       try 
       {   	 
    	   	am = new ColumnarFileScan("myfile1", c1Type, c1sizes ,(short) c1Type.length, 4,  proj1, null);   	  
       }
    	    
       catch (Exception e) 
       {
    	   System.err.println ("*** Error creating scan for ColumnIndexScan");
    	   System.err.println (""+e);
    	   Runtime.getRuntime().exit(1);
       }
       
    	ColumnarNestedLoopsJoins nlj = null;
    	try 
    	{
    	      nlj = new ColumnarNestedLoopsJoins (c1Type, 4, c1sizes, c2Type, 4, c2sizes, 10, am, "myfile2", outFilter2, null, proj1, 4);
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
    		t.print(c1Type);
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
