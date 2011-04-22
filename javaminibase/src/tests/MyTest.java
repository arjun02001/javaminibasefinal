package tests;

import index.ColumnIndexScan;
import iterator.*;
import global.*;
import heap.*;
import columnar.*;

class Employee
{
	int eid;
	String ename;
	public Employee(int eid, String ename)
	{
		this.eid = eid;
		this.ename = ename;
	}
}

public class MyTest implements GlobalConst
{
	public static void main(String[] args) throws Exception
	{

		
		
		initDB();
		Employee emp1, emp2, emp3;
		emp1 = new Employee(1, "arjun");
		emp2 = new Employee(2, "anwasha");
		short numColumns = 2;
			 
		AttrType[] type = new AttrType[numColumns];
		type[0] = new AttrType(AttrType.attrInteger);
		type[1] = new AttrType(AttrType.attrString);
		
		
		
	    System.out.println("**********************");
		ColumnarFile cf = new ColumnarFile("MyTest", numColumns, type);
		System.out.println("**********************");
		
		short[] strSizes = new short[1];
		strSizes[0] = STRINGSIZE;
		
		Tuple t = new Tuple();
		t.setHdr(numColumns, type, strSizes);
		
		Tuple t1 = new Tuple();
		t1.setHdr(numColumns, type, strSizes);
		
		t.setIntFld(1, emp1.eid);
		t.setStrFld(2, emp1.ename);
		t1.setIntFld(1, emp2.eid);
		t1.setStrFld(2, emp2.ename);
		
		TID[] tids = new TID[2];
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[1] = cf.insertTuple(t1.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		
		
		
		System.out.println("Starting data insert");
		
		Tuple[] returnTuples = new Tuple[2];
		returnTuples[0] = cf.getTuple(tids[0]);
		returnTuples[1] = cf.getTuple(tids[1]);
		
		returnTuples[0].print(type);
		returnTuples[1].print(type);
		System.out.println("Finished printi-ng data");
		

	    CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].next  = null;
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrString);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
	    expr[0].operand2.string = "arjun";
	    expr[1] = null;
	    
	    cf.createBTreeIndex(2);
	    ColumnIndexScan dummy = new ColumnIndexScan();
	    dummy.checkIndex(cf, 2, expr);
	    
	    dummy.deleteTuple(cf, 2, expr);
	
	    cf.purgeAllDeletedTuples();
	    
	    cf.createBTreeIndex(2);
	    dummy.checkIndex(cf, 2, null);
	    
	    System.out.println("Finished Deleting Data");
	}
	
	
	static void initDB()
	{
		 String dbpath = "C:\\tmp\\" + System.getProperty("user.name") + ".minibase.jointestdb";
		 SystemDefs sysdef = new SystemDefs( dbpath, 1000, 50, "Clock" );
	}
}
