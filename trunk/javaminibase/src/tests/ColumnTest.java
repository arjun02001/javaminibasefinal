package tests;

import iterator.*;
import global.*;
import heap.*;
import columnar.*;

class Emp
{
	int eid;
	String ename;
	public Emp(int eid, String ename)
	{
		this.eid = eid;
		this.ename = ename;
	}
}

public class ColumnTest implements GlobalConst
{
	public static void main(String[] args) throws Exception
	{
		initDB();
		Emp emp1, emp2, emp3;
		emp1 = new Emp(1, "arjun");
		emp2 = new Emp(2, "anwasha");
		short numColumns = 2;
		 
		AttrType[] type = new AttrType[numColumns];
		type[0] = new AttrType(AttrType.attrInteger);
		type[1] = new AttrType(AttrType.attrString);
		
		ColumnarFile cf = new ColumnarFile("myfile", numColumns, type);
		
		short[] strSizes = new short[1];
		strSizes[0] = STRINGSIZE;
		
		Tuple t = new Tuple();
		t.setHdr(numColumns, type, strSizes);
		
		t.setIntFld(1, emp1.eid);
		t.setStrFld(2, emp1.ename);
		
		TID[] tids = new TID[2];
		tids[0] = cf.insertTuple(t.returnTupleByteArray());
		
		t.setIntFld(1, emp2.eid);
		t.setStrFld(2, emp2.ename);
		
		tids[1] = cf.insertTuple(t.returnTupleByteArray());
		System.out.println("Starting data insert");
		Tuple[] returnTuples = new Tuple[2];
		returnTuples[0] = cf.getTuple(tids[0]);
		returnTuples[1] = cf.getTuple(tids[1]);
		
		returnTuples[0].print(type);
		returnTuples[1].print(type);
		System.out.println("Finished printing data");
		
		
		CondExpr[] expr1 = new CondExpr[2];
		expr1[0] = new CondExpr();
		expr1[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr1[0].next  = null;
	    expr1[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr1[0].type2 = new AttrType(AttrType.attrString);
	    expr1[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
	    expr1[0].operand2.string = "arjun";
	    expr1[1] = null;
	    
	    FldSpec [] Sprojection = new FldSpec[2];
	    Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    
	    ColumnarFileScan cs = new ColumnarFileScan("myfile", type, strSizes ,(short) type.length, (short)Sprojection.length,
	    		Sprojection, expr1);
	    Tuple newT = new Tuple();
	    newT = null;
	    newT= cs.get_next();
	    System.out.println("Printing record where ename = arjun");
	    newT.print(type);
	    
	}
	
	static void initDB()
	{
		 String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
		 SystemDefs sysdef = new SystemDefs( dbpath, 1000, 50, "Clock" );
	}
}
