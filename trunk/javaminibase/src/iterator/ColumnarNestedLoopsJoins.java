package iterator;

import global.GlobalConst;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import columnar.*;

public class ColumnarNestedLoopsJoins implements GlobalConst 
{
	public ColumnarNestedLoopsJoins(AttrType[] in1, int len_in1, short[] t1_str_sizes
			, AttrType[] in2, int len_in2, short[] t2_str_sizes, int amt_of_mem, Iterator am1
			, String columnarFileName, CondExpr[] outFilter, CondExpr[] rightFilter
			, FldSpec[] proj_list, int n_out_flds)
	{
		
	}
	
}
