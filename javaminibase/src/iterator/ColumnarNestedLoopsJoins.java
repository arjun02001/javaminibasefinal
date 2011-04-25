package iterator;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;

import java.lang.*;
import java.io.*;

import columnar.*;

public class ColumnarNestedLoopsJoins extends Iterator
{
	  private   AttrType      _in1[],  _in2[];
	  private   int        in1_len, in2_len;
	  private   Iterator  outer;
	  private   short t2_str_sizescopy[];
	  private   CondExpr OutputFilter[];
	  private   CondExpr RightFilter[];
	  private   int        n_buf_pgs;        // # of buffer pages available.
	  private   boolean        done,         // Is the join complete
	    get_from_outer;                 // if TRUE, a tuple is got from outer
	  private   Tuple     outer_tuple, inner_tuple;
	  private   Tuple     Jtuple;           // Joined tuple
	  private   FldSpec   perm_mat[];
	  private   int        nOutFlds;
	  //private   Heapfile  hf;
	  private ColumnarFile cf;
	  //private   Scan      inner;
	  private TupleScan inner;
	
	public ColumnarNestedLoopsJoins(AttrType[] in1, int len_in1, short[] t1_str_sizes
			, AttrType[] in2, int len_in2, short[] t2_str_sizes, int amt_of_mem, Iterator am1
			, String columnarFileName, CondExpr[] outFilter, CondExpr[] rightFilter
			, FldSpec[] proj_list, int n_out_flds) throws IOException, NestedLoopException
	{
		  _in1 = new AttrType[in1.length];
	      _in2 = new AttrType[in2.length];
	      System.arraycopy(in1,0,_in1,0,in1.length);
	      System.arraycopy(in2,0,_in2,0,in2.length);
	      in1_len = len_in1;
	      in2_len = len_in2;
	      
	      
	      outer = am1;
	      t2_str_sizescopy =  t2_str_sizes;
	      inner_tuple = new Tuple();
	      Jtuple = new Tuple();
	      OutputFilter = outFilter;
	      RightFilter  = rightFilter;
	      
	      n_buf_pgs    = amt_of_mem;
	      inner = null;
	      done  = false;
	      get_from_outer = true;
	      
	      AttrType[] Jtypes = new AttrType[n_out_flds];
	      short[]    t_size;
	      
	      perm_mat = proj_list;
	      nOutFlds = n_out_flds;
	      try {
		t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
						   in1, len_in1, in2, len_in2,
						   t1_str_sizes, t2_str_sizes,
						   proj_list, nOutFlds);
	      }catch (TupleUtilsException e){
		throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
	      }
	      
	      
	      
	      try {
		  //hf = new Heapfile(relationName);
	    	  cf = new ColumnarFile(columnarFileName, in2_len, _in2);
		  
	      }
	      catch(Exception e) {
		throw new NestedLoopException(e, "Create new columnarfile failed.");
	      }

	}

	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {
		if (!closeFlag) {
			
			try {
			  outer.close();
			}catch (Exception e) {
			  throw new JoinsException(e, "ColumnarNestedLoopsJoin.java: error in closing iterator.");
			}
			closeFlag = true;
		      }
		
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {

	      
	      if (done)
	    	  return null;
	      
	      do
	      {
		  // If get_from_outer is true, Get a tuple from the outer, delete
		  // an existing scan on the file, and reopen a new scan on the file.
		  // If a get_next on the outer returns DONE?, then the nested loops
		  //join is done too.
		  
		  if (get_from_outer == true)
		  {
		      get_from_outer = false;
		      if (inner != null)     // If this not the first time,
		      {
		    	  // close scan
		    	  inner = null;
		      }
		    
		      try 
		      {
		    	  inner = cf.openTupleScan();
		    	  //cf.openTupleScan();
		      }
		      catch(Exception e)
		      {
		    	  throw new NestedLoopException(e, "openTupleScan failed");
		      }
		      
		      if ((outer_tuple = outer.get_next()) == null)
		      {
		    	  done = true;
		    	  if (inner != null) 
		    	  {
		    		  inner = null;
		    	  }
			  
		    	  return null;
		      }   
		  }  // ENDS: if (get_from_outer == TRUE)
		 
		  
		  // The next step is to get a tuple from the inner,
		  // while the inner is not completely scanned && there
		  // is no match (with pred),get a tuple from the inner.
		  
		 
		      //RID rid = new RID();
		  		TID tid = new TID(cf.numColumns);
		      while ((inner_tuple = inner.getNext(tid)) != null)
		      {
		    	  inner_tuple.setHdr((short)in2_len, _in2,t2_str_sizescopy);
		    	  if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true)
		    	  {
		    		  if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) == true)
		    		  {
		    			  // Apply a projection on the outer and inner tuples.
		    			  Projection.Join(outer_tuple, _in1, 
		    					  inner_tuple, _in2, 
		    					  Jtuple, perm_mat, nOutFlds);
		    			  return Jtuple;
		    		  }
		    	  }
		      }
		      
		      // There has been no match. (otherwise, we would have 
		      //returned from t//he while loop. Hence, inner is 
		      //exhausted, => set get_from_outer = TRUE, go to top of loop
		      
		      get_from_outer = true; // Loop back to top and get next outer tuple.	      
		} while (true);
	}
	
}
