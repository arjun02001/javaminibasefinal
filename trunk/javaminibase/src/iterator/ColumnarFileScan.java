package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import columnar.*;


import java.lang.*;
import java.io.*;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public class ColumnarFileScan extends  Iterator
{
  private AttrType[] _in1;
  private short in1_len;
  private short[] s_sizes; 
  private ColumnarFile f;
  private TupleScan scan;
  private Tuple     tuple1;
  private Tuple    Jtuple;
  private int        t1_size;
  private int nOutFlds;
  private CondExpr[]  OutputFilter;
  public FldSpec[] perm_mat;
  public FileScan[] columnarFileScans;

  public int position_counter;

  /**
   *constructor
   *@param file_name heapfile to be opened
   *@param in1[]  array showing what the attributes of the input fields are. 
   *@param s1_sizes[]  shows the length of the string fields.
   *@param len_in1  number of attributes in the input tuple
   *@param n_out_flds  number of fields in the out tuple
   *@param proj_list  shows what input fields go where in the output tuple
   *@param outFilter  select expressions
   *@exception IOException some I/O fault
   *@exception ColumnarFileScanException exception from this class
   *@exception TupleUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
 * @throws HFDiskMgrException 
 * @throws HFBufMgrException 
 * @throws HFException 
   */
  public  ColumnarFileScan (String  file_name,
		    AttrType in1[],                
		    short s1_sizes[], 
		    short     len_in1,              
		    int n_out_flds,
		    FldSpec[] proj_list,
		    CondExpr[]  outFilter        		    
		    )
    throws IOException,
	   FileScanException,
	   TupleUtilsException, 
	   InvalidRelation, HFException, HFBufMgrException, HFDiskMgrException
    {
      _in1 = in1; 
      in1_len = len_in1;
      s_sizes = s1_sizes;
      position_counter = 0;
      Jtuple =  new Tuple();
      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    ts_size;
      ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);
      
      OutputFilter = outFilter;
      perm_mat = proj_list;
      nOutFlds = n_out_flds; 
      tuple1 =  new Tuple();

      try {
	tuple1.setHdr(in1_len, _in1, s1_sizes);
      }catch (Exception e){
	throw new FileScanException(e, "setHdr() failed");
      }
      t1_size = tuple1.size();
      
      try {
	f = new ColumnarFile(file_name,(short)in1_len,_in1);
	
      }
      catch(Exception e) {
	throw new FileScanException(e, "Create new heapfile failed");
      }
      
      try {
	scan = f.openTupleScan();
      }
      catch(Exception e){
	throw new FileScanException(e, "openScan() failed");
      }
	  
	/*  f= new ColumnarFile(file_name, len_in1,in1);
	  AttrType[] type = new AttrType[1];
	  short[] strSize = new short[1];
	  FldSpec[] pList = new FldSpec[1];
	  CondExpr[]  filter= new CondExpr[1];  
	  for(int i=0,j=0;i<len_in1;i++)
	  {
		  if(in1[i].attrType==AttrType.attrString)
		  {
			  strSize[0]=s1_sizes[j];
			  j++;
		  }
		  type[0] = in1[i];
		  pList[0]=proj_list[i];
		  filter[0]= outFilter[i];
		  columnarFileScans[i] = new FileScan(f+"."+i,type,strSize,1,n_out_flds,pList,);
	  }*/
	  
    }
  
  /**
   *@return shows what input fields go where in the output tuple
   */
  public FldSpec[] show()
    {
      return perm_mat;
    }
  
  /**
   *@return the result tuple
   *@exception JoinsException some join exception
   *@exception IOException I/O errors
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception PredEvalException exception from PredEval class
   *@exception UnknowAttrType attribute type unknown
   *@exception FieldNumberOutOfBoundException array out of bounds
   *@exception WrongPermat exception for wrong FldSpec argument
   */
  public Tuple get_next()
    throws JoinsException,
	   IOException,
	   InvalidTupleSizeException,
	   InvalidTypeException,
	   PageNotReadException, 
	   PredEvalException,
	   UnknowAttrType,
	   FieldNumberOutOfBoundException,
	   WrongPermat
    {     
      //RID rid = new RID();;
      TID tid = new TID(in1_len);
      
      while(true) {
	if((tuple1 =  scan.getNext(tid)) == null) {
		return null;
	}
	else
		position_counter++;
	
	
	tuple1.setHdr(in1_len, _in1, s_sizes);
	if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true){
	  Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds); 
	  return  Jtuple;
	}        
      }
    }

  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */
  public void close() 
    {
     
      if (!closeFlag) {
	scan.closeTupleScan();
	closeFlag = true;
      } 
    }
  
}