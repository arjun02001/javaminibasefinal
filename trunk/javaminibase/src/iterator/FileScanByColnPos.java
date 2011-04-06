package iterator;

import heap.*;
import index.IndexException;
import global.*;
import bufmgr.*;
import diskmgr.*;
import columnar.*;
import java.lang.*;
import java.io.*;

public class FileScanByColnPos extends Iterator {
	FileScan fs;
	//ColumnarFile cf;
	Heapfile hf;
	String columnarFileName;
	int columnNo;

	public FileScanByColnPos(String  file_name,
		    AttrType in1[],                
		    short s1_sizes[], 
		    short     len_in1,              
		    int n_out_flds,
		    FldSpec[] proj_list,
		    CondExpr[]  outFilter,
		    int victimColumnNumber) throws FileScanException, TupleUtilsException, InvalidRelation, IOException, HFException, HFBufMgrException, HFDiskMgrException
	{
		columnarFileName = file_name;
		
		columnNo =victimColumnNumber;
		
		AttrType[] type_in1 = new AttrType[1];
		type_in1[0]=in1[columnNo-1];
		
		short[] strSizes_s1_sizes=new short[1];
		strSizes_s1_sizes[0] = s1_sizes[0];
		
		short lengthofType = (short)type_in1.length;
				
		FldSpec[] fieldSpec_proj_list = new FldSpec[1];
		fieldSpec_proj_list[0]=proj_list[0];
		int projectionLength = fieldSpec_proj_list.length;
		
		fs= new FileScan(file_name+"."+(columnNo-1), type_in1, strSizes_s1_sizes, lengthofType, projectionLength, fieldSpec_proj_list, outFilter);
				
	}

	 
	public PositionData get_next_PositionData() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception 
	{
		int Position = 0;
		PositionData returnPD = new PositionData();
		
		returnPD = fs.get_next_PositionData();// I have both tuple from the conditional column and the RID
		hf = new Heapfile(columnarFileName+"."+(columnNo-1));//I have to open the Heap file corresponding to the conditional column and get the position for RID(returnPD)
		if(returnPD != null)
		{
			Position = hf.getPositionForRID(returnPD.rid);//open the other heapfiles for the columnar files and get scan the files to get the tuple at the Position.
			returnPD.position=Position;
		}
		else 
			Position = 0;
				
		if(Position != 0)
			return returnPD;
		else
			return null;
		
		
	}

	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {
		fs.close();
		
	}


	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
