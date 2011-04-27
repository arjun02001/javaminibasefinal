package iterator;

import index.IndexException;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

public class ColumnarSort extends Iterator implements GlobalConst {

   public Sort cS;
   
	ColumnarSort(AttrType[] in,	short len_in,short[] str_sizes,	String ColumnarFileName,
				int sort_fld,	TupleOrder sort_order,	int sort_fld_len,	int n_pages) throws SortException, IOException, FileScanException, TupleUtilsException, InvalidRelation, HFException, HFBufMgrException, HFDiskMgrException
	{
		FldSpec [] Sprojection = new FldSpec[len_in];	//create the projection array
		for(int i = 0; i < len_in; i++)
		{
			Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), (i + 1));
		}
		ColumnarFileScan cfS = new ColumnarFileScan(ColumnarFileName, in, str_sizes, len_in, Sprojection.length
				, Sprojection, null);
		cS = new Sort(in, len_in, str_sizes, cfS, sort_fld, sort_order, sort_fld_len, n_pages);
	}


	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception 
	{
		Tuple t = new Tuple();
		t = null;
		t = cS.get_next();
		return t;
	
	}

	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {
	 cS.close();
		
	}
}