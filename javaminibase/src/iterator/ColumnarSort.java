package iterator;

import index.IndexException;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

public class ColumnarSort extends Sort implements GlobalConst 
{
	public	ColumnarSort(AttrType[] in,	short len_in,short[] str_sizes,	String ColumnarFileName,
	int sort_fld,	TupleOrder sort_order,	int sort_fld_len,	int n_pages, ColumnarFileScan cfS) throws SortException, IOException, FileScanException, TupleUtilsException, InvalidRelation, HFException, HFBufMgrException, HFDiskMgrException, JoinsException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat
	{
		super(in, len_in, str_sizes, cfS, sort_fld, sort_order, sort_fld_len, n_pages);
	}
}