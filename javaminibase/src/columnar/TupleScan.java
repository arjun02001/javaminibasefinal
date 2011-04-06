package columnar;

import java.io.IOException;
import java.util.Arrays;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TID;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;


public class TupleScan implements GlobalConst {

	private ColumnarFile _columnarfile;
	private Scan[] scans;
	private int numColumns;
	private AttrType[] type;
	//int position = 0;
	
	public TupleScan(ColumnarFile colfile) throws InvalidTupleSizeException, IOException
	{
		
		_columnarfile = colfile;
		numColumns = colfile.numColumns;
		type = colfile.type;
		Heapfile[] columnFiles = colfile.columnFiles;
		int length =  columnFiles.length;

		scans = new Scan[length];
		for(int i=0 ; i < length ; i++ )
		{
			scans[i] = new Scan(columnFiles[i]);
		}
	}
	
	public void closeTupleScan()
	{
		for(int i=0; i< scans.length; i++)
		{
			scans[i].closescan();
		}
	}
	
	public Tuple getNext(TID tid) throws InvalidTypeException, InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException
	{

		Tuple FinalTuple = new Tuple();
		Tuple scannedTuple = new Tuple();
		RID[] rids = new RID[numColumns];
		int noOfStrings = 0;

		for(int i=0;i < numColumns; i ++){
			rids[i] = new RID();
		}

		for(int i=0; i < type.length ; i++)
		{
			if (type[i].attrType == AttrType.attrString)
				++noOfStrings;
		}

		short[] strSizes = new short[noOfStrings];
		Arrays.fill(strSizes,(short)STRINGSIZE);
		
		

		FinalTuple.setHdr((short)numColumns, type, strSizes);
		AttrType newtype[] = new AttrType[1];

		for (int i = 0 ; i < numColumns ; i++) 
		{

			scannedTuple = scans[i].getNext(rids[i]);
			if(scannedTuple!= null)
			{	
				if(type[i].attrType == AttrType.attrInteger)
				{
					newtype[0] = new AttrType(AttrType.attrInteger);
					scannedTuple.setHdr((short)1,newtype,strSizes);
					int intVal = scannedTuple.getIntFld(1);
					FinalTuple.setIntFld(i+1, intVal);

				}else if(type[i].attrType == AttrType.attrString)
				{
					newtype[0] = new AttrType(AttrType.attrString);
					scannedTuple.setHdr((short)1,newtype,strSizes);
					String strVal = scannedTuple.getStrFld(1);
					FinalTuple.setStrFld(i+1, strVal);
				}
			}
			else
				return null;
		}

		tid.setNumRIDs(numColumns);
		//tid.setPosition(position);
		tid.setRecordIDs(rids);
		
		return FinalTuple;
	}
	
	public boolean position(TID tid) throws InvalidTupleSizeException, IOException
	{
		for(int i=0; i < tid.getNumRIDs(); i++ )
		{
			if(!scans[i].position(tid.getRecordIDs()[i]))
			{
				return false;
				//TODO failure condition
			}				
		}
		return true;	
	}
}
