package global;

import java.io.IOException;

import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.PositionData;

public class TID extends java.lang.Object {

	//field summary
	int numRIDs;
	int position;
	RID[] recordIDs;
	
	//constructor
	public TID(int numRIDs){
		this.numRIDs = numRIDs;
	}
	
	//constructor
	public TID(int numRIDs, int position){
		this.numRIDs = numRIDs;
		this.position = position;
	}
	
	//constructor
	public TID(int numRIDs, int position, RID[] recordIDs){
		this.numRIDs = numRIDs;
		this.position = position;
		this.recordIDs = recordIDs;
	}
	
	public void copyTID(TID tid){
		this.numRIDs = tid.numRIDs;
		this.position = tid.position;
		this.recordIDs = tid.recordIDs;
	}
	
	public boolean equals(TID tid){
		if(this.numRIDs == tid.numRIDs && this.position == tid.position && this.recordIDs == tid.recordIDs){
			return true;
		}
		else{
			return false;
		}
	}
	
	public void writeToByteArray(byte[] array, int offset) throws java.io.IOException
	{	
		Convert.setIntValue(this.numRIDs, offset, array);
		Convert.setIntValue(this.position, offset + 4, array);
		for(int i = 0; i < recordIDs.length; i++)
		{
			RID rid = recordIDs[i];
			rid.writeToByteArray(array, offset + 8 * (i +1));
		}
	}
	
	public int getNumRIDs()
	{
		return this.numRIDs;
	}
	
	public void setNumRIDs(int numRIDs)
	{
		this.numRIDs = numRIDs;
	}
	
	public int getPosition()
	{
		return this.position;
	}
	
	public void setPosition(int position)
	{
		this.position = position;
	}
	
	public RID[] getRecordIDs()
	{
		return this.recordIDs;
	}
	
	public void setRecordIDs(RID[] recordIDs)
	{
		this.recordIDs = recordIDs;
	}
	
	public void setRID(int column, RID recordID)
	{
		//doubt
		/*if(recordIDs[column] == null)
		{
			recordIDs[column] = new RID();
		}*/
		recordIDs[column] = recordID;
	}
	
	public TID constructTIDfromPosition(PositionData positionData, String fileName, int numColumns) throws HFException, HFBufMgrException, HFDiskMgrException, IOException, InvalidSlotNumberException, InvalidTupleSizeException
	{
		RID[] rids = new RID[numColumns];
		for(int i = 0; i < numColumns; i++)
		{
			Heapfile hf =  new Heapfile(fileName + "." + i);
			RID rid = new RID(); 
			rid =	hf.getRIDFromPosition(positionData.position);
			rids[i] = rid;
		}
		TID tid = new TID(numColumns, positionData.position, rids);
		return tid;
	}
}
