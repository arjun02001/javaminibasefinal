package columnar;

import index.IndexException;
import index.IndexScan;
import index.ColumnIndexScan;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.FileScanByColnPos;
import iterator.FldSpec;
import iterator.PredEval;
import iterator.Projection;
import iterator.RelSpec;
import iterator.UnknownKeyTypeException;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskmgr.*;
import bufmgr.*;
import bitmap.BitMapFile;
import bitmap.Entry;
import btree.*;
import global.*;
import heap.*;

public class ColumnarFile implements GlobalConst
{
	public Heapfile[] columnFiles;
	public String[] heapFileNames;
	public BTreeFile[] btreeIndexFiles; 
    
	
	//according to specification
	public static int numColumns;
	public AttrType[] type;
	public short[] attrSize;
	public String name;
	
	public ColumnarFile(String name, int numColumns, AttrType[] type) 
	throws HFException, HFBufMgrException, HFDiskMgrException, IOException
	{
		this.numColumns = numColumns;
		this.type = type;
		this.name = name;
		attrSize = new short[numColumns];
		for(int i = 0; i < numColumns; i++)
		{	
			if (type[i].attrType == AttrType.attrInteger)
				attrSize[i] = 4; 
			else attrSize[i] = STRINGSIZE;
		}	
	
		columnFiles = new Heapfile[numColumns];
		btreeIndexFiles = new BTreeFile[numColumns];
		heapFileNames = new String[numColumns];

		
		for(int i = 0; i < numColumns; i++)
		{
			btreeIndexFiles[i] = null;
			heapFileNames[i] = name + "." + i;
			columnFiles[i] = new Heapfile(heapFileNames[i]);
		}
		
		//TODO: create headerfile
	}
	
	
	public void deleteColumnarFile()
	throws HFException, HFBufMgrException, HFDiskMgrException, IOException, 
	InvalidTupleSizeException, InvalidSlotNumberException, FileAlreadyDeletedException
	{
		for(int i = 0; i < this.numColumns; i++)
		{
			columnFiles[i].deleteFile();
		}
		
		//TODO: delete headerfile
	}
	
	public TID insertTuple(byte[] tuplePtr) throws Exception 
	{
		Tuple tempTuple = null;
		AttrType[] tempTypes =  new AttrType[1];
		short[] tempStringSize = new short[1];
		tempStringSize[0] = STRINGSIZE;
		
		int offset = 0;
		short dataOffset = 0;
		int numColumnsInTuple = Convert.getShortValue(offset, tuplePtr);
		if(numColumnsInTuple != numColumns)
		{
			throw new Exception("Number of columns do not match. Number of columns should be: " + numColumnsInTuple);
		}
		
		offset += 2;
		
		RID[] rids = new RID[numColumns];
		
		for(int i = 0; i < numColumns; i++)
		{
			dataOffset = Convert.getShortValue(offset, tuplePtr);
			if(type[i].attrType == AttrType.attrInteger)
			{
				tempTypes[0] = new AttrType(AttrType.attrInteger);
				int intValue = Convert.getIntValue(dataOffset, tuplePtr);
				tempTuple = new Tuple();
				/*setHdr is not considering attribute size from the parameter*/
				tempTuple.setHdr((short)1, tempTypes, tempStringSize);
				tempTuple.setIntFld(1, intValue);
			}
			else if(type[i].attrType == AttrType.attrString)
			{
				tempTypes[0] = new AttrType(AttrType.attrString);
				String stringValue = Convert.getStrValue(dataOffset, tuplePtr, STRINGSIZE + 2); //doubt
				tempTuple = new Tuple();
				tempTuple.setHdr((short)1, tempTypes, tempStringSize);
				tempTuple.setStrFld(1, stringValue);
			}
			offset += 2;
			rids[i] = columnFiles[i].insertRecord(tempTuple.getTupleByteArray());
		}
		TID tid = new TID(numColumns, 0, rids); //doubt . need to send the correct position
		return tid;
	}
		
	public Tuple getTuple(TID tid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		if(tid==null) return null;
		RID[] ridArray = new RID[numColumns];
		Tuple[] tupleArray = new Tuple[numColumns];
		ridArray = tid.getRecordIDs();
		Tuple finalTuple = new Tuple();
		int numOfStrings = 0;
		for(int i = 0 ; i < numColumns ; i++){
			if (type[i].attrType == AttrType.attrString){
				++numOfStrings;
			}	
		}
		
		short[] strSizes = new short[numOfStrings];
		Arrays.fill(strSizes, (short)STRINGSIZE);	
		finalTuple.setHdr((short)numColumns, type, strSizes);
		for(int i=0;i<numColumns;i++)
		{	//System.out.println("ColumnFile name : "+columnFiles[i]);
			tupleArray[i]=columnFiles[i].getRecord(ridArray[i]);
			if(type[i].attrType== AttrType.attrInteger)
			{
				AttrType[] newType = new AttrType[1];
				newType[0] = new AttrType(AttrType.attrInteger);
				tupleArray[i].setHdr((short)1, newType, strSizes);
				
				int intValue = tupleArray[i].getIntFld(1);
				finalTuple.setIntFld(i+1, intValue);			
			}
			else if(type[i].attrType==AttrType.attrString)
			{
				AttrType[] newType = new AttrType[1];
				newType[0] = new AttrType(AttrType.attrString);
				tupleArray[i].setHdr((short)1, newType, strSizes);
				
				String stringValue = tupleArray[i].getStrFld(1);
				finalTuple.setStrFld(i+1, stringValue);
			}
			
		}
		return finalTuple;
		
	}
	
	public ValueClass getValue(TID tid, int column) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		RID[] ridArray = new RID[numColumns];
		ridArray = tid.getRecordIDs();
		Tuple t = columnFiles[column-1].getRecord(ridArray[column-1]);
		Tuple tt=new Tuple();
		tt.modifyTuple(t);
		ValueClass valueItem = null;
		if(type[column-1].attrType == AttrType.attrInteger)
		{
			int value = t.getIntFld(1);
			valueItem = new IntValueClass(value);
		}
		else if(type[column-1].attrType == AttrType.attrString)
		{
			String value = t.getStrFld(1);
			valueItem = new StringValueClass(value);
			
		}
		return valueItem;
		
	}
	
	public int getTupleCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, IOException{
		return columnFiles[0].getRecCnt();
	}
	
	public TupleScan openTupleScan() throws InvalidTupleSizeException, IOException
	{
		TupleScan tS = new TupleScan(this);
		return tS;
	}
	
	public Scan openColumnScan(int columnNo) throws InvalidTupleSizeException, IOException
	{
		Scan colScan = new Scan(columnFiles[columnNo-1]);
		return colScan;	
	}
	
	public boolean updateTuple(TID tid, Tuple newTuple) throws InvalidSlotNumberException, InvalidUpdateException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{	
		RID[] rids = tid.getRecordIDs();
		short[] strSizes = new short[1];
		strSizes[0] = STRINGSIZE;
		Tuple t = null;
		
		
		for(int i=0; i < numColumns; i++)
		{
			AttrType[] attrTypes = new AttrType[1];
			t = new Tuple();
			
			if(type[i].attrType == AttrType.attrInteger)
			{
				attrTypes[0] = new AttrType(AttrType.attrInteger);
				
				t.setHdr((short)1, attrTypes, strSizes);
				
				int intVal = newTuple.getIntFld(i+1);
				t.setIntFld(1, intVal);
				
			}
			else if(type[i].attrType == AttrType.attrString)
			{
				
				attrTypes[0] = new AttrType(AttrType.attrString);
				t.setHdr((short)1, attrTypes, strSizes);
				
				String strVal = newTuple.getStrFld(i+1);
				strVal = newTuple.getStrFld(i+1);
				t.setStrFld(1, strVal);
			}
			
			if(!columnFiles[i].updateRecord(rids[i], t))
			{
				return false;
			}
	
		}
		
		tid.setRecordIDs(rids); 		//doubt about position
				
		return true;
	}
	
	public boolean updateColumnOfTuple(TID tid, Tuple newTuple, int column) throws InvalidSlotNumberException, InvalidUpdateException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		RID[] recIDs = tid.getRecordIDs();
		RID rid = recIDs[column - 1];          
		return( columnFiles[column-1 ].updateRecord(rid, newTuple));
	}
	
	public boolean createBTreeIndex(int columnNo) throws InvalidTupleSizeException, IOException, GetFileEntryException, ConstructPageException, AddFileEntryException, FieldNumberOutOfBoundException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, InvalidTypeException{

		Scan colScan = new Scan(columnFiles[columnNo-1]);
	    // create the index file
	    btreeIndexFiles[columnNo-1] = new BTreeFile(this.name + "_Btree." + (columnNo-1), type[columnNo-1].attrType, attrSize[columnNo-1], DeleteFashion.FULL_DELETE/*delete*/); 
	    
		AttrType[] tempTypes =  new AttrType[1];
		short[] tempStringSize = new short[1];
		tempStringSize[0] = STRINGSIZE;
	
		RID   rid;
	    rid = new RID();
	    String strKey = null;
	    Integer intKey = null;
	    
	    Tuple temp = new Tuple();
	    Tuple t = null;
		if(type[columnNo-1].attrType == AttrType.attrInteger)
		{
			tempTypes[0] = new AttrType(AttrType.attrInteger);
			temp.setHdr((short)1, tempTypes, tempStringSize);
			t = colScan.getNext(rid);
	
			while ( t != null) {
			    temp.tupleCopy(t);
		    	intKey = temp.getIntFld(1);
		    	//System.out.println(intKey + "\n");
		    	btreeIndexFiles[columnNo-1].insert(new IntegerKey(intKey), rid); 
		        t = colScan.getNext(rid);
		     }
		}     
		else if(type[columnNo-1].attrType == AttrType.attrString)
		{
			tempTypes[0] = new AttrType(AttrType.attrString);
			temp.setHdr((short)1, tempTypes, tempStringSize);
			t = colScan.getNext(rid);
	
			while ( t != null) {
			    temp.tupleCopy(t);
		    	strKey = temp.getStrFld(1);
		    	//System.out.println(strKey + "\n");
		    	btreeIndexFiles[columnNo-1].insert(new StringKey(strKey), rid); 
		        t = colScan.getNext(rid);
		    }
		}    
	    // close the file scan
	    colScan.closescan();
	    return true;
	}
	
	public void createEnvironment(int columnNo) throws InvalidTupleSizeException, GetFileEntryException, ConstructPageException, AddFileEntryException, FieldNumberOutOfBoundException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, InvalidTypeException, IOException
	{
		this.createBTreeIndex(columnNo);
	}
	public void setClock(int columnNo) throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException
	{
		this.destroyBTreeIndex(columnNo);
	}

	public boolean destroyBTreeIndex(Integer columnNo) throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException
	{
		if (btreeIndexFiles[columnNo-1] != null)
		{
			btreeIndexFiles[columnNo-1].close();
		}
		return true;
	}
	
	public boolean createBitMapIndex(int columnNo, ValueClass value) throws Exception{
		Scanner sc_ColsName=new Scanner(new File(DIRPATH + name + "_schema.txt"));
		String colName=null;
		while(sc_ColsName.hasNextLine()){
			String line=sc_ColsName.nextLine();
			line=line.trim();
			String[] splt=line.split("\\s");
			splt[1]=splt[1].trim();
			if(splt[1].equals(new String(new Integer(columnNo).toString()))){
				colName=splt[0].trim();
				break;
			}
		}
		sc_ColsName.close();
		Scan colsScn=this.openColumnScan(columnNo);
		RID rid=new RID();
		int bitIndex=0;
		BitSet bset=new BitSet();
		Tuple tuple=new Tuple();
		int numRecords=0;
		if(type[columnNo-1].attrType==1){ 
			int dis_value=Integer.parseInt(value.toString());
			while((tuple=colsScn.getNext(rid))!=null){
				AttrType[] ty=new AttrType[1];
				ty[0]=type[columnNo-1];
				Tuple tup=new Tuple();
				tup.modifyTuple(tuple);
				//tup.print(ty);
				int cmp=tup.getIntFld(1);
				numRecords++;
				if(cmp==dis_value){
					bset.set(bitIndex++);
				}
				else{
					bset.set(bitIndex++,false);
				}
			}
			colsScn.closescan();
			byte[] bArr=convertToByteArray(bset);
			String btName=name+colName+new String(value.toString().trim());
			BitMapFile bmFile = new BitMapFile(btName, this, columnNo, value);
			bmFile.insert(bArr, numRecords);
			bmFile.close();
			bmFile = new BitMapFile(btName, this, columnNo, value);
			byte[] byteArr = bmFile.readBitMapFile();
			bmFile.close();
			for(int i=0; i < byteArr.length; i++){
				printByte(byteArr[i],btName);
			}
			btName=null;
		}
		else{
			String dis_value=value.toString().trim();
			while((tuple=colsScn.getNext(rid))!=null){
				AttrType[] ty=new AttrType[1];
				ty[0]=type[columnNo-1];
				Tuple tup=new Tuple();
				tup.modifyTuple(tuple);
				//tup.print(ty);
				String cmp=tup.getStrFld(1);
				numRecords++;
				if(cmp.equalsIgnoreCase(dis_value)){
					//System.out.println("cmp==dis_value "+ cmp+"   "+dis_value);
					bset.set(bitIndex++);
				}
				else{
					//System.out.println("cmp!=dis_value "+cmp+"   "+dis_value);
					bset.set(bitIndex++,false);
				}
			}
			colsScn.closescan();
			byte[] bArr=convertToByteArray(bset);
			String btName=name+colName+value.toString().trim();
			BitMapFile bmFile = new BitMapFile(btName, this, columnNo, value);
			bmFile.insert(bArr, numRecords);
			bmFile.close();
			bmFile = new BitMapFile(btName, this, columnNo, value);
			byte[] byteArr = bmFile.readBitMapFile();
			bmFile.close();
			for(int i=0; i < byteArr.length; i++){
				printByte(byteArr[i],btName);
			}
			btName=null;
		}
		String file_name=name+"tuple_cnter.txt";
		File file=new File(file_name);
		if(!file.exists()){
			file.createNewFile();
			//Entry.Logging.addLog(file_name);
		}
		Scanner sc1=new Scanner(file);
		int cnt=0;
		while(sc1.hasNextLine()){
			String line=sc1.nextLine();
			line=line.trim();
			Pattern pattern=Pattern.compile(name+"tuple_count:[\\d]+");
			Matcher matcher=pattern.matcher(line);
			while(matcher.find()){
				String k=matcher.group();
				k=k.trim();
				String[] ar=k.split(":");
				cnt=Integer.parseInt(ar[1]);
			}
		}
		sc1.close();
		if(cnt-1==numRecords){
			return true;
		}
		return false;
	}
	
	public  static void printByte(byte b,String fname) throws IOException{
		String fnm=fname+".txt";
		File f=new File(fnm);
		if(!f.exists()){
			f.createNewFile();
			//Entry.Logging.addLog(fnm);
		}
		PrintWriter pw=new PrintWriter(new FileWriter(fnm,true));
		for(int j=7; j>=0; j--){
			pw.println((b>>j & 1));
			pw.flush();
		}
		pw.close();
	}
	public byte[] convertToByteArray(BitSet bArr) {

		int byteArrSize = (bArr.size()) / 8;
		byte[] byteArr = new byte[byteArrSize];
		byte b = 0;
		int j = 0;
		for (int i = 0; i < bArr.size(); i++) {
			if (bArr.get(i)) {
				j = i % 8;
				b |= (1 << (8-1-j));
			}
			if ((i + 1) % 8 == 0) {
				byteArr[i / 8] = b;
				b = 0;
			}
		}
		return byteArr;
	}
	
	public BitSet fromByteArray(byte[] bytes) {
		BitSet bits = new BitSet();

		byte b = 0;
		int index = 0;
		for (int i = 0; i < bytes.length; i++) {
			b = bytes[i];
			for (int j = 7; j >=0; j--) {
				if (((b >> j) & 1) == 1) {
					bits.set(index++, true);
				} else {
					bits.set(index++, false);
				}
			}
		}
		return bits;
	}
	
	// Getting distict values for the target column
public boolean distinctValues(int column) throws Exception{
		
		Scanner sc_ColsName=new Scanner(new File(DIRPATH + name + "_schema.txt"));
		String colName=null;
		int victimColumnNumber=column;
		int check=0;
		while(sc_ColsName.hasNextLine()){
			String line=sc_ColsName.nextLine();
			line=line.trim();
			String[] splt=line.split("\\s");
			splt[1]=splt[1].trim();
			if(splt[1].equalsIgnoreCase(new String(new Integer(column).toString()))){
				colName=splt[0].trim();
				break;
			}
		}
		sc_ColsName.close();
		String f=name+colName.toLowerCase()+".txt";
		File file=new File(f);
		if(!file.exists()){
			file.createNewFile();
			//Entry.Logging.addLog(f);
		}
		PrintWriter pw=new PrintWriter(file);
		Tuple tpl=new Tuple();
		//TID tid=new TID(numColumns);
		int keyType=type[column-1].attrType;
		//TupleScan sc=openTupleScan();
		AttrType[] ty=new AttrType[1];
		//System.out.println("Column number is : "+column);
		//Scan colSc=openColumnScan(column);
		//FileScanByColnPos fscp = new FileScanByColnPos(name, type, strSizes, (short)type.length, (short)Sprojection.length, Sprojection, expr2, victimColumnNumber);
		RID rid=new RID();
    	Scan sc=this.openColumnScan(column);
    	Tuple t = new Tuple();
		if(keyType==1){
			Set<Integer> set=new HashSet<Integer>();
			ty[0]=new AttrType(1);
			//while((tpl=sc.getNext(tid))!=null){
			while((tpl=sc.getNext(rid))!=null){
				//tpl.print(types);
				//RID[] rids=tid.getRIDs();
				Tuple tup=new Tuple();
				tup.modifyTuple(tpl);
				//tup.print(ty);
				
				Integer key=new Integer(tpl.getIntFld(1));
				if(!set.contains(key)){
					set.add(key);
					pw.println(key);
					pw.flush();
					
				}
				//btFile.insert(key, rids[column-1]);
				check++;
			}
			sc.closescan();
			pw.close();
	    	//t = cf.getTuple(tid);
		}
		else{
			Set<String> set=new HashSet<String>();
			ty[0]=new AttrType(0);
			//while((tpl=sc.getNext(tid))!=null){
			//System.out.println("out of loop");
			while((tpl=sc.getNext(rid))!=null){
				//tpl.print(types);
				//RID[] rids=tid.getRIDs();
				Tuple tup=new Tuple();
				tup.modifyTuple(tpl);
				//System.out.println("inside loop find");
				String key=tpl.getStrFld(1);
				if(!set.contains(key)){
					set.add(key);
					pw.println(key);
					pw.flush();
					
				}
				//btFile.insert(key, rids[column-1]);
				check++;
			}
			sc.closescan();
			pw.close();
		}
		Scanner sc1=new Scanner(new File(name+"tuple_cnter.txt"));
		int cnt=0;
		while(sc1.hasNextLine()){
			String line=sc1.nextLine();
			line=line.trim();
			Pattern pattern=Pattern.compile(name+"tuple_count:[\\d]+");
			Matcher matcher=pattern.matcher(line);
			while(matcher.find()){
				String k=matcher.group();
				k=k.trim();
				String[] ar=k.split(":");
				cnt=Integer.parseInt(ar[1]);
			}
		}
		sc1.close();
		if(check==cnt-1){
			//System.out.println("\n\n\t\tDone With Distinct values for Column Name : "+colName+" in file : "+f);
			return true;
		}
		return false;
	}

	

	public boolean purgeAllDeletedTuples() throws InvalidTypeException, InvalidTupleSizeException, IOException, InvalidSlotNumberException, SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, FileAlreadyDeletedException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException{

		int i=0;
		Scan[] colScan;
		colScan = new Scan[numColumns];
		
		Heapfile[] tempHeapFiles;
		tempHeapFiles = new Heapfile[numColumns];
		
		for (i=0; i<numColumns; i++)
		{
			heapFileNames[i] = heapFileNames[i]+".x";
			tempHeapFiles[i] = new Heapfile(heapFileNames[i]);
			colScan[i] = new Scan(columnFiles[i]);
		}
		
		AttrType[] tempTypes =  new AttrType[1];
		short[] tempStringSize = new short[1];
		tempStringSize[0] = STRINGSIZE;
	
		RID   rid;
	    rid = new RID();
	    String strKey = null;
	    Integer intKey = null;
	    
	    Tuple temp = new Tuple();
	    Tuple t = null;
		
		for (i=0; i<numColumns; i++)
		{
			if(type[i].attrType == AttrType.attrInteger)
			{
				tempTypes[0] = new AttrType(AttrType.attrInteger);
				temp.setHdr((short)1, tempTypes, tempStringSize);
				
				t = colScan[i].getNext(rid);
				while ( t != null) {
					tempHeapFiles[i].insertRecord(t.getTupleByteArray());
					t = colScan[i].getNext(rid);
			     }
			}     
			else if(type[i].attrType == AttrType.attrString)
			{
				tempTypes[0] = new AttrType(AttrType.attrString);
				temp.setHdr((short)1, tempTypes, tempStringSize);
		
				t = colScan[i].getNext(rid);
				while ( t != null) {
					tempHeapFiles[i].insertRecord(t.getTupleByteArray());
					t = colScan[i].getNext(rid);
			     }
			}    
		}
		// close the file scan
		for (i=0; i<numColumns; i++)
		{
			//columnFiles[i].deleteFile();
			columnFiles[i] = tempHeapFiles[i];
			colScan[i].closescan();
		}
	 	return true;
	}

	public boolean purgeDeletedTuples() throws HFException, HFDiskMgrException, Exception
	{
		/*Deleting marked delete tuples*/
		Mark delete = new Mark();
		TID tid = new TID(numColumns);
		PositionData newPositionData = new PositionData();
	    RID[] rids = new RID[numColumns];
	    for (int i=0; i<numColumns; i++)
	    	rids[i] = new RID();
	    
		for (int position = 0; position < this.columnFiles[0].getRecCnt();position++)
		{
			if(delete.isDeleted(this.name, position))
			{
				newPositionData.position = position;
				tid = tid.constructTIDfromPosition(newPositionData, this.name, this.numColumns);
			    rids = tid.getRecordIDs();
				for (int column = 0; column < numColumns; column++)
				{
					if(rids[column] != null)
					this.columnFiles[column].deleteRecord(rids[column]);
				}
			}
		}
	    	
		
		/*Purge starts*/
		
		ArrayList<RID>[] allRID = new ArrayList[numColumns];
		Tuple[] tempTuple = new Tuple[numColumns];
		Tuple t = new Tuple();
		byte[] arr = new byte[STRINGSIZE];
		for (int i=0; i<numColumns; i++)
		{
			allRID[i] = new ArrayList<RID>();
			allRID[i] = this.columnFiles[i].getallRID();
			tempTuple[i] = new Tuple();
		}
		boolean end = false; 
		
		for (int i=0; i<allRID[0].size() && !end ; i++)
		{
			for (int j=0; j<numColumns;j++)
			{
				tempTuple[j] = this.columnFiles[j].getRecord(allRID[j].get(i));
				if (allRID[j].get(i) != null)
				this.columnFiles[j].deleteRecord(allRID[j].get(i));
				arr = tempTuple[j].getTupleByteArray();
				this.columnFiles[j].insertRecord(arr);
			}
		}
		
		return true;
	}
	
	public boolean purgeDelete() throws HFException, HFDiskMgrException, Exception
	{
		/*Deleting marked delete tuples*/
		Mark delete = new Mark();
		TID tid = new TID(numColumns);
		PositionData newPositionData = new PositionData();
	    RID[] rids = new RID[numColumns];
	    for (int i=0; i<numColumns; i++)
	    	rids[i] = new RID();
	  
	    int count =0, recCount = this.columnFiles[0].getRecCnt(); boolean flag = false;
		System.out.println("Record Count Before: "+this.columnFiles[0].getRecCnt());
		for (int position = this.columnFiles[0].getRecCnt(); position > 10 ;position--)
		{
			if(delete.isDeleted(this.name, position))
			{
				System.out.println("position :" + position);
				newPositionData.position = position;
				tid = tid.constructTIDfromPosition(newPositionData, this.name, this.numColumns);
			    rids = tid.getRecordIDs();
			    for (int column = 0; column < numColumns; column++)
				{
			    	if(rids[column] != null)
					{	
						flag = true;
			    		System.out.println(rids[column].pageNo.pid + " " + rids[column].slotNo);
						this.columnFiles[column].deleteRecord(rids[column]);
					}
						}
			    if(flag)
			    {
			    	count++;
			    	flag = false;
			    }
			}
		}
		
	    for (int column = 0; column < numColumns; column++)
		{
	    	this.columnFiles[column].compact();
		}
	    
	    System.out.println("Total Number of records deleted: " + count);
		System.out.println("Latest Record Count: " + this.columnFiles[0].getRecCnt());
			
	/*	for (int position = this.columnFiles[0].getRecCnt(); position;position--)
		{
			if(delete.isDeleted(this.name, position))
			{
				newPositionData.position = position;
				tid = tid.constructTIDfromPosition(newPositionData, this.name, this.numColumns);
			    rids = tid.getRecordIDs();
				for (int column = 0; column < numColumns; column++)
				{
					if(rids[column] != null)
					this.columnFiles[column].deleteRecord(rids[column]);
				}
			}
		}
		
	*/  
	    System.out.println("purge completed");
		File Deletefile = new File(DIRPATH + this.name + "_delete.txt");
		RandomAccessFile raF = new RandomAccessFile(Deletefile, "rw");
		raF.seek(0);
		for (int i=0; i < 2*(recCount+1); i++)
			raF.write(48);
		raF.close();

		return true;
	}
	
	public boolean markTupleDeleted(TID tid, ColumnarFile cf) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, Exception{

		RID[] rids = tid.getRecordIDs();
		for(int i=0; i < numColumns; i++)
		{
			if (rids[i] != null)
			{
/*				Tuple t = new Tuple();
				t =	cf.getTuple(tid);
				t.print(cf.type);
*/				
				if(!columnFiles[i].deleteRecord(rids[i]))
				{
					return false;
				}
			}
		}
		return true;
	}
	
	public TID getTIDFromPos(int pos) throws IOException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException{
		RID[] rids = new RID[type.length];
		for(int i = 0; i<type.length; i++){
			rids[i] = columnFiles[i].getRIDFromPosition(pos);
		}
		TID tid = new TID(type.length);
		tid.setRecordIDs(rids);
		return tid;
	}
}


/********************** Bhaskar's extra code 

public boolean scanBTreeIndex(ColumnarFile cf, int columnNo, CondExpr selects[]) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
{
    FldSpec[] projlist = new FldSpec[1];
    RelSpec rel = new RelSpec(RelSpec.outer); 
    projlist[0] = new FldSpec(rel, 1);
    
    AttrType[] tempTypes =  new AttrType[1];
    tempTypes[0] = new AttrType(this.type[columnNo-1].attrType);
   
    short[] strSizes = new short[1];
    strSizes[0] = attrSize[columnNo-1];

    
    // start index scan
    ColumnIndexScan iscan = null;
    //IndexScan iscan = null;
	    //iscan = new IndexScan(new IndexType(IndexType.B_Index), this.name + "." + (columnNo-1), this.name + "_Btree." + (columnNo-1), tempTypes, strSizes, 1, 1, projlist, selects, 1, true);
    iscan = new ColumnIndexScan(new IndexType(IndexType.B_Index), cf.name + "." + (columnNo-1), cf.name + "_Btree." + (columnNo-1), tempTypes, strSizes, 1, 1, projlist, selects, 1, true);
    
    Tuple t = new Tuple();
	RID rid = new RID();
    String outStrVal = null;
    Integer outIntVal = null;
    Integer position = null;
    PositionData newPositionData = new PositionData();
	
    rid = iscan.get_next_rid();
    
    while (rid != null) 
    {
 //   	t = columnFiles[columnNo-1].getRecord(rid);
 //   	
 //   	if (tempTypes[0].attrType == AttrType.attrInteger)
//	    	outIntVal = t.getIntFld(1);
 //   	else
 //   		outStrVal = t.getStrFld(1);
  //  	System.out.println(outIntVal + " " + outStrVal + "\n"); 
    	
  //  	System.out.println(rid.pageNo.pid + " " + rid.slotNo + "\n"); 
    	
    	position = columnFiles[columnNo-1].getPositionForRID(rid);
    	newPositionData.position = position;
    	TID tid = new TID(numColumns);
	    tid = tid.constructTIDfromPosition(newPositionData, name, numColumns);
	    t = this.getTuple(tid);
		t.print(type);

		rid = iscan.get_next_rid();
	    
    }

    iscan.close();
    System.out.println("BTreeIndex Scan Completed.\n"); 
    
    return true;
	
}

*********************/