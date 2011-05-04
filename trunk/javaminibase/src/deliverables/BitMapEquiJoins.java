package deliverables;

import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import bitmap.BitMapFile;

import columnar.ColumnarFile;
import global.IntValueClass;
import global.StringValueClass;
import global.ValueClass;

import diskmgr.PCounter;

public class BitMapEquiJoins implements GlobalConst{
	public static void main(String[] args) throws Exception{
		PCounter.initialize();		
		BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Enter DB Name : ");
		String columnarDBName=br.readLine().trim();
		
		//String dbpath = "/tmp/"+System.getProperty("user.name")+columnarDBName;
		String dbpath = DIRPATH + System.getProperty("user.name") + ".minibase."+columnarDBName;
		FileWriter bw=new FileWriter(new File("databaseSet.txt"),true);
		PrintWriter pw2=new PrintWriter(bw);
		Scanner sc2=new Scanner(new File("databaseSet.txt"));
		
		int count=0;
		boolean flag=false;
		while(sc2.hasNextLine()){
			String ln=sc2.nextLine();
			ln=ln.trim();
			count++;
			if(ln.equalsIgnoreCase(dbpath)) {
				flag=true;
				break;
			}
		}
		if(!sc2.hasNextLine()&& !flag){
			SystemDefs sysdef = new SystemDefs( dbpath, 1000000, 50, "Clock" );
			pw2.println(dbpath);
		}
		SystemDefs sysdef = new SystemDefs( dbpath, 1000000, 50, "Clock" );
		pw2.close();
		sc2.close();
		System.out.println("Enter Outer Columnar File Name : ");
		String columnarFileName1=br.readLine().trim();
		System.out.println("Enter Inner Columnar File Name : ");
		String columnarFileName2=br.readLine().trim();
		
		System.out.println("Enter Joining ColumnsNumber of Outer Relation : ");
		String join1=br.readLine().trim();
		System.out.println("Enter Joining ColumnsNumber of Inner Relation : ");
		String join2=br.readLine().trim();
		
		String lyn1 = null;
		int count1 = 0; 
		Scanner sc=new Scanner(new File(DIRPATH + columnarFileName1 + "_schema.txt"));
		while(sc.hasNextLine()){
			sc.nextLine();
			count1++;
		}
		String[] ty=new String[count1];
		String colName1 = null;
		int i1 = 0;
		String retTuple = new String();
		sc.close();
		sc=new Scanner(new File(DIRPATH + columnarFileName1 + "_schema.txt"));
		while(sc.hasNextLine()){
			lyn1 = sc.nextLine();
			String[] ar = lyn1.split("\\s");
			ty[i1++] = ar[2];
			retTuple += ar[0]+"\t";
			if(ar[1].equals(join1.trim())){
				colName1 = ar[0].trim();
			}
			//count1++;
		}
		int numColumns1=ty.length;
		AttrType[] types1=new AttrType[numColumns1];
		for(int i=0;i<numColumns1;i++){
			ty[i]=ty[i].trim();
			if(ty[i].equalsIgnoreCase("int")){
				types1[i]=new AttrType(1);
			}
			else{
				types1[i]=new AttrType(0);
			}
		}
		sc.close();
		
		
		lyn1 = null;
		count1 = 0; 
		sc=new Scanner(new File(DIRPATH + columnarFileName1 + "_schema.txt"));
		while(sc.hasNextLine()){
			sc.nextLine();
			count1++;
		}
		ty=new String[count1];
		i1 = 0;
		String[] ar = null;
		String colName2 = null;
		sc.close();
		sc=new Scanner(new File(DIRPATH + columnarFileName1 + "_schema.txt"));
		while(sc.hasNextLine()){
			lyn1 = sc.nextLine();
			ar = lyn1.split("\\s");
			ty[i1++] = ar[2];
			retTuple += ar[0]+"\t";
			if(ar[1].equals(join2.trim())){
				colName2 = ar[0].trim();
			}
			//count1++;
			//count1++;
		}
		int numColumns2=ty.length;
		AttrType[] types2=new AttrType[numColumns2];
		for(int i=0;i<numColumns2;i++){
			ty[i]=ty[i].trim();
			if(ty[i].equalsIgnoreCase("int")){
				types2[i]=new AttrType(1);
			}
			else{
				types2[i]=new AttrType(0);
			}
		}
		sc.close();
		ColumnarFile cf1 = new ColumnarFile(columnarFileName1, numColumns1,types1);
		int col1=Integer.parseInt(join1.trim());
		ColumnarFile cf2 = new ColumnarFile(columnarFileName2, numColumns2,types2);
		int col2=Integer.parseInt(join2.trim());
//		Scanner sc_ColsName=new Scanner(new File(DIRPATH + columnarFileName1+"_ColsName.txt"));
//		String colName1=null;
//		String retTuple="";
//		while(sc_ColsName.hasNextLine()){
//			String line=sc_ColsName.nextLine();
//			line=line.trim();
//			String[] splt=line.split("\\s");
//			splt[1]=splt[1].trim();
//			retTuple+=splt[0]+"\t";
//			if(splt[1].equals(new String(new Integer(col1).toString()))){
//				colName1=splt[0].trim();
//			}
//		}
//		sc_ColsName.close();
//		sc_ColsName=new Scanner(new File(columnarFileName2+"_ColsName.txt"));
//		String colName2=null;
//		while(sc_ColsName.hasNextLine()){
//			String line=sc_ColsName.nextLine();
//			line=line.trim();
//			String[] splt=line.split("\\s");
//			splt[1]=splt[1].trim();
//			retTuple+=splt[0]+"\t";
//			if(splt[1].equals(new String(new Integer(col1).toString()))){
//				colName2=splt[0].trim();
//			}
//		}
//		sc_ColsName.close();
		String f1=columnarFileName1+colName1+".txt";
		File file1=new File(f1);
		Scanner sc1=new Scanner(file1);
		String f2=columnarFileName2+colName2+".txt";
		File file2=new File(f2);
		sc2=new Scanner(file2);
		Set<String> h=new HashSet<String>();
		while(sc1.hasNext()){
			h.add(sc1.nextLine().trim());
		}
		sc1.close();
		List<String> comm=new ArrayList<String>();
		while(sc2.hasNext()){
			String ch=sc2.nextLine().trim();
			if(h.contains(ch)){
				comm.add(ch);
			}
		}
		sc2.close();
		System.out.println("Retrieved Tuple's Header is : \n\n"+retTuple+"\n\n");
		int total=0;
		for(int i=0;i<comm.size();i++){
			String val=comm.get(i);
			String f_tmp="Output1_tmp.txt";
			File file_tmp1=new File(f_tmp); 
			if(!file_tmp1.exists()){
				file_tmp1.createNewFile();
				//Entry.Logging.addLog(f_tmp);
			}
			else{
				file_tmp1.delete();
				file_tmp1.createNewFile();
			}
			int r1=find(cf1,columnarFileName1,colName1,col1,val,types1,numColumns1,f_tmp);
			
			f_tmp="Output2_tmp.txt";
			File file_tmp2=new File(f_tmp); 
			if(!file_tmp2.exists()){
				file_tmp2.createNewFile();
				//Entry.Logging.addLog(f_tmp);
			}
			else{
				file_tmp2.delete();
				file_tmp2.createNewFile();
			}
			int r2=find(cf2,columnarFileName2,colName2,col2,val,types2,numColumns2,f_tmp);
			total+=r1*r2;
			Scanner sc1_tmp=new Scanner(file_tmp1);
			Scanner sc2_tmp=new Scanner(file_tmp2);
			while(sc1_tmp.hasNextLine()){
				String tuple="[ "+sc1_tmp.nextLine().trim()+", ";
				while(sc2_tmp.hasNextLine()){
					tuple+=sc2_tmp.nextLine().trim()+" ]";
					System.out.println(tuple);
				}
			}
			
		}
		System.out.println("\n\t\tTotal # of Retrieved Tuples are : "+total);
		System.out.println("\n\t\t# of Disk Pages that were read   : \t\t"+PCounter.rcounter);
		System.out.println("\n\t\t# of Disk Pages that were write  : \t\t"+PCounter.wcounter);
		System.out.println("********************************* BitmapEquiJoin DONE *************************************");
		
	}
	public static int find(ColumnarFile cf,String columnarFileName,String colName,int col,String columnValue,AttrType[] types,int numColumns,String fl) throws Exception{
		String btName=columnarFileName+colName+columnValue.trim();
		
		int column=col;
		int keyType=types[column-1].attrType;
		ValueClass key=null;
		if(keyType==1){
			key=new IntValueClass(Integer.parseInt(columnValue.trim()));
			
		}
		else{
			key=new StringValueClass(columnValue.trim());
			
		}
		BitMapFile bmFile1=new BitMapFile(btName,cf,column,key);
		byte[] data=bmFile1.readBitMapFile();
		bmFile1.close();
		BitSet bSet=cf.fromByteArray(data);
		
		String file_name=columnarFileName+"tuple_cnt.txt";
		File file=new File(file_name);
		if(!file.exists()){
			file.createNewFile();
			//Entry.Logging.addLog(file_name);
		}
		Scanner sc=new Scanner(file);
		int cnt=0;
		while(sc.hasNextLine()){
			String line=sc.nextLine();
			line=line.trim();
			Pattern pattern=Pattern.compile(columnarFileName+"tuple_count:[\\d]+");
			Matcher matcher=pattern.matcher(line);
			while(matcher.find()){
				String k=matcher.group();
				k=k.trim();
				String[] ar=k.split(":");
				cnt=Integer.parseInt(ar[1]);
			}
		}
		sc.close();
		String targetColumns="all";
		String[] colNames=targetColumns.split("\\s");
		sc=new Scanner(new File(columnarFileName+"_ColsName.txt"));
		String[] colNamesArray=new String[numColumns];
		boolean[] posSet=new boolean[numColumns];
		int index=0;
		while(sc.hasNextLine()){
			String line=sc.nextLine();
			line=line.trim();
			String[] splt=line.split("\\s");
			colNamesArray[index++]=splt[0];
		}
		sc.close();
		if(colNames[0].equalsIgnoreCase("all")){
			colNames=colNamesArray;
		}
		Set<String> colsNamesUnique=new HashSet<String>();
		for(int j=0;j<colNames.length;j++){
			colsNamesUnique.add(colNames[j]);
		}
		for(int j=0;j<posSet.length;j++){
			if(colsNamesUnique.contains(colNamesArray[j]))
				posSet[j]=true;
		}
		String ps1=fl;
		return bitset2Tuple(bSet, cf, cnt, posSet,false,11111,ps1,types);
		
	}
	public static int bitset2Tuple(BitSet bSet,ColumnarFile cf,int cnt,boolean[] posSet,boolean already,int done,String fl,AttrType[] types) throws HFBufMgrException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, FieldNumberOutOfBoundException {
		int ret=0;
		int len=types.length;
		if(already&&done<=len){
			posSet[done-1]=false;
		}
		PageId[] currentDirPageId=new PageId[len];
		HFPage[] currentDirPage=new HFPage[len];
		PageId[] nextDirPageId=new PageId[len];
		RID[] currentDirPageRid =new RID[len];
		Tuple[] atuple = new Tuple[len];
		Heapfile[] heapfiles=new Heapfile[len];
		
		for(int i=0;i<len;i++){
			if(posSet[i]){
				heapfiles[i]=cf.columnFiles[i];
				currentDirPageId[i]=new PageId(heapfiles[i]._firstDirPageId.pid);
				currentDirPage[i]=new HFPage();
				nextDirPageId[i]=new PageId();
				currentDirPageRid[i]=new RID();
				atuple[i]=new Tuple();
			}
		}
			BitSet b=bSet;
			int count_num=0;
			for(int i=0;i<len;i++){
				PrintWriter pw=null;
				
				if(posSet[i]){
					count_num=0;
					boolean overflag=false;
					bSet=b;
					String fname="Output"+i+".txt";
					File f=new File(fname);
					if(!f.exists()){
						//Entry.Logging.addLog(fname);
					}
					pw=new PrintWriter(f);
					int position=0;
					int pos=0;
					int count=0;
					pos=bSet.nextSetBit(pos);
					position=pos;
					if(position<0 && position+1>cnt){
						continue;
					}
					count=position+1;
					int left=0;
					boolean samePage=false;
					while(currentDirPageId[i].pid != INVALID_PAGE){
						heapfiles[i].pinPage(currentDirPageId[i], currentDirPage[i], false);
						 for( currentDirPageRid[i] = currentDirPage[i].firstRecord();
				  	       currentDirPageRid[i] != null;
				  	       currentDirPageRid[i] = currentDirPage[i].nextRecord(currentDirPageRid[i]))
						 {
					  			atuple[i] = currentDirPage[i].getRecord(currentDirPageRid[i]);
					  		    DataPageInfo dpinfo = new DataPageInfo(atuple[i]);
					  
					  		      if(position >= dpinfo.recct)
					  		      { 	  
					  		    	  position -= dpinfo.recct;
					  		    		 
					  		      }
					  		      else
					  		      {
					  		    	HFPage currentDataPage=new HFPage();
					  		    	heapfiles[i].pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/); 
					  		    	
					  		    	do{
						  		    	RID rid =new RID();
						  		    	rid.pageNo.pid=dpinfo.pageId.pid;
						  		    	rid.slotNo=position;
						  		    	int just_before=position;
						  		    	left=dpinfo.recct-just_before-1;
						  		    	Tuple t=currentDataPage.getRecord(rid);
						  		    	Tuple tt=new Tuple(t);
						  		    	count_num++;
						  		    	if(types[i].attrType==1){
						  		    		int data=t.getIntFld(1);
						  		    		pw.println(data);
						  		    		pw.flush();
						  		    	}
						  		    	else{
						  		    		String data=t.getStrFld(1);
						  		    		pw.println(data);
						  		    		pw.flush();
						  		    		data=null;
						  		    	}
						  		    	pos=bSet.nextSetBit(++pos);
						  		    	position=pos;
						  		    	if(position<0||position+1>cnt){
						  		    		overflag=true;
											break;
										}
						  		    	int k=position-count+1;
						  		    	if(k<=left){
						  		    		samePage=true;
						  		    		position=just_before+k;
						  		    	}
						  		    	else{
						  		    		samePage=false;
						  		    		position=k-1-left;
						  		    	}
						  		    	count=count+k;
					  		    	}while(samePage);
					  		    	heapfiles[i].unpinPage(dpinfo.pageId, false);
					  		      }
					  		    if(overflag){
									break;
								}
					  		 
				  	    }// Ending for loop
						if(overflag){
							heapfiles[i].unpinPage(currentDirPageId[i], false /*undirty*/);
							break;
						}
					  	  //since I did not break yet, I dint find the dataPage in currentDirectoryPage
				  	  nextDirPageId[i] = currentDirPage[i].getNextPage();
					  heapfiles[i].unpinPage(currentDirPageId[i], false /*undirty*/);
					  currentDirPageId[i].pid = nextDirPageId[i].pid;
				}
				pw.close();
				//System.out.println("Num of Records Found "+count_num);
				ret=count_num;
				//System.out.println("************************** Done **********************************");
			}	// If posSet ends here
				
		}
		if(already){
			posSet[done-1]=true;
		}
		Scanner[] sc=new Scanner[len];
		for(int i=0;i<len;i++){
			if(posSet[i]){
				String fname="Output"+i+".txt";
				sc[i]=new Scanner(new File(fname));
			}
		}
		int count=1;
		File ff=new File(fl);
		PrintWriter pw=new PrintWriter(ff);
		String feed="";
		while(count<=count_num){
			for(int i=0;i<len;i++){
				if(posSet[i]){
					if(sc[i].hasNext()){
						if(i<len-1){
							feed+=sc[i].next()+", ";
						}
						else{
							feed+=sc[i].next();
						}
					}
				}
			}
			pw.println(feed.trim());
			pw.flush();
			count++;
		}
		pw.close();
		for(int i=0;i<len;i++){
			if(posSet[i]){
				sc[i].close();
			}
		}
		return ret;
	}
}
