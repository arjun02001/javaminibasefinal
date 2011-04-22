package deliverables;

import global.AttrType;
import global.GlobalConst;
import global.IntValueClass;
import global.Mark;
import global.RID;
import global.StringValueClass;
import global.SystemDefs;
import global.TID;
import global.ValueClass;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import columnar.ColumnarFile;
import diskmgr.PCounter;

public class RunQueryOnBitMap implements GlobalConst{
	public static void queryOnBitMap(String[] args) throws IOException{
		String columnarDBName=args[0].trim();					//mydb
		String columnarFileName=args[1].trim();					//columnarfile
		String targetColumns=args[2].trim();					// "A B C D"
		String valueConstraint=args[3].trim();					// "A = arjun"
		String[] const_split=valueConstraint.split("\\s");
		String columnName=const_split[0];
		String columnOpt=const_split[1];
		int pk=0;
		PCounter.initialize();
		if(columnOpt.equalsIgnoreCase(">=")||columnOpt.equalsIgnoreCase(">")||columnOpt.equalsIgnoreCase("<=")||columnOpt.equalsIgnoreCase("<")||columnOpt.equalsIgnoreCase("!=")||columnOpt.equalsIgnoreCase("=")){
			pk=2;
		}
		else{
			pk=2;
			String check=const_split[pk++];
			while(!check.equals(",")){
				columnOpt=columnOpt+" "+check;
				check=const_split[pk++];
			}
		}
		columnOpt=columnOpt.trim();
		String columnValue="";
		for(int i=0;i<const_split.length-pk;i++){
			columnValue+=const_split[i+pk];
			columnValue+=" ";
		}
		columnValue=columnValue.trim();
		int numBuf=Integer.parseInt(args[4].trim());
		String accessType=args[5].trim();
		initDB(columnarDBName,numBuf);
		Scanner s1 = new Scanner(new File(DIRPATH + columnarFileName + "_schema.txt"));
		int numColumns = 0;
		while(s1.hasNextLine())			//count the no. of lines in schema file
		{
			s1.nextLine();
			numColumns++;
		}
		s1.close();
		s1 = new Scanner(new File(DIRPATH + columnarFileName + "_schema.txt"));
		AttrType[] type = new AttrType[numColumns];
		int j = 0;
		int strCount = 0;
		while(s1.hasNextLine())					//construct the type[]
		{
			String dataType = s1.nextLine().split("\\s")[2].toLowerCase();
			if(dataType.equals("int"))
			{
				type[j++] = new AttrType(AttrType.attrInteger);
			}
			if(dataType.equals("char"))
			{
				type[j++] = new AttrType(AttrType.attrString);
				strCount++;
			}
		}
		s1.close();
		try {
			ColumnarFile cf=new ColumnarFile(columnarFileName,numColumns,type);
			String btName=columnarFileName+columnName.toLowerCase()+columnValue.trim();
			
			Scanner sc_ColsName=new Scanner(new File(DIRPATH + columnarFileName + "_schema.txt"));
			int column=0;
			while(sc_ColsName.hasNextLine()){
				String line=sc_ColsName.nextLine();
				line=line.trim();
				String[] splt=line.split("\\s");
				if(splt[0].equalsIgnoreCase(columnName)){
					column=Integer.parseInt(splt[1]);
					break;
				}
			}
			sc_ColsName.close();
			int keyType=type[column-1].attrType;
			ValueClass key=null;
			if(keyType==1){
				key=new IntValueClass(Integer.parseInt(columnValue.trim()));
				
			}
			else{
				key=new StringValueClass(columnValue.trim());
				
			}
			BitSet bSet=new BitSet();
			Scanner sc=new Scanner(new File(btName+".txt"));
			int index=0;
			while(sc.hasNextLine()){
				String k=sc.nextLine();
				k=k.trim();
				if(k.equalsIgnoreCase("1")){
					bSet.set(index++);
				}
				else{
					bSet.set(index++,false);
				}
			}
			sc.close();
			int pos=0;
			String file_name=columnarFileName+"tuple_cnter.txt";
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
				Pattern pattern=Pattern.compile(columnarFileName+"tuple_count:[\\d]+");
				Matcher matcher=pattern.matcher(line);
				while(matcher.find()){
					String k=matcher.group();
					k=k.trim();
					String[] ar=k.split(":");
					cnt=Integer.parseInt(ar[1]);
				}
			}
			sc1.close();
			String[] colNames=targetColumns.split("\\s");
			sc_ColsName=new Scanner(new File(DIRPATH + columnarFileName + "_schema.txt"));
			String[] colNamesArray=new String[numColumns];
			boolean[] posSet=new boolean[numColumns];
			index=0;
			while(sc_ColsName.hasNextLine()){
				String line=sc_ColsName.nextLine();
				line=line.trim();
				String[] splt=line.split("\\s");
				colNamesArray[index++]=splt[0].toUpperCase();
			}
			sc_ColsName.close();
			Set<String> colsNamesUnique=new HashSet<String>();
			for(int i=0;i<colNames.length;i++){
				colsNamesUnique.add(colNames[i]);
			}
			for(int i=0;i<posSet.length;i++){
				if(colsNamesUnique.contains(colNamesArray[i]))
					posSet[i]=true;
			}
			System.out.println("Retrieved Tuples' Header : ->  ");
			for(int i=0;i<colNamesArray.length;i++){
				if(posSet[i]){
					System.out.print(colNamesArray[i]+"  ");
				}
			}
			System.out.println("\n\n");
			RID[] rids=null;
			pos=bSet.nextSetBit(pos);
			Mark delete = new Mark();
		    boolean flag = false;
			int count = 0, temp_pos = pos;
			while(pos<=cnt-1&&pos>=0){
				pos++;
				TID tid = cf.getTIDFromPos(pos);
				temp_pos = pos;
				pos=bSet.nextSetBit(pos);
				rids=tid.getRecordIDs();
				for(int i=0;i<rids.length;i++){
					if(posSet[i]){
						if(!delete.isDeleted(columnarFileName, temp_pos))
						{
							System.out.print(cf.getValue(tid, i+1)+" ");
							flag = true;
						}
					}
				}System.out.println();
				if (flag)
				{
					count++;
					flag = false;
				}
			}
			
			System.out.println("Total tuples: " + count);
			System.out.println("Bit map query completed.");
			System.out.println("Read count: " + PCounter.rcounter);
			System.out.println("Write count: " + PCounter.wcounter);
			
			
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	static void initDB(String dbname, int numBuf)
	{
		 String dbpath = DIRPATH + System.getProperty("user.name") + ".minibase."+dbname;
		 SystemDefs sysdef = new SystemDefs( dbpath, 100000, numBuf, "Clock" );
	}
}