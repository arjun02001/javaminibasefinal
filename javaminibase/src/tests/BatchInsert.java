package tests;

import iterator.*;
import global.*;
import heap.*;
import columnar.*;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskmgr.PCounter;

public class BatchInsert implements GlobalConst
{
	static String dataFileName, columnDBName, columnarFileName;
	static int numColumns;
	
	public static void main(String[] args) throws Exception
	{
		int numTuplesInserted = 0;
		int numStrings = 0;
		if(args.length != 4)
		{
			throw new Exception("Please enter all 4 arguments");
		}
		dataFileName = args[0];
		columnDBName = args[1];
		columnarFileName = args[2];
		numColumns = Integer.parseInt(args[3].trim());
		
		initDB(columnDBName);
		
		Scanner s = new Scanner(new FileInputStream(dataFileName));
		String[] parameterType = s.nextLine().split("\\s");				//use \\s
		
		AttrType[] type = new AttrType[numColumns];
		String[] colName = new String[numColumns];
		

		File file = new File(DIRPATH + columnarFileName + "_schema.txt");
		PrintWriter pw = new PrintWriter(file);
		
		for(int i = 0; i < parameterType.length; i++)
		{
			colName[i] = parameterType[i].split(":")[0].toLowerCase();
			pw.print(colName[i] + "\t" + (i + 1) + "\t");
			if(parameterType[i].split(":")[1].toLowerCase().equals("int"))
			{
				type[i] = new AttrType(AttrType.attrInteger);
				pw.print("int\n");
			}
			if(parameterType[i].split(":")[1].toLowerCase().contains("char"))
			{
				type[i] = new AttrType(AttrType.attrString);
				pw.print("char\n");
				++numStrings;
			}
		}
		pw.close();
		
		ColumnarFile cf = new ColumnarFile(columnarFileName, numColumns, type);
		
		short[] strSizes = new short[numStrings];
		Arrays.fill(strSizes, (short)STRINGSIZE);
		
		TID tid = null;
		String[] value = new String[numColumns];
		
		Tuple t = new Tuple();
		t.setHdr((short)numColumns, type, strSizes);
		
		int cnt = 1;
		while(s.hasNextLine())
		{
			value = s.nextLine().split("\\s");
			
			for(int i = 0; i < numColumns; i++)
			{
				if(type[i].attrType == AttrType.attrInteger)
				{
					t.setIntFld(i + 1, Integer.parseInt(value[i]));
				}
				if(type[i].attrType == AttrType.attrString)
				{
					t.setStrFld(i + 1, value[i]);
				}
			}
			tid = cf.insertTuple(t.returnTupleByteArray());
			
			
			RID[] rd=tid.getRecordIDs();
			File cnt_file=new File(columnarFileName + "tuple_cnter.txt");
			if(!cnt_file.exists()){
				cnt_file.createNewFile();
			}
			Scanner sc_Tcount=new Scanner(cnt_file);
			
			while(sc_Tcount.hasNextLine())
			{
				String line=sc_Tcount.nextLine();
				line=line.trim();
				Pattern pattern=Pattern.compile(columnarFileName + "tuple_count:[\\d]+");
				Matcher matcher=pattern.matcher(line);
				while(matcher.find()){
					String k=matcher.group();
					k=k.trim();
					String[] ar=k.split(":");
					cnt=Integer.parseInt(ar[1]);
				}
			}
			sc_Tcount.close();
			PrintWriter pw_cnt=new PrintWriter(cnt_file);
		
			
			File Deletefile = new File(DIRPATH + columnarFileName + "_delete.txt");
			RandomAccessFile raF = new RandomAccessFile(Deletefile, "rw");
			raF.seek(0);
			for (int i=0; i < 2*(numTuplesInserted+1); i++)
				raF.write(48);
			raF.close();


/*			
			File fr=new File(columnarFileName);
			if(!fr.exists()){
				fr.createNewFile();
			}
			FileWriter fw_t=new FileWriter(fr,true);
			PrintWriter pw_RID=new PrintWriter(fw_t);
			PrintWriter pw_cnt=new PrintWriter(cnt_file);
		
			for(int l1=0;l1<numColumns;l1++){
				pw_RID.print("[ "+rd[l1].pageNo.pid+" , "+rd[l1].slotNo+" ] : ");
				pw_RID.flush();
				for(int k1=0;k1<numColumns;k1++){
					pw_RID.print("[ "+rd[k1].pageNo.pid+" , "+rd[k1].slotNo+" ] ");
					pw_RID.flush();
				}
				pw_RID.print("{ "+(cnt)+" }");
				
				pw_RID.flush();
				pw_RID.println("");
			}
*/			
			cnt++;
			pw_cnt.println(columnarFileName + "tuple_count: " + cnt);
			pw_cnt.flush();
			pw_cnt.close();
		//End
	
			numTuplesInserted++;
			Tuple tempTuple = new Tuple();
			tempTuple = cf.getTuple(tid);
			tempTuple.print(type);
		}
		
		s.close();
		System.out.println("Total tuples inserted: " + numTuplesInserted);
		System.out.println("Read count: " + PCounter.rcounter);
		System.out.println("Write count: " + PCounter.wcounter);
	}
	
	static void initDB(String columnDBName)
	{

		 String dbpath = DIRPATH + System.getProperty("user.name") + ".minibase." + columnDBName;

		 File f = new File(dbpath);
		 if(!f.exists())
		 {
			 SystemDefs.MINIBASE_RESTART_FLAG = false;
		 }
		 SystemDefs sysdef = new SystemDefs( dbpath, 100000, 1000, "Clock" );
	}
	
}