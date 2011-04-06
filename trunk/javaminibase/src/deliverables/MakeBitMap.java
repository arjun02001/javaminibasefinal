package deliverables;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

import tests.RunQueryBMap;

import columnar.ColumnarFile;
import global.IntValueClass;
import global.StringValueClass;
import global.ValueClass;
import diskmgr.PCounter;
import global.AttrType;
import global.GlobalConst;
import global.SystemDefs;

public class MakeBitMap implements GlobalConst{
	public static void makeBitMap(String[] args) throws IOException{
		String columnarDBName=args[0].trim();			//mydb
		String columnarFileName=args[1].trim();			//columnarfile
		String columnName=args[2].trim();				//A
		PCounter.initialize();
		initDB(columnarDBName, 50);
		Scanner s1 = new Scanner(new File(DIRPATH + columnarFileName + "_schema.txt"));
		int numColumns = 0;
		while(s1.hasNextLine())	//count the no. of lines in schema file
		{
			s1.nextLine();
			numColumns++;
		}
		s1.close();
		s1 = new Scanner(new File(DIRPATH + columnarFileName + "_schema.txt"));
		AttrType[] type = new AttrType[numColumns];
		int j = 0;
		int strCount = 0;
		while(s1.hasNextLine())	//construct the type[]
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
			cf.distinctValues(column);
			String f=columnarFileName+columnName.toLowerCase()+".txt";
			File file=new File(f);
			Scanner sc_dist=new Scanner(file);
			while(sc_dist.hasNextLine()){
				String val=sc_dist.nextLine();
				val=val.trim();
				if(type[column-1].attrType==AttrType.attrInteger){
					ValueClass v=new IntValueClass(Integer.parseInt(val));
					cf.createBitMapIndex(column,v);
				}
				else{
					ValueClass v=new StringValueClass(val);
					cf.createBitMapIndex(column,v);
				}
			}
			sc_dist.close();
			//RunQueryBMap.main();
			System.out.println("Bit map index created.");
			System.out.println("Read count: " + PCounter.rcounter);
			System.out.println("Write count: " + PCounter.wcounter);

		} catch (Exception e) {
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
