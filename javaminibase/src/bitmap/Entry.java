package bitmap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import global.PageId;
//import Entry.*;

public class Entry {
	public static void addEntry(String f, PageId pId) throws IOException{
		int p=pId.pid;
		String k="bmpEntry.txt";
		File bmap=new File(k);
		if(!bmap.exists()){
			bmap.createNewFile();
			//Logging.addLog(k);
		}
		FileWriter fw=new FileWriter(bmap,true);
		PrintWriter pw= new PrintWriter(fw);
		pw.println(f+":"+p);
		pw.flush();
		pw.close();
	}
	public static PageId getEntry(String f) throws IOException{
		String k="bmpEntry.txt";
		File bmap=new File(k);
		if(!bmap.exists()){
			bmap.createNewFile();
			//Logging.addLog(k);
		}
		Scanner sc=new Scanner(bmap);
		sc.useDelimiter("\\A");
		if(!sc.hasNext()){
			return null;
		}
		String input=sc.next();
		sc.close();
		Pattern pattern=Pattern.compile(f+":[\\d]+");
		Matcher matcher=pattern.matcher(input);
		while(matcher.find()){
			String lyn=matcher.group();
			lyn=lyn.trim();
			String[] split=lyn.split(":");
			int ret=Integer.parseInt(split[1].trim());
			PageId pageId=new PageId(ret);
			return pageId;
		}
		input=null;
		return null;
		
	}

}
