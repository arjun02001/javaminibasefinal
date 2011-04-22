package tests;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;

import global.*;

public class UseLess {

	public static void main(String[]args) throws IOException {
	
		//for (int i=0; i<20; i = i+2)
/*		{
			isDeleted(args[0], 0);
	//		isDeleted(args[0], 1);
		//	isDeleted(args[0], 2);
			//isDeleted(args[0], 3);
			//isDeleted(args[0], 4);
		
			File Deletefile = new File("C://tmp//" + args[0] + "_delete.txt");
			RandomAccessFile raF = new RandomAccessFile(Deletefile, "rw");
			raF.seek(0);
			for (int i=0; i<356; i++)
				raF.write(48);
			raF.close();
		} */
		
		
	}
	
	static boolean isDeleted(String filename, int position) throws IOException
	{
	File Deletefile = new File("C://tmp//" + filename + "_delete.txt");
	RandomAccessFile raF = new RandomAccessFile(Deletefile, "rw");
	raF.seek(position);
	for (int i=0; i<20; i++)
		raF.write(48);
	//raF.write('\n');
	
	return true;//not deleted
	}

	

}
