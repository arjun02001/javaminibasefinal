package global;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Mark {

	public Mark(){}
	
	public boolean isDeleted(String filename, int position) throws IOException
	{
		File Deletefile = new File("C://tmp//" + filename + "_delete.txt");
		RandomAccessFile raF = new RandomAccessFile(Deletefile, "rw");
		raF.seek(position);
		int value = raF.read();
		if(value == 48)
		return false;//not deleted
		else 
		return true;//deleted
	}
	
	public void setDeleted(String filename, int position) throws IOException
	{
		File Deletefile = new File("C://tmp//" + filename + "_delete.txt");
		RandomAccessFile raF = new RandomAccessFile(Deletefile, "rw");
		raF.seek(position);
		raF.write(49);
		raF.close();
	}
	
	public long getLength(String filename) throws IOException
	{
		File Deletefile = new File("C://tmp//" + filename + "_delete.txt");
		RandomAccessFile raF = new RandomAccessFile(Deletefile, "rw");
		return raF.length();
	}

}
