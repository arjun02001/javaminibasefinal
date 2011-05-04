package global;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class MarkEliminate implements GlobalConst{

	public MarkEliminate(String filename, int numRecords) throws IOException
	{
		File conditionFile = new File(DIRPATH + filename + "_eliminate.txt");
		RandomAccessFile raF = new RandomAccessFile(conditionFile, "rw");
		raF.seek(0);
		for (int i=0; i < numRecords+1; i++)
			raF.write(48);
		raF.close();
	}
	
	public void close(String fileName)
	{
		 File f = new File(fileName);
		 f.delete();
	}
	
	public boolean isEliminated(String filename, int position) throws IOException
	{
		File conditionFile = new File(DIRPATH + filename + "_eliminate.txt");
		RandomAccessFile raF = new RandomAccessFile(conditionFile, "rw");
		raF.seek(position);
		int value = raF.read();
		if(value == 48)
		return false;//not deleted
		else 
		return true;//deleted
	}
	
	public void setEliminated(String filename, int position) throws IOException
	{
		File Deletefile = new File(DIRPATH + filename + "_eliminate.txt");
		RandomAccessFile raF = new RandomAccessFile(Deletefile, "rw");
		raF.seek(position);
		raF.write(49);
		raF.close();
	}
	
	public void testAndSetEliminated(String filename, int position) throws IOException
	{
		File Deletefile = new File(DIRPATH + filename + "_eliminate.txt");
		RandomAccessFile raF = new RandomAccessFile(Deletefile, "rw");

		raF.seek(position);
		int value = raF.read();
		raF.seek(position);
		raF.write(value+1);
		raF.close();
	}
	
	
	public long getLength(String filename) throws IOException
	{
		File Deletefile = new File(DIRPATH + filename + "_eliminate.txt");
		RandomAccessFile raF = new RandomAccessFile(Deletefile, "rw");
		return raF.length();
	}

}
