package tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import deliverables.RunQueryOnBitMap;

public class RunQueryBMap {

	/**
	 * @param args
	 */
	public static void main(String[]args) {
		
		try {
			String[] arr = user();
		RunQueryOnBitMap.queryOnBitMap(arr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated method stub

	}

	public static String[] user() throws IOException {
		String[] arr = new String[6];
		System.out
		.println("Enter the information as:  mydb <Enter> columnarfile <Enter> A B C D <Enter> A = arjun <Enter> 50 <Enter> bitmap");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		for(int i = 0; i < 6; i++){
			arr[i] = br.readLine().trim();
		}
		
		// System.out.println("Enter the information as: DBFileName ColumnarFileName ColumnName");
		
		//String usr = br.readLine();
		//String[] ret = usr.split("\\s");
		return arr;

	}

}