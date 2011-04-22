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
		
		String[] arr = new String[6];
		
		arr[0] = args[0];
		arr[1] = args[1];
		arr[2] = args[2];
		arr[3] = args[3];
		arr[4] = args[4];
		arr[5] = args[5];
		
		try {
		RunQueryOnBitMap.queryOnBitMap(arr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated method stub
	}
}