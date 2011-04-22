package tests;

import global.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import deliverables.RunDeleteOnBitMap;

public class Delete {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[]args) throws Exception {
	
		String[] arr = new String[7];
		
		arr[0] = args[0];
		arr[1] = args[1];
		arr[2] = args[2];
		arr[3] = args[3];
		arr[4] = args[4];
		
		if(args.length == 6)
			arr[5] = args[5];
		else
			arr[5] = "null";
		
		try {
		RunDeleteOnBitMap.queryOnBitMap(arr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated method stub
	}
}