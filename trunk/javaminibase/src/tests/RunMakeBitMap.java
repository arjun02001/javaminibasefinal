package tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import deliverables.MakeBitMap;
import deliverables.RunQueryOnBitMap;

public class RunMakeBitMap {

        
        public static void main(String[] args) {
                
                try {
                        String[] arr = user();
                        MakeBitMap.makeBitMap(arr);
                } catch (IOException e) {
                        e.printStackTrace();
                }

        }

        public static String[] user() throws IOException {
                String[] arr = new String[3];
                System.out.println("Please Enter the information as:  mydb <Enter> columnarfile <Enter> A <Enter>");
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                for(int i = 0; i < 3; i++){
                        arr[i] = br.readLine().trim();
                }
                return arr;
        }

}
