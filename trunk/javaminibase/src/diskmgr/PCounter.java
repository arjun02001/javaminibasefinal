//------------------------------------------------------------------
// Project: Column-Oriented DBMS 
// Course: CSE 510 - Database Management Systems Implementation
//------------------------------------------------------------------
// File name:PCounter.java
//------------------------------------------------------------------
// Author: Adithya Gundmi Ramesh
// Notes:  Created on 3/23/2011
//------------------------------------------------------------------

package diskmgr;

public class PCounter {

	public static int rcounter;
	public static int wcounter;
	
	public PCounter() {
		rcounter = 0;		//initialize counters in ctor
		wcounter = 0;
	}

	public static void initialize() {
		rcounter = 0;
		wcounter = 0;
	}
	public static void readIncrement() {
		rcounter++;
	}
	public static void writeIncrement() {
		wcounter++;
	}
}
