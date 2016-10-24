/*
 * @author Shweta Verma
 */
package com.iiit.db;

public class Metadata {

	
	
	public Metadata() {
		super();
	}
	public Integer memoryRecords;  //No.of records that can be sorted at time in main memory
	public Integer totalRecords;   //total number of records;
	public Integer numSubList;   //No.of sublists createds
	public Integer sortIndex ;  //List of index on which sort needs to be performed;
	public String inputFile;
	public String outputFile;
	public Integer numCols;  //No.of columns

	
	
	
}
