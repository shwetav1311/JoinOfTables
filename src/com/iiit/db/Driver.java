package com.iiit.db;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import javax.print.attribute.standard.DateTimeAtCompleted;

public class Driver {

	
	public static Integer blockSize=100;  //blocksize
	public static Integer M;  //blocksize
	public static Metadata meta1;
	public static Metadata meta2;
	public static Integer dataType = Constants.DATATYPE_STRING;   // 0 integer 1:String
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		String table1=args[0];
		String table2=args[1];
		String type=args[2];
		M=Integer.parseInt(args[3]);
		
		meta1 = new Metadata();
		meta2 = new Metadata();
		
		meta1.inputFile = table1;
		meta1.memoryRecords = M*blockSize;
		meta1.totalRecords = countLines(table1);
		meta1.numSubList = (int) Math.ceil(meta1.totalRecords / (meta1.memoryRecords*1.0)) ;
		meta1.numCols = getNumColumns(table1);
		meta1.sortIndex = meta1.numCols -1;
		meta1.outputFile=Constants.OUTPUT_1;
		
		
		
		meta2.inputFile = table2;
		meta2.memoryRecords = M*blockSize;
		meta2.totalRecords = countLines(table2);
		meta2.numSubList = (int) Math.ceil(meta2.totalRecords / (meta2.memoryRecords*1.0)) ;
		meta2.numCols = getNumColumns(table2);
		meta2.sortIndex = 0;
		meta2.outputFile=Constants.OUTPUT_2;
		
		
		Long beginTime = new Date().getTime();
		
		if(type.equals(Constants.TYPE_SORT))
		{
			SortJoin sort = new SortJoin();
			System.out.println("Creating Sublists...");
			sort.createSublist(meta1);
			sort.createSublist(meta2);
			System.out.println("Performing Join...");
			sort.performMyJoin(meta1, meta2)	;
		}else
		{
			HashJoin hashJoin  = new HashJoin();
			hashJoin.performJoin(meta1, meta2);
		}
		
		Long endTime = new Date().getTime();
		
		System.out.println("Time taken : "+(endTime-beginTime)/(1000.0));
		
		/** No.of sublists = R + S  = M 
		 * 
		 */
		
		
	}

	private static Integer getNumColumns(String fileName) {
		// TODO Auto-generated method stub
		int index=0;
		String line="";
		try {
			BufferedReader buff = new BufferedReader(new FileReader(fileName));
			
			line = buff.readLine();
			
			String[] token = line.split(Constants.SEPARATOR);
			
			buff.close();
			
			index = token.length;
			
		}catch(IOException e)
		{
			e.printStackTrace();
			
		}
		
		
		return index;
	}

	public static void myExit(String string) {
		// TODO Auto-generated method stub
		System.out.println(string);
		System.exit(0);
	}

	public static int countLines(String filename) throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(filename));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}
	
	
}
