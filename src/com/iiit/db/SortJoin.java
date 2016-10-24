package com.iiit.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;


public class SortJoin {
	
	String minValue="";
	
	public SortJoin()
	{
		
	}
	
	
	public void performMyJoin(Metadata meta1,Metadata meta2)
	{
		Vector<ReadFile> read_table1 = new  Vector<>();
		Vector<ReadFile> read_table2 = new  Vector<>();
		String outFileName = meta1.inputFile+"_"+meta2.inputFile+"_join";
		createFile(outFileName);
		int i=0;
		List<List> tempTab1 = new ArrayList<>();  
		List<List> tempTab2 = new ArrayList<>();  
		
		/* open all read buffers */
		
		for(i=0;i<meta1.numSubList;i++)
		{
			try {
				ReadFile rd_table1 = new ReadFile();
				BufferedReader buff = new BufferedReader(new FileReader(meta1.outputFile+i+".txt"));
				rd_table1.buff = buff;	
				
				List tuple = readLineToList(buff);
				if(tuple!=null)
				{
					tempTab1.add(tuple);
				}
				
				read_table1.add(rd_table1);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		for(i=0;i<meta2.numSubList;i++)
		{
			try {
				ReadFile rd_table2 = new ReadFile();
				BufferedReader buff = new BufferedReader(new FileReader(meta2.outputFile+i+".txt"));
				rd_table2.buff = buff;	
				
				List tuple = readLineToList(buff);
				if(tuple!=null)
				{
					tempTab2.add(tuple);
				}
				
				read_table2.add(rd_table2);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		List<String> output = new ArrayList<>();
		
		 int nullCntTbl1 = 0;
		 int nullCntTbl2 = 0;
		
		 
		
		while(true)
		{
			List<List> minTab1 = new ArrayList<>();  
			List<List> minTab2 = new ArrayList<>();

			String minBoth = findMin(tempTab1,meta1,tempTab2,meta2);
			
			
			for(i=0;i<tempTab1.size();i++)
			{
				if(tempTab1.get(i)!=null && tempTab1.get(i).get(meta1.sortIndex).toString().equals(minBoth))
				{
					minTab1.add(tempTab1.get(i));
					while(true)
					{
						List<String> tuple  = readLineToList(read_table1.get(i).buff);
						
						if(tuple==null)
						{
							tempTab1.set(i, null);
							nullCntTbl1++;
							break;
						}
						
						if(tuple.get(meta1.sortIndex).toString().equals(minBoth))
						{
							minTab1.add(tuple);
						}else
						{
							tempTab1.set(i,tuple);
							break;
						}
 					}
					
				}
			}
			
			
			for(i=0;i<tempTab2.size();i++)
			{
				if(tempTab2.get(i)!=null && tempTab2.get(i).get(meta2.sortIndex).equals(minBoth))
				{
					minTab2.add(tempTab2.get(i));
					while(true)
					{
						List<String> tuple  = readLineToList(read_table2.get(i).buff);
						
						if(tuple==null)
						{
							tempTab2.set(i, null);
							nullCntTbl2++;
							break;
						}
						
						if(tuple.get(meta2.sortIndex).toString().equals(minBoth))
						{
							minTab2.add(tuple);
						}else
						{
							tempTab2.set(i,tuple);
							break;
						}
 					}
					
				}
			}
			
			
			if(minTab1.size()!=0 && minTab2.size()!=0)
			{
				
				
				for(i=0;i<minTab1.size();i++)
				{
					List<String> tuple = minTab1.get(i);
					String str="";
					for(int j=0;j<tuple.size();j++)
					{
						str=str+tuple.get(j)+Constants.SEPARATOR;
					}
					

				
					
					for(int j=0;j<minTab2.size();j++)
					{
						String str2 = str;
						List<String> tuple2 = minTab2.get(j);
						for(int k=0;k<tuple2.size();k++)
						{
							if(k!=meta2.sortIndex)
							{
								str2=str2+tuple2.get(k);
								
								if(k!=tuple2.size()-1)
								{
									str2=str2+Constants.SEPARATOR;
								}
							}
						}
						
//						System.out.println(str2);
						output.add(str2);
						
						
						if(output.size()==Driver.blockSize)
						{
							writeToOutFile(outFileName, output);
							output = new ArrayList<>();
						}
						
						
					}
				}
			}
			
			if(nullCntTbl1==meta1.numSubList || nullCntTbl2==meta2.numSubList)
			{
				break;
			}
			
			
			
		}
		
		writeToOutFile(outFileName, output);
		System.out.println("Deleting output files");
		deleteAllOutputFiles(meta1, meta2);
		
	}
	
	
	
	
	private String findMin(List<List> tempTab1, Metadata meta1, List<List> tempTab2, Metadata meta2) {
		// TODO Auto-generated method stub
		
		int i=0;
		int min=0;
		while(i<tempTab1.size() && tempTab1.get(i)==null)
		{
			i++;
		}
		min=i;
		
		String minValue = (String) tempTab1.get(min).get(meta1.sortIndex);
		
		for(i=min+1;i<tempTab1.size();i++)
		{
			if(tempTab1.get(i)!=null)
			{
				if(minValue.compareTo(tempTab1.get(i).get(meta1.sortIndex).toString())>0)
				{
					minValue = tempTab1.get(i).get(meta1.sortIndex).toString();
				}
			}
		}
		
		for(i=0;i<tempTab2.size();i++)
		{
			if(tempTab2.get(i)!=null)
			{
				if(minValue.compareTo(tempTab2.get(i).get(meta2.sortIndex).toString())>0)
				{
					minValue = tempTab2.get(i).get(meta2.sortIndex).toString();
				}
			}
		}
		
		
		return minValue;
	}


	
	
	

	
	public int getMin(List<List> tab,Metadata meta)
	{
		
		int i=0;
		int min=0;
		while(i<meta.numSubList && tab.get(i)==null)
		{
			i++;
		}
		min=i;
		
//		System.out.print("Min "+min + " i "+ i + "table length "+tab.size());
		
		for(i=min+1;i<meta.numSubList;i++)
		{
//			System.out.println(tab.get(i).get(4));
			if(tab.get(i)!=null)
			{
				int cmp = new Asc_Comparator(meta).compare(tab.get(min),tab.get(i));
				if(cmp<0)
				{
					min = i;
				}
			}
			
		}
		
		return min;
	}
	
	
	
	public void deleteAllOutputFiles(Metadata meta1,Metadata meta2)
	{
		for(int i=0;i<meta1.numSubList;i++)
		{
			boolean success = (new File(meta1.outputFile+i+".txt")).delete();
		}
		for(int i=0;i<meta2.numSubList;i++)
		{
			boolean success = (new File(meta2.outputFile+i+".txt")).delete();
		}
		
		
	}
	
	
	public static List<String> readLineToList(BufferedReader buff)
	{
		
		String line=null;
		List<String> tuple = new ArrayList<>();
		try {
			line = buff.readLine();
//			System.out.println("line is " + line);
			
			if(line!=null)
			{
				String tokens[] = line.split(Constants.SEPARATOR);
//				System.out.println("token length"+tokens.length);
				for (int i = 0; i <tokens.length; i++) {
			        tuple.add(tokens[i]);
			    } 
			}else
			{
				buff.close();
				return null;
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		return tuple;
		
		
	}
	
	
	
	public List<List> readFileToList(BufferedReader buff) throws IOException
	{
		int cnt=0;
		String line="";
		List<List> tab = new ArrayList<>();
		while((line = buff.readLine())!=null)
		{
			String tokens[] = line.split(Constants.SEPARATOR);
			
			List tuple = new ArrayList<>();
			
			 for (int i = 0; i <tokens.length; i++) {
			        tuple.add(tokens[i]);
			    } 
			 
			 tab.add(tuple);
			 cnt++;
			 
			 if(cnt==Driver.blockSize)
			 {
				 break;
			 }
		}
	
	
	
		if(tab.size()==0)
		{
			buff.close();
			return null;
		}
		else
			return tab;
	

	}
	
	public void createSublist(Metadata meta)
	{
		String line="";
		try {
			BufferedReader buff = new BufferedReader(new FileReader(meta.inputFile));
			int cnt=0;
			int size=0;
			int subListNum=0;
			
			String tokens[]={};
			while(size!=meta.totalRecords)
			{
				List<List> tab = new ArrayList<>();
				while(cnt!=meta.memoryRecords && (line = buff.readLine())!=null)
				{
					tokens = line.split(Constants.SEPARATOR);
					
					List tuple = new ArrayList<>();
					
					 for (int i = 0; i <tokens.length; i++) {
					        tuple.add(tokens[i]);
					    } 
					 
					 tab.add(tuple);
					 cnt++;
				}
				
				
				
				Collections.sort(tab,new Asc_Comparator(meta));
				

//				System.out.println("writiti "+meta.outputFile+subListNum+".txt");
				createFile(meta.outputFile+subListNum+".txt");
				writeToFile(meta.outputFile+subListNum+".txt", tab);
				size = size+cnt;
				cnt=0;
				subListNum++;
				
				
				
				if(line==null)
				{
					break;
				}
			}
						
			buff.close();
						
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Driver.myExit("Error Opening File");
		}     
	}
	
	
	
	public void writeToFile(String fileName, List<List> tab )
	{
		PrintWriter pw = null;
		
		
		for(int i = 0;i<tab.size();i++)
		{
			StringBuilder sb = new StringBuilder();
			List tuple = tab.get(i);
	        sb.append(tuple.get(0).toString());
		
			for(int j=1;j<tuple.size();j++)
			{
				sb.append(Constants.SEPARATOR);
				sb.append(tuple.get(j).toString());
			}
			
			//if(i!=tab.size()-1)
				sb.append("\n");
				
				try {
					
					pw = new PrintWriter(new FileWriter(fileName,true));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					Driver.myExit("File not found");
				}

		        pw.write(sb.toString());
		        pw.close();
				
				
			
		}
		
		
		
	
			
	}
	
	public void writeToOutFile(String fileName, List<String> tab )
	{
		
		PrintWriter pw = null;
		
		StringBuilder sb = new StringBuilder();
		for(int i = 0;i<tab.size();i++)
		{
	        sb.append(tab.get(i));
			sb.append("\n");
		}

		try {
			
			pw = new PrintWriter(new FileWriter(fileName,true));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Driver.myExit("File not found");
		}

        pw.write(sb.toString());
        pw.close();
		
	
	}
	
	
	public void createFile(String fileName)
	{
		PrintWriter pw = null;
		
		try {
			
			pw = new PrintWriter(new FileWriter(fileName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Driver.myExit("File not found");
		}

        pw.close();
	}

	
	
	/* this is my code */

	
	public void performFinalJoin(Metadata meta1,Metadata meta2)
	{
		Vector<ReadFile> read_table1 = new  Vector<>();
		Vector<ReadFile> read_table2 = new  Vector<>();
		String outFileName = meta1.inputFile+"_"+meta2.inputFile+"_join";
		createFile(outFileName);
		int i=0;
		List<List> tempTab1 = new ArrayList<>();  
		List<List> tempTab2 = new ArrayList<>();  
		
		/* open all read buffers */
		
		for(i=0;i<meta1.numSubList;i++)
		{
			try {
				ReadFile rd_table1 = new ReadFile();
				BufferedReader buff = new BufferedReader(new FileReader(meta1.outputFile+i+".txt"));
				rd_table1.buff = buff;	
				
				List tuple = readLineToList(buff);
				if(tuple!=null)
				{
					tempTab1.add(tuple);
				}
				
				read_table1.add(rd_table1);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		for(i=0;i<meta2.numSubList;i++)
		{
			try {
				ReadFile rd_table2 = new ReadFile();
				BufferedReader buff = new BufferedReader(new FileReader(meta2.outputFile+i+".txt"));
				rd_table2.buff = buff;	
				
				List tuple = readLineToList(buff);
				if(tuple!=null)
				{
					tempTab2.add(tuple);
				}
				
				read_table2.add(rd_table2);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		List<String> output = new ArrayList<>();
		
		 int nullCntTbl1 = 0;
		 int nullCntTbl2 = 0;
		
		 
//		 System.out.println(tempTab1+"\n");
//		 System.out.println(tempTab2);
		
		while(true)
		{
			List<List> minTab1 = new ArrayList<>();  
			List<List> minTab2 = new ArrayList<>();
//			System.out.println("temp 1 table"+tempTab1);
			int min = getMin(tempTab1, meta1);
			minValue  = (String) tempTab1.get(min).get(meta1.sortIndex);
			
			
			for(i=0;i<meta1.numSubList;i++)
			{
				if(tempTab1.get(i)!=null && tempTab1.get(i).get(meta1.sortIndex).toString().equals(minValue))
				{
					minTab1.add(tempTab1.get(i));
					List<String> tuple = readLineToList(read_table1.get(i).buff);
					if(tuple==null)
					{
						tempTab1.set(i,null);
						nullCntTbl1++;
						
						if(nullCntTbl1==meta1.numSubList)
							break;
						
					}else
					{
						tempTab1.set(i,tuple);
						i--;
					}	
				}
			}
			
//			System.out.println("min value table "+minTab1.size());
			
			while(true)
			{
				int min2 = getMin(tempTab2, meta2);
				List<String> tuple2 = tempTab2.get(min2);
				
				int cmp = minValue.compareTo(tuple2.get(meta2.sortIndex));
				
				System.out.println("Comparator "+cmp+ " Min value "+minValue + " second " + tuple2.get(meta2.sortIndex));
				
				if(minValue.compareTo(tuple2.get(meta2.sortIndex))<0)
				{
					System.out.println("Breaking because min is greater than mymin value found");
					break;
				}else if(minValue.compareTo(tuple2.get(meta2.sortIndex))==0)
				{
					minTab2.add(tuple2);
					System.out.println("Same found " );
				}
				
//				System.out.println("Min value found "+tuple2);
				
				tuple2 = readLineToList(read_table2.get(min2).buff);
//				System.out.println("*******New tuple read   " + tuple2);
				if(tuple2==null)
				{
					tempTab2.set(min2, null);
					nullCntTbl2++;
					if(nullCntTbl2==meta2.numSubList)
					{
						break;
					}
					
				}else
				{
					tempTab2.set(min2, tuple2);
				}
				
				
			}
			
			
//			System.out.println("mintable1  size "+minTab1.size() + " min table2 size "  +minTab2.size());
//			System.out.println("min value table2 "+minTab2.size());
		
			
			if(minTab1.size()!=0 && minTab2.size()!=0)
			{
				
				
				for(i=0;i<minTab1.size();i++)
				{
					List<String> tuple = minTab1.get(i);
					String str="";
					for(int j=0;j<tuple.size();j++)
					{
						str=str+tuple.get(j)+Constants.SEPARATOR;
					}
					

				
					
					for(int j=0;j<minTab2.size();j++)
					{
						String str2 = str;
						List<String> tuple2 = minTab2.get(j);
						for(int k=0;k<tuple2.size();k++)
						{
//							if(k!=meta2.sortIndex)
							{
								str2=str2+tuple2.get(k);
								
								if(k!=tuple2.size()-1)
								{
									str2=str2+Constants.SEPARATOR;
								}
							}
						}
						
//						System.out.println(str2);
						output.add(str2);
						
						
						if(output.size()==Driver.blockSize)
						{
							writeToOutFile(outFileName, output);
							output = new ArrayList<>();
						}
						
						
					}
				}
			}
			
//			break;
			if(nullCntTbl1==meta1.numSubList || nullCntTbl2==meta2.numSubList)
			{
				System.out.println("Null count2 " + nullCntTbl2);
				System.out.println("Null count1 " + nullCntTbl1);
				System.out.println("SUBLIST1 " + meta1.numSubList);
				System.out.println("SUBLIST2 " + meta2.numSubList);
				
				System.out.println("Value reached");
				break;
			}
			
		}
		
		writeToOutFile(outFileName, output);
		System.out.println("Deleting output files");
//		deleteAllOutputFiles(meta1, meta2);
		
	}
	
	
	
	
	
}



