package com.iiit.db;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Vector;

public class HashJoin {
	
	Integer modNum;
	
	public HashJoin() {
		
	}
	
	public void performJoin(Metadata meta1,Metadata meta2)
	{
				
		if(Driver.M  == 1)
		{
			modNum = 10;
		}else
		{
			modNum = Driver.M;
		}
		
		
		System.out.println("Hashing input files...");
		
		performHash(meta1.inputFile,meta1);
		performHash(meta2.inputFile,meta2);
		
		System.out.println("Performing Join...");
		
		performUnion(meta1,meta2);
		deleteAllOutputFiles(meta1,meta2);
		
		
		
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
	
	
	private void performUnion(Metadata meta1,Metadata meta2) {
		// TODO Auto-generated method stub
		
		
		int hashSize=10000;
//		int maxSize = (numRecords*M)/hashSize;
		
		HashMap<Integer, String> outFileMap = new HashMap<>();
 		
		int i,hashBucket;
		Vector<Vector<String[]>> hashMap = new Vector<>(hashSize);
		for( i = 0;i<hashSize;i++)
		{
			hashMap.add(new Vector<>());
		}
		
		String outFileName = meta1.inputFile+"_"+meta2.inputFile+"_join";
		createFile(outFileName);
		Vector<String> output = new Vector<>();
		int cnt=0;
		
		try{
			

			for(i=0;i<modNum;i++)
			{
				//Read table1 i'th file, Read table2 i'th file,perform hash
				
				BufferedReader buff = new BufferedReader(new FileReader(meta1.outputFile+i));
				String line;
				
				while((line = buff.readLine())!=null)
				{
					String tokens[] = line.split(Constants.SEPARATOR);
//					int index=Integer.parseInt(tokens[pKey]);
					int index= Math.abs(Objects.hashCode(tokens[meta1.sortIndex]));
					hashBucket = index%(hashSize);
					
					hashMap.get(hashBucket).add(tokens);
					
				}
				
				buff.close();
				
				
				buff = new BufferedReader(new FileReader(meta2.outputFile+i));
				
				while((line = buff.readLine())!=null)
				{
					String tokens[] = line.split(Constants.SEPARATOR);
//					int index=Integer.parseInt(tokens[pKey]);
					int index= Math.abs(Objects.hashCode(tokens[meta2.sortIndex]));
					String field = tokens[meta2.sortIndex];
					hashBucket = index%(hashSize);
					
					if(hashMap.get(hashBucket).size()>=1)
					{
						int j;
						for(j=0;j<hashMap.get(hashBucket).size();j++)
						{
							if(field.compareTo(hashMap.get(hashBucket).get(j)[meta1.sortIndex])==0)
							{
								String tokens2[] = hashMap.get(hashBucket).get(j);
								
								String str="";
								for(int k=0;k<tokens2.length;k++)
								{
									str=str+tokens2[k]+Constants.SEPARATOR;
								}
								
								for(int k=0;k<tokens.length;k++)
								{
									if(k!=meta2.sortIndex)
									{
										str=str+tokens[k];
										
										if(k!=tokens.length-1)
										{
											str=str+Constants.SEPARATOR;
										}
									}
								}
								
								
								output.add(str);
								if(output.size()==Driver.blockSize)
								{
									writeToFile(outFileName,output);
									output.removeAllElements();
								}
							}
						}
					}
					
					
				}
				
				
				buff.close();
					
				
			}
			
			
			writeToFile(outFileName,output);
			
			for(String fileName :outFileMap.values())
			{
				boolean success = (new File(fileName)).delete();
			}
			

			
		}catch(IOException e)
		{
			e.printStackTrace();
		}
				
		
	}

	void performHash(String fileName,Metadata meta)
	{
		int i,hashBucket;
		Vector<Vector<String>> hashMap = new Vector<>(modNum);
		for( i = 0;i<modNum;i++)
		{
			createFile(meta.outputFile+i);
			hashMap.add(new Vector<>());
		}
		
		try {
			
			BufferedReader buff = new BufferedReader(new FileReader(fileName));
			String line;
			
			while((line = buff.readLine())!=null)
			{
				String tokens[] = line.split(Constants.SEPARATOR);
//				int index=Integer.parseInt(tokens[pKey]);
				int index= Math.abs(Objects.hashCode(tokens[meta.sortIndex]));
			
				hashBucket = index%(modNum);
//				System.out.println("Hash Bucket : "+hashBucket+" Value: "+index);
	
				hashMap.get(hashBucket).add(line);
				
				if(hashMap.get(hashBucket).size()==Driver.blockSize)
				{
					writeToFile(meta.outputFile+hashBucket,hashMap.get(hashBucket));
					hashMap.get(hashBucket).removeAllElements();
				}
				
		
			}
			
			buff.close();
			
			
			for( i = 0;i<modNum;i++)
			{
				writeToFile(meta.outputFile+i,hashMap.get(i));
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public static void createFile(String fileName)
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

	public  void writeToFile(String fileName,Vector<String> tuples)
	{ 
		PrintWriter pw;
		try {
			pw = new PrintWriter(new FileWriter(fileName,true));
			for(String tuple:tuples)
			{
				//System.out.println(tuple);
				pw.write(tuple);
				pw.write("\n");
			}
	        
	        pw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public boolean checkForRecord(String fileName,String record)
	{
		try
		{
			BufferedReader buff1 = new BufferedReader(new FileReader(fileName));
			String line;
			
			while((line = buff1.readLine())!=null)
			{
				if(record.compareTo(line)==0)
				{
					return true;
				}
			}
			buff1.close();
			
		}catch(IOException e)
		{
			e.printStackTrace();
		}
		
		
		return false;
	}
	
	
	public void deleteAllOutputFiles(Metadata meta1,Metadata meta2)
	{
		for(int i=0;i<modNum;i++)
		{
			boolean success = (new File(meta1.outputFile+i)).delete();
		     if (success) {
		        
		     }
		     success = (new File(meta2.outputFile+i)).delete();
		}
		
		
	}
	
}
