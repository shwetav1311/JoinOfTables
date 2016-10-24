package com.iiit.db;

import java.util.Comparator;
import java.util.List;

public class Asc_Comparator implements Comparator<List>{

	Metadata meta;
	
	public Asc_Comparator(Metadata meta) {
		// TODO Auto-generated constructor stub
		
		this.meta = meta;
	}

	@Override
	public int compare(List l1, List l2) {
		// TODO Auto-generated method stub
		
			 if(Driver.dataType==Constants.DATATYPE_INT)
			{
				Integer int1 = Integer.parseInt(l1.get(meta.sortIndex).toString());
				Integer int2 = Integer.parseInt(l2.get(meta.sortIndex).toString());
				
				if(int1<int2)
				{
					return -1;
				}else if ( int1> int2)
				{
					return 1;
				}
			}
			else
			{
				if(l1.get(meta.sortIndex).toString().compareTo(l2.get(meta.sortIndex).toString())!= 0)
				{
					return l1.get(meta.sortIndex).toString().compareTo(l2.get(meta.sortIndex).toString());
				}
			}
			
		
	    return 0;
	}

}
