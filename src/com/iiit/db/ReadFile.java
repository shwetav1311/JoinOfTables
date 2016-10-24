package com.iiit.db;

import java.io.BufferedReader;
import java.util.List;

public class ReadFile {

	public BufferedReader buff;
	public List<List> tab;
	public int count;
	public ReadFile(BufferedReader buff, List<List> tab) {
		super();
		this.buff = buff;
		this.tab = tab;
		count = 0; 
	}
	public ReadFile() {
		super();
	}
}
