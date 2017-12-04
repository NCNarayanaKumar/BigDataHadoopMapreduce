package com.siva.hadoop.training.jobs;

import java.util.Vector;

public class vector {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	Vector v=new Vector(3,3);
	String[] ven={"ven, mat","san"};
	for(int i=0;i<=3; i++){
		v.add(ven[i]);
			}
System.out.println(v);
System.out.println(v.capacity());
	
	
	}

}
