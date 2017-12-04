package com.siva.hadoop.training.job;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateConv {
	public static void main(String[] args) {
		String dd = "12334";
		Long input = Long.valueOf(dd);
		Date d = new Date(input * 1000L);
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		String dt = sdf.format(d);
	}
}
