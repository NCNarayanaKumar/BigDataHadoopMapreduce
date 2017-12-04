package com.siva.hadoop.training.job;

import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.PdfTextExtractor;

public class PdfFileReader {
	/**
	 * @param args
	 *            the command line arguments
	 */
	private static String INPUTFILE = "/home/hadoop/myhadoop.pdf";

	// Specifying the file location.

	public static void main(String[] args) {
		try {

			PdfReader reader = new PdfReader(INPUTFILE);
			int n = reader.getNumberOfPages();
			String str = PdfTextExtractor.getTextFromPage(reader, 2);
			// Extracting the content from a particular page.
			System.out.println(str);
			reader.close();

		} catch (Exception e) {
			System.out.println(e);
		}

	}
}
