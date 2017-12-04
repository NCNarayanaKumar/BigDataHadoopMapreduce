package com.siva.hadoop.training.job;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class RemoteDOMParserClass {
	public static void main(String[] args) throws ParserConfigurationException, IOException, SAXException {

		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		URL url = new URL("https://appvigil.co/test1.xml");
		InputStream inputStream = url.openStream();
		Document document = db.parse(inputStream);

		Element docEle = document.getDocumentElement();

		// Print root element of the document
		System.out.println("Root element of the document: " + docEle.getNodeName());

		NodeList list = docEle.getElementsByTagName("properties");

		// print total element in file to verify file is readed properly
		System.out.println("Total : " + list.getLength());

		if (list != null && list.getLength() > 0) {
			for (int i = 0; i < list.getLength(); i++) {

				Node node = list.item(i);

				if (node.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("=====================");

					Element e = (Element) node;
					NodeList nodeList = e.getElementsByTagName("project.build.sourceEncoding");
					System.out.println("Name of element: " + nodeList.item(0).getChildNodes().item(0).getNodeValue());

					inputStream.close();

				}
			}
		}

	}
}