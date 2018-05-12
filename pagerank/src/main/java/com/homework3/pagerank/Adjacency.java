package com.homework3.pagerank;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.homework3.pagerank.PageRankDriver.PageCount;

public class Adjacency {
	private static Pattern namePattern;
	private static Pattern linkPattern;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}

	public static class ParserMapper extends Mapper<Object, Text, Text, Text>{
		List<String> linkPageNames;
		XMLReader xmlReader;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException  {
			try {
				// Configure parser.
				SAXParserFactory spf = SAXParserFactory.newInstance();
				spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
				SAXParser saxParser = spf.newSAXParser();
				xmlReader = saxParser.getXMLReader();
				// Parser fills this list with linked page names.
				linkPageNames = new LinkedList<>();
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		
		// Map task which emit nodes and adjacency list
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			xmlReader.setContentHandler(new WikiParser(linkPageNames));
			String line = value.toString();
			int delimLoc = line.indexOf(':');
			//String pageName = line.substring(0, delimLoc);
			String pageName = new String(Arrays.copyOf(value.getBytes(), delimLoc), "latin1");
			String html = line.substring(delimLoc + 1);
			html = html.replaceAll("&", "&amp;");
			Matcher matcher = namePattern.matcher(pageName);
			if (!matcher.find()) {
				// Skip this html file, name contains (~).
				return;
			}
			// Parse page and fill list of linked pages.
			linkPageNames.clear();
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));	
			} catch (Exception e) {
				// Discard ill-formatted pages.
				return;
			}
			List<String> adjTemp = new LinkedList<String>();
			// Remove duplicates in the adjacency list
			Set<String> uniqueList = new HashSet<String>();
			uniqueList.addAll(linkPageNames);
			List<String> list = new LinkedList<String>(uniqueList);
			String links = String.join("~", list);
			
			context.write(new Text(pageName), new Text(links));
			for(String page : list) {
				context.write(new Text(page), new Text());
			}
		}
	}
	
	// Reduce task that handles dangling nodes
	public static class ParserReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean flag= false;
			for (Text each: values) {
				if (each.toString().length()>0) {
					context.write(key, each);
					context.getCounter(PageCount.Counter).increment(1);
					flag = true;
				}
			}
			if (!flag) {
				context.write(key, new Text(""));
				context.getCounter(PageCount.Counter).increment(1);
			}
		}
	}
	
	// Mapper to initialize page rank values for all nodes
	public static class ParserInitialPgMapper extends Mapper<Object, Text, Text, Text>{
		double initialPg;
		
		protected void setup(Context context) throws IOException,InterruptedException  {
			Configuration conf = context.getConfiguration();
			initialPg =conf.getDouble("Pages", 0.0);
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			List<String> tokens = new ArrayList<String>();
			tokens = Arrays.asList(value.toString().split("\\s+"));
			String rank = Double.toString(1.0/initialPg);
			if (tokens.size()==1) {
				context.write(new Text(tokens.get(0)), new Text(rank));
			}
			else context.write(new Text(tokens.get(0)), new Text(rank+ " "+tokens.get(1))); 
		}
	}
	
	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {
		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}
	
}
