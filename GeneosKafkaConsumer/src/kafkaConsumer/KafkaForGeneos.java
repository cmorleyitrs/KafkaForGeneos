package kafkaConsumer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONException;
import org.json.JSONObject;

/*
 * Create by:	Connor Morley
 * Date:		June 2016
 * Description:	Kafka Java interface, using xpaths in conjunction with topics to discern (and in default form display) information on specific
 * 				Geneos information. Time offset from current time available for historical data collecting (within the Kafka sampling pool of course).
 */

public class KafkaForGeneos {

		public static String kafkaServer = "";
		public static String groupID = "";
		public static String topics = "";
		public static ArrayList<String> xpaths = new ArrayList<String>();
		public static String timeZone = "";
		public static String seekOffset = "";
		public static boolean offsetPerformed = false;
		public static ArrayList<TopicPartition> topicsForOffset = new ArrayList<TopicPartition>();
		public static boolean historical = false;
		public static int historicalCounter = 0;
		public static ArrayList<String> historicalData = new ArrayList<String>();
		public static Map<String, JSONObject> historyTree = new TreeMap<String, JSONObject>();
		public static Long historicalFrom = 0L;
		public static boolean historyFinished = false;
		
		public static void main(String[] args) throws JSONException, InterruptedException {
			Map<String, JSONObject> example;
			//FOR TESTING ONLY!!!
/*			args = new String[10];
			args[0] = "-history";
			args[1] = "-xpath";
			args[2] = "/geneos/gateway[(@name=\"Kafka\")]/directory/probe[(@name=\"New Probe\")]/managedEntity[(@name=\"New Managed entity 1\")]/sampler[(@name=\"New Sampler\")][(@type=\"\")]/dataview[(@name=\"New Sampler\")]/rows/row[(@name=\"cpu_0_logical#1\")]/cell[(@column=\"percentUtilisation\")]";
			args[3] = "-from(s)";
			args[4] = "86400";*/
			
			ArrayList<String> arguments = new ArrayList<String>(Arrays.asList(args));
			FileHandler.readSettingsFile();
			if(arguments.contains("-history")) //If historical data requested reconfigure consumer appropriately.
			{
				topics = "geneos-enriched.table";
				groupID = "historical-grab";
				//xpaths.clear();
				if(arguments.size() < 3)
				{
					System.exit(0);
				}
				//int index = arguments.indexOf("-xpath");
				//xpaths.add(arguments.get(index + 1));
				if(xpaths.size() != 1)
				{
					System.exit(0);
				}
				int indexFrom = arguments.indexOf("-from(s)");
				Long hr = Long.parseLong(arguments.get(indexFrom + 1));
				historicalFrom = (System.currentTimeMillis() / 1000)- hr;
				System.out.println(historicalFrom);
				seekOffset = "beginning";
				historical = true;
				
			}
			Properties props = new Properties();
			props.put("bootstrap.servers", kafkaServer);
			props.put("group.id", groupID);
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("session.timeout.ms", "30000");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("fetch.min.bytes", "50000");
			props.put("receive.buffer.bytes", "262144");
			props.put("max.partition.fetch.bytes", "2097152");
			props.put("consumer.timeout.ms", "1");
			props.put("auto.offset.reset", "earliest");
			KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
			String[] tList = topics.split(",");
			consumer.subscribe(Arrays.asList(tList));
			String[] metaData = {"geneos-probes", "geneos-managed.entities", "geneos-dataviews"};
			ArrayList<String> metaDataTopics = new ArrayList<String>(Arrays.asList(metaData));
			if(!seekOffset.equals(""))
			{
				for(String T : tList)
				{
					if(metaDataTopics.contains(T))
					{
						TopicPartition temp = new TopicPartition(T, 0);
						topicsForOffset.add(temp);
					}
					else
					{
						TopicPartition temp = new TopicPartition(T, 0);
						TopicPartition temp1 = new TopicPartition(T, 1);
						TopicPartition temp2 = new TopicPartition(T, 2);
						TopicPartition temp3 = new TopicPartition(T, 3);
						topicsForOffset.add(temp);
						topicsForOffset.add(temp1);
						topicsForOffset.add(temp2);
						topicsForOffset.add(temp3);
					}
				}
			}
			String[] levels = { "gateway", "probe", "managedEntity", "sampler", "dataview", "name", "cell" };
			Map<Integer, ArrayList<String>> params = extractXpathDetails(xpaths);
			int paramSize = params.size();
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(1000);
				if(!seekOffset.equals("") && offsetPerformed == false)
				{
					if(seekOffset.equals("beginning"))
						consumer.seekToBeginning(topicsForOffset);
					else
						for(TopicPartition T : topicsForOffset)
						{
							consumer.seek(T, Long.parseLong(seekOffset));
						}
					offsetPerformed = true;
				}
				for (ConsumerRecord<String, String> record : records) {
					try {
						System.out.println("TESTING OUTPUT HERE!!! " + record);
						int counter = 0;
						int found = 0;
						JSONObject temp = new JSONObject(record.value()); //THERE ARE FOUR JSON OBJECTS IN ONE KAFKA MESSAGE!!!!
						JSONObject data = temp.getJSONObject("data");
						if(data.has("name"))
						{
						counter = processTableMessage(levels, params, paramSize, record, counter, data);
						}
						if(data.has("severity"))
						{
							processSeverityMessage(levels, params, paramSize, record, counter, data);
						}
					} catch(JSONException e)
					{
						e.printStackTrace();
					}
					catch (Exception e) {
						System.out.println(record.toString());
						e.printStackTrace();
					}
				}
				if(historyFinished)
				printTree(); // Handle the historical data once collected and termiante
			}
		}

		private static int processTableMessage(String[] levels, Map<Integer, ArrayList<String>> params, int paramSize,
				ConsumerRecord<String, String> record, int counter, JSONObject data) throws JSONException, ParseException {
			JSONObject target = data.getJSONObject("target");
			JSONObject row = data.getJSONObject("row");
			for (int i = 0; i < paramSize && i < 5; i++) { //Parameters down to the data view level
				ArrayList<String> current = new ArrayList(params.get(i));
				for(String check : current)
				{
					if (check.equals(target.get(levels[i]))) {
						counter++;
						if(counter == paramSize)
							System.out.println("Dataview :'" + target.get("dataview") + "'	Row name: " + data.get("name") + "	With the values :" + row.toString());
						break;
					}
				}
			}
			if (paramSize > 5 && counter == 5) //If includes row and column parameters
				for (int i = 5; i < paramSize && i < 7; i++) {
					if (i == 5) {
						ArrayList<String> current = new ArrayList(params.get(i));
						for(String check : current)
						{
							if (check.equals(data.get(levels[i]))) {
								counter++;
								if(counter == paramSize)
									System.out.println("Row name: " + data.get(levels[i]) + "	With the values :" + row.toString());
								break;
							}
						}
					}
					if (i == 6) {
						ArrayList<String> current = new ArrayList(params.get(i));
						for(String check : current)
						{
							if (row.has(check) && counter == 6) {
								if(historical) // Check if historical data is requested and if requested limit is reached
								{
								SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
								formatter.setTimeZone(TimeZone.getTimeZone(timeZone)); // Assign appropriate timezone to kafka timestamp that is unaltered
								Date temp = formatter.parse(data.getString("sampleTime"));
								if ((temp.getTime() / 1000) >= historicalFrom) //If within the sample time specitfied by user
									historyTree.put(data.getString("sampleTime"), data);
								if ((temp.getTime() / 1000) >= ((System.currentTimeMillis() / 1000) - 10)) // If the current set reached within 10 seconds of the current system time
									historyFinished = true; // historical data has been successfully collected
							} else
								System.out.println("Row name: " + data.get(levels[i - 1]) + "  Columns " + check
										+ "	with value :" + row.get(check));
							counter++;
							break;
						}
					}
				}
			}
			return counter;
		}

		private static void processSeverityMessage(String[] levels, Map<Integer, ArrayList<String>> params, int paramSize,
				ConsumerRecord<String, String> record, int counter, JSONObject data) throws JSONException {
			JSONObject target = data.getJSONObject("target");
			if(target.length() - 1 < paramSize)
			{
				return;
			}
			else
			{
			for (int i = 0; i < paramSize && i < 7; i++) { //Parameters down to the data view level
				ArrayList<String> current = new ArrayList(params.get(i));
				for(String check : current)
				{
					if (check.equals(target.get(levels[i]))) {
						counter++;
						if(counter == paramSize)
							System.out.println("Xpath level :'" + levels[i] + "' with value :'" + target.get(levels[i]) + "' Changes to severity : '" + data.get("severity") + "'");
						break;
					}
				}
			}
			return;
			}
		}
		
		public static void printTree() throws JSONException // Print/handle the resulting historical data
		{
			for(Map.Entry<String, JSONObject> entry : historyTree.entrySet())
			{
				JSONObject temp = entry.getValue();
				JSONObject target = temp.getJSONObject("target");
				JSONObject row = temp.getJSONObject("row");
				System.out.println("Time : " + entry.getKey() + " Dataview : " + target.get("dataview") + " Row name: " + temp.get("name") + " with values : " + row.toString());
			}
			System.exit(0);
		}

		public static Map<Integer, ArrayList<String>> extractXpathDetails(ArrayList<String> xpaths) {
			Map<Integer, ArrayList<String>> params = new HashMap<Integer, ArrayList<String>>();
			for(String xpath : xpaths)
			{
			if (xpath.contains("gateway")) {
				String filter = xpath.substring(xpath.lastIndexOf("gateway[(@name=\""));
				String ret = filter.substring(16, filter.indexOf("\")"));
				if(!params.containsKey(0))
				{
					ArrayList<String> temp = new ArrayList<String>();
					params.put(0, temp);
				}
				params.get(0).add(ret);
			}
			if (xpath.contains("probe")) {
				String filter = xpath.substring(xpath.lastIndexOf("probe[(@name=\""));
				String ret = filter.substring(14, filter.indexOf("\")"));
				if(!params.containsKey(1))
				{
					ArrayList<String> temp = new ArrayList<String>();
					params.put(1, temp);
				}
				params.get(1).add(ret);
			}
			if (xpath.contains("managedEntity")) {
				String filter = xpath.substring(xpath.lastIndexOf("managedEntity[(@name=\""));
				String ret = filter.substring(22, filter.indexOf("\")"));
				if(!params.containsKey(2))
				{
					ArrayList<String> temp = new ArrayList<String>();
					params.put(2, temp);
				}
				params.get(2).add(ret);
			}
			if (xpath.contains("sampler")) {
				String filter = xpath.substring(xpath.lastIndexOf("sampler[(@name=\""));
				String ret = filter.substring(16, filter.indexOf("\")"));
				if(!params.containsKey(3))
				{
					ArrayList<String> temp = new ArrayList<String>();
					params.put(3, temp);
				}
				params.get(3).add(ret);
			}
			if (xpath.contains("dataview")) {
				String filter = xpath.substring(xpath.lastIndexOf("dataview[(@name=\""));
				String ret = filter.substring(17, filter.indexOf("\")"));
				if(!params.containsKey(4))
				{
					ArrayList<String> temp = new ArrayList<String>();
					params.put(4, temp);
				}
				params.get(4).add(ret);
			}
			if (xpath.contains("row")) {
				String filter = xpath.substring(xpath.lastIndexOf("row[(@name=\""));
				String ret = filter.substring(12, filter.indexOf("\")"));
				if(!params.containsKey(5))
				{
					ArrayList<String> temp = new ArrayList<String>();
					params.put(5, temp);
				}
				params.get(5).add(ret);
			}
			if (xpath.contains("cell")) {
				String filter = xpath.substring(xpath.lastIndexOf("cell[(@column=\""));
				String ret = filter.substring(15, filter.indexOf("\")"));
				if(!params.containsKey(6))
				{
					ArrayList<String> temp = new ArrayList<String>();
					params.put(6, temp);
				}
				params.get(6).add(ret);
			}
			}
			return params;
		}

}