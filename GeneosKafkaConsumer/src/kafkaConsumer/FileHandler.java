package kafkaConsumer;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Properties;


/*
* Create by: 	Connor Morley
* Date: 		April 2016
* Title: 		Error Scraper Application
* Version:		1.5
*/

public class FileHandler {
	
	public static Map<String, String> setting = new HashMap<String,String>();

	public static void readSettingsFile() throws InterruptedException {
		Properties prop = new Properties();
		InputStream input = null;
		int varCount = 5; // Default number of entries without maintenance roll back

		try {
			String file = "";
			if (System.getProperty("os.name").contains("Windows"))
				file = ".\\settings.properties";
			if (System.getProperty("os.name").contains("Linux"))
				file = "./settings.properties";
			input = new FileInputStream(file);

			prop.load(input);

			setting.put("kafkaServer", prop.getProperty("kafkaServer"));
			setting.put("groupID", prop.getProperty("groupID"));
			setting.put("topic", prop.getProperty("topic"));
			setting.put("xpaths", prop.getProperty("xpaths"));
			setting.put("timeZone", prop.getProperty("timeZone"));
			if (prop.getProperty("seekOffset") != null) {
				setting.put("seekOffset", prop.getProperty("seekOffset"));
				varCount = 6; // If roll back is present increase the variable number check
			}
			System.out.println(setting);
			if (setting.size() != varCount || setting.containsValue(null)) {
				System.exit(0);
			}
			configureSetting();
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
	
	public static void configureSetting()
	{
		KafkaForGeneos.kafkaServer = setting.get("kafkaServer");
		KafkaForGeneos.groupID = setting.get("groupID");
		KafkaForGeneos.topics = setting.get("topic");
		KafkaForGeneos.timeZone = setting.get("timeZone");
		String[] paths = setting.get("xpaths").split(",");
		for(String path : paths)
		{
			KafkaForGeneos.xpaths.add(path);
		}
		if(setting.size() == 6)
			KafkaForGeneos.seekOffset = setting.get("seekOffset");
	}
}
