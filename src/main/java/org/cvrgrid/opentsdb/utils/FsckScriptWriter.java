package org.cvrgrid.opentsdb.utils;
/* Copyright 2015 Cardiovascular Research Grid
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 *	All rights reserved
 * 	
 * 	@author Stephen J Granite (Email: sgranite@jhu.edu)
 */



/*
 * This is the main class to generate an OpenTSDB fsck script, based upon all the variables in an 
 * OpenTSDB instance.
 * 
 * The tool itself is a Java command line tool, intended to be placed in a service job that runs at a
 * specific interval.  The tool does not need to be co-located with the data, as the server.properties
 * specifies the path information for the data.  The tool begins by using the OpenTSDB suggest API to
 * retrieve all the variable names in the instance.  Once all the variables are in an array, the tool
 * then cycles through the array, generating the contents of the script with one line for each variable.
 * The tool then writes the script in the location specified by the processedFile variable in the
 * server.properties file.
 * 
 * The tool requires the CVRG OpenTSDB client to work with OpenTSDB.  The dependency is stored in the 
 * pom.xml.
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.cvrgrid.opentsdb.utils.model.OpenTSDBConfiguration;

import edu.jhu.cvrg.timeseriesstore.exceptions.OpenTSDBException;
import edu.jhu.cvrg.timeseriesstore.opentsdb.TimeSeriesUtility;


public class FsckScriptWriter { 

	private String configFilename = "/resources/server.properties";
	private OpenTSDBConfiguration openTSDBConfiguration = new OpenTSDBConfiguration();

	/**
	 * Constructor for this code intended to set all the variables based upon the properties file.
	 */
	public FsckScriptWriter(){		 

		try {

			OpenTSDBConfiguration openTSDBConfiguration = new OpenTSDBConfiguration();
			Properties serverProperties = new Properties();
			InputStream stream = FsckScriptWriter.class.getResourceAsStream(this.getConfigFilename());
			serverProperties.load(stream);
			openTSDBConfiguration.setOpenTSDBUrl(serverProperties.getProperty("openTSDBUrl"));
			openTSDBConfiguration.setApiPut(serverProperties.getProperty("apiPut"));
			openTSDBConfiguration.setApiQuery(serverProperties.getProperty("apiQuery"));
			openTSDBConfiguration.setAwareSupportedParams(serverProperties.getProperty("awareSupportedParams"));
			openTSDBConfiguration.setIdMatch(serverProperties.getProperty("idMatch"));
			openTSDBConfiguration.setIdMatchSheet(serverProperties.getProperty("idMatchSheet"));
			openTSDBConfiguration.setProcessedFile(serverProperties.getProperty("processedFile"));
			openTSDBConfiguration.setRootDir(serverProperties.getProperty("rootDir"));
			openTSDBConfiguration.setFolderPath(serverProperties.getProperty("folderPath"));
			openTSDBConfiguration.setStudyString(serverProperties.getProperty("studyString"));
			this.setOpenTSDBConfiguration(openTSDBConfiguration);

		} catch (IOException e) {

			e.printStackTrace();

		}

	}

	public static void main(String[] args) throws Exception {

		FsckScriptWriter fsckScriptWriter = new FsckScriptWriter();
		OpenTSDBConfiguration openTSDBConfiguration = fsckScriptWriter.getOpenTSDBConfiguration();
		String urlString = openTSDBConfiguration.getOpenTSDBUrl() + openTSDBConfiguration.getApiQuery();
		HttpURLConnection conn = buildConnection(urlString, "GET");
		StringBuilder sb = new StringBuilder();  

		try {

			int HttpResult = conn.getResponseCode(); 

			if(HttpResult == HttpURLConnection.HTTP_OK){

				BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(),"utf-8"));  

				String line = null;  

				while ((line = br.readLine()) != null) {  
					sb.append(line + "\n");  
				}  

				br.close();  

			} else {

				if(HttpResult > 301 || HttpResult == 0){
					throw new OpenTSDBException(conn.getResponseCode(), urlString, urlString + "\n");
				}

			}  

			String metrics = sb.toString();
			metrics = metrics.replaceAll("\\[", "");
			metrics = metrics.replaceAll("\\]", "");
			metrics = metrics.replaceAll("\"", "");
			metrics = metrics.replaceAll("\n", "");
			String[] metricsArray = metrics.split(",");
			PrintWriter writer = new PrintWriter(openTSDBConfiguration.getProcessedFile(), "UTF-8");

			for (String metric : metricsArray) {
				writer.println(openTSDBConfiguration.getRootDir() + "/build/tsdb fsck 2014/07/01 sum " + metric + " --fix");
			}

			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (OpenTSDBException e) {
			e.printStackTrace();
		}


	}

	private static HttpURLConnection buildConnection(String urlString, String method) {
		URL url;
		try {
			url = new URL(urlString);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod(method);
			connection.setRequestProperty("Accept", "application/json");
			connection.setRequestProperty("Content-type", "application/json");
			if (method.equalsIgnoreCase("post")) {
				connection.setDoOutput(true);
			} else {
				connection.setDoInput(true);
			}
			return connection;
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * @return the configFilename
	 */
	public String getConfigFilename() {
		return configFilename;
	}


	/**
	 * @return the openTSDBConfiguration
	 */
	public OpenTSDBConfiguration getOpenTSDBConfiguration() {
		return openTSDBConfiguration;
	}


	/**
	 * @param openTSDBConfiguration the openTSDBConfiguration to set
	 */
	public void setOpenTSDBConfiguration(OpenTSDBConfiguration openTSDBConfiguration) {
		this.openTSDBConfiguration = openTSDBConfiguration;
	}

}