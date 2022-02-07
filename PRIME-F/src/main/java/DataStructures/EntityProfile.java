/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */

package DataStructures;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import NewsAPI.R7API;
import TwitterApi.TwitterAPI;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;
import tokens.NLPutils;

/**
 *
 * @author gap2
 */
public class EntityProfile implements Serializable {

	private static final long serialVersionUID = 122354534453243447L;

	private Set<Attribute> attributes;
	private final String entityUrl;
	private boolean isSource;
	private int key;
	private int incrementID;
	private Timestamp creation;
	private Set<Integer> setOfTokens;
	
	//Used only to case study
	private String id;

	public Object[] getAllValues() {
	    return new Object[]{key, entityUrl, isSource, creation};
	}
	
	private final String split1 = "<<>>";
	private final String split2 = "#=#";

//	public EntityProfile(String url) {
//		entityUrl = url;
//		attributes = new HashSet();
//	}

	/**
	 * Parse out of the format found in generated CSV files
	 * 
	 * @param csvLine
	 *            A comma separated key and values
	 * @throws IllegalArgumentException
	 */
	public EntityProfile(String csvLine, String separator) {
		String[] parts = csvLine.split(separator);
		key = Integer.valueOf(parts[0]);
		entityUrl = parts[1];
		creation = new Timestamp(System.currentTimeMillis());
		attributes = new HashSet();
		for (int i = 1; i < parts.length; i++) {//the first element is the key (avoid!)
			attributes.add(new Attribute("", parts[i]));
		}
	}
	
//	public EntityProfile() {
//		key = -1;
//		entityUrl = "NONE";
//		creation = new Timestamp(System.currentTimeMillis());
//		attributes = new HashSet();
//	}
	
	public EntityProfile(String standardFormat, boolean isNoisy) {
		KeywordGenerator kw = new KeywordGeneratorImpl();
		String[] parts = standardFormat.split(split1);
		isSource = Boolean.parseBoolean(parts[0]);
		entityUrl = parts[1];
		key = Integer.valueOf(parts[2]);
		if (!parts[3].equalsIgnoreCase("incrementID")) {
			incrementID = Integer.valueOf(parts[3]);
		}
		creation = new Timestamp(System.currentTimeMillis());
		attributes = new HashSet();
		setOfTokens = new HashSet<Integer>();
		for (int i = 4; i < parts.length; i++) {//the first element is the key (avoid!)
			String[] nameValue = parts[i].split(split2);
			if (nameValue.length == 1) {
				attributes.add(new Attribute(nameValue[0], ""));
			} else {
				attributes.add(new Attribute(nameValue[0], nameValue[1]));
				if (isNoisy) {
					setOfTokens.addAll(generateTokensFromHash(nameValue[1]));
				} else {
					setOfTokens.addAll(generateTokens(nameValue[1]));
				}
				
			}
		}
	}

	public EntityProfile(Set<Attribute> attributes) {
		this.attributes = attributes;
		this.entityUrl = "";
	}
	
	//used to case study
	public EntityProfile(R7API newsApi, boolean isNoisy) {
		setOfTokens = new HashSet<Integer>();
		entityUrl = null;
		KeywordGenerator kw = new KeywordGeneratorImpl();
		isSource = true;
		id = newsApi.getId();
		if (isNoisy) {
			for (String token : newsApi.getAllHashTokens(true)) {
				if (!token.isEmpty()) {
					setOfTokens.add(Integer.parseInt(token));
				}
			}
		} else {
			for (String token : newsApi.getAllTokens(true)) {
				if (!token.isEmpty()) {
					setOfTokens.add(token.hashCode());
				}
			}
		}
	}
	//used to case study
	public EntityProfile(TwitterAPI twitterApi, boolean isNoisy) {
		setOfTokens = new HashSet<Integer>();
		entityUrl = null;
		KeywordGenerator kw = new KeywordGeneratorImpl();
		isSource = false;
		id = String.valueOf(twitterApi.getId());
		if (isNoisy) {
			for (String token : twitterApi.getAllHashTokens(true)) {
				if (!token.isEmpty()) {
					setOfTokens.add(Integer.parseInt(token));
				}
			}
		} else {
			for (String token : twitterApi.getAllTokens(true)) {
				if (!token.isEmpty()) {
					setOfTokens.add(token.hashCode());
				}
			}
		}
	}
	
	public String getId() {
		return id;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public void addAttribute(String propertyName, String propertyValue) {
		attributes.add(new Attribute(propertyName, propertyValue));
	}


	public String getEntityUrl() {
		return entityUrl;
	}

	public int getProfileSize() {
		return attributes.size();
	}

	public Set<Attribute> getAttributes() {
		return attributes;
	}

	public String getStandardFormat() {
		String output = "";
		output += isSource + split1;
		output += entityUrl + split1;//separate the attributes
		output += key + split1;//separate the attributes
		output += incrementID + split1;//separate the attributes
		
		for (Attribute attribute : attributes) {
			output += attribute.getName() + split2 + attribute.getValue() + split1;
		}
				
		return output;
	}
	
	public String getStandardFormatCleaned() {
		KeywordGenerator kw = new KeywordGeneratorImpl();
		String output = "";
		output += isSource + split1;
		output += entityUrl + split1;//separate the attributes
		output += key + split1;//separate the attributes
		output += incrementID + split1;//separate the attributes
		
		for (Attribute attribute : attributes) {
			String standardString = standardString(attribute.getValue());
			Set<String> keys = kw.generateKeyWords(standardString);
			output += attribute.getName() + split2 + NLPutils.cleanValues2(keys) + split1;
			
//			output += attribute.getName() + split2 + NLPutils.cleanValues(attribute.getValue()) + split1;
		}
				
		return output;
	}
	
	
	public String getStandardFormat2() {
		String output = "";
		output += isSource + split1;
		output += entityUrl + split1;//separate the attributes
		output += key + split1;//separate the attributes
		output += "incrementID" + split1;//separate the attributes
		
		for (Attribute attribute : attributes) {
			output += attribute.getName() + split2 + attribute.getValue() + split1;
		}
				
		return output;
	}
	
//	public String getStandardFormat2() {
//		String output = "";
//		output += isSource + split1;
////		output += entityUrl + split1;//separate the attributes
//		output += key + split1;//separate the attributes
//		
//		for (Attribute attribute : attributes) {
//			output += attribute.getValue() + " ";
//		}
//				
//		return output;
//	}


	public boolean isSource() {
		return isSource;
	}


	public void setSource(boolean isSource) {
		this.isSource = isSource;
	}

	public Timestamp getCreation() {
		return creation;
	}

	public void setCreation(Timestamp creation) {
		this.creation = creation;
	}
	
	public int getIncrementID() {
		return incrementID;
	}

	public void setIncrementID(int incrementID) {
		this.incrementID = incrementID;
	}
	
	public Set<Integer> getSetOfTokens() {
		return setOfTokens;
	}

	public void setSetOfTokens(Set<Integer> setOfTokens) {
		this.setOfTokens = setOfTokens;
	}

	private Set<Integer> generateTokens(String string) {
		if (string.length() > 20 && string.substring(0, 20).equals("<http://dbpedia.org/")) {
			String[] uriPath = string.split("/");
			string = Arrays.toString(uriPath[uriPath.length-1].split("[\\W_]")).toLowerCase();
		}
		
		//To improve quality, use the following code
		String standardString = standardString(string);
		
		KeywordGenerator kw = new KeywordGeneratorImpl();
		return kw.generateKeyWordsHashCode(standardString);
	}
	
	private String standardString(String string) {
		Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
		Matcher m = p.matcher("");
		m.reset(string);
		return m.replaceAll("");
	}
	
	private Set<Integer> generateTokensFromHash(String string) {
		Set<Integer> output = new HashSet<Integer>();
		for (String value : string.split(" ")) {
			output.add(Integer.parseInt(value));
		}
		return output;
	}
	
	private String[] generateTokensForBigData(String string) {
		if (string.length() > 20 && string.substring(0, 20).equals("<http://dbpedia.org/")) {
			String[] uriPath = string.split("/");
			string = uriPath[uriPath.length-1];
		}
		
		return string.split("[\\W_]");
	}

	@Override
	public String toString() {
		return "" + getKey() + "-" + isSource();
	}

}