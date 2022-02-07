package TwitterApi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import DataStructures.TokenExtractor;
import FootballApi.PlayerAPI;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;
import tokens.NLPutils;

public class TwitterAPI implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8101742696072028239L;
	private Long id;
	private String createdAt;
	private String text;
	private String fullText;
	private String userName;
	private String userDescription;
	private String hashtags;
	private String userScreenName;
	private String citedUsers;
	
	private final String split1 = "<<>>";
	private final String split2 = "#=#";
	
	
	public TwitterAPI(Long id, String createdAt, String text, String fullText, String userName, String userDescription,
			String hashtags) {
		super();
		this.id = id;
		this.createdAt = createdAt;
		this.text = text;
		this.fullText = fullText;
		this.userName = userName;
		this.userDescription = userDescription;
		this.hashtags = hashtags;
	}
	
	public TwitterAPI(String stringStandardFormat) {
		String[] splitedString = stringStandardFormat.split(split1);
		this.id = Long.parseLong(splitedString[0].trim());
		this.createdAt = splitedString[1].trim();
		this.text = splitedString[2].trim();
		this.fullText = splitedString[3].trim();
		this.userName = splitedString[4].trim();
		this.userScreenName = splitedString[5].trim();
		this.userDescription = splitedString[6].trim();
		this.hashtags = splitedString[7].trim();
		this.citedUsers = splitedString[8].trim();
	}


	public String getCreatedAt() {
		return createdAt;
	}


	public void setCreatedAt(String createdAt) {
		this.createdAt = createdAt;
	}


	public String getText() {
		return text;
	}


	public void setText(String text) {
		this.text = text;
	}


	public String getFullText() {
		return fullText;
	}


	public void setFullText(String fullText) {
		this.fullText = fullText;
	}


	public String getUserName() {
		return userName;
	}


	public void setUserName(String userName) {
		this.userName = userName;
	}


	public String getUserDescription() {
		return userDescription;
	}


	public void setUserDescription(String userDescription) {
		this.userDescription = userDescription;
	}


	public String getHashtags() {
		return hashtags;
	}


	public void setHashtags(String hashtags) {
		this.hashtags = hashtags;
	}


	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getUserScreenName() {
		return userScreenName;
	}

	public void setUserScreenName(String userScreenName) {
		this.userScreenName = userScreenName;
	}

	public String getCitedUsers() {
		return citedUsers;
	}

	public void setCitedUsers(String citedUsers) {
		this.citedUsers = citedUsers;
	}

	public Set<String> getAllTokens(boolean useAttSelection) {
		Set<String> tokens = new HashSet<String>();
		
		tokens.addAll(TokenExtractor.generateTokens(getText())); //if text is empty, full_text has content ... and vice and versa
		tokens.addAll(TokenExtractor.generateTokens(getFullText()));
		tokens.addAll(TokenExtractor.generateTokens(getHashtags()));
//		if (!getHashtags().isEmpty()) {
//			System.out.println(getHashtags());
//		}
		
		
		if (!useAttSelection) {//just applied without att selection
			tokens.addAll(TokenExtractor.generateTokens(getUserName()));
			tokens.addAll(TokenExtractor.generateTokens(getUserScreenName()));
			tokens.addAll(TokenExtractor.generateTokens(getCitedUsers()));
			tokens.addAll(TokenExtractor.generateTokens(getCreatedAt()));
		}
		
//		tokens.addAll(TokenExtractor.generateTokens(getUserDescription()));
//		for (String tag : hashtags) {
//			tokens.addAll(TokenExtractor.generateTokens(tag));
//		}
		
		
		return tokens;
	}
	
	public Set<String> getAllHashTokens(boolean useAttSelection) {
		Set<String> tokens = new HashSet<String>();
		
		tokens.addAll(getHashTokens(getText())); //if text is empty, full_text has content ... and vice and versa
		tokens.addAll(getHashTokens(getFullText()));
		tokens.addAll(getHashTokens(getHashtags()));

		if (!useAttSelection) {//just applied without att selection
			tokens.addAll(getHashTokens(getUserName()));
			tokens.addAll(getHashTokens(getUserScreenName()));
			tokens.addAll(getHashTokens(getCitedUsers()));
			tokens.addAll(getHashTokens(getCreatedAt()));
		}
		return tokens;
	}
	
	private Set<String> getHashTokens(String value) {
		Set<String> output = new HashSet<String>();
		if (!value.trim().isEmpty()) {
			output.addAll(Arrays.asList(value.split(" ")));
		}
		
		return output;
	}
	
	@Override
	public String toString() {
		return createdAt + "\n" + (text.equals("") ? fullText : text);
	}
	
	@Override
	public boolean equals(Object obj) {
		return id.equals(((TwitterAPI)obj).getId());
	}

	public String getStandardFormat() {
		String split1 = " <<>> ";
		return id + split1 + createdAt + split1 + text + split1 + fullText + split1 + userName + split1 + userScreenName + split1 + userDescription + split1 + hashtags + split1 + citedUsers;
	}
	
	public String getStandardFormatHash() {
		String split1 = " <<>> ";
		KeywordGenerator kw = new KeywordGeneratorImpl();
		return id + split1 + NLPutils.cleanValues2(kw.generateKeyWords(createdAt)) + split1 + NLPutils.cleanValues2(kw.generateKeyWords(text)) + split1 + NLPutils.cleanValues2(kw.generateKeyWords(fullText)) + split1 + NLPutils.cleanValues2(kw.generateKeyWords(userName)) + split1 + NLPutils.cleanValues2(kw.generateKeyWords(userScreenName)) + split1 + NLPutils.cleanValues2(kw.generateKeyWords(userDescription)) + split1 + NLPutils.cleanValues2(kw.generateKeyWords(hashtags)) + split1 + NLPutils.cleanValues2(kw.generateKeyWords(citedUsers));
	}

}
