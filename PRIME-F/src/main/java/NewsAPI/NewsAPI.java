package NewsAPI;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import DataStructures.TokenExtractor;

public class NewsAPI implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String id;
	private String title;
	private String links;
	private String link;
	private String summary;
	private String media_content;
	private String tags;
	private String published;
	private String published_parsed;
	
	private final String split1 = "<<>>";
	
	public NewsAPI(String stringStandardFormat) {
		String[] splitedString = stringStandardFormat.split(split1);
		this.title = splitedString[0].trim();
		this.id = splitedString[1].trim();
		this.links = splitedString[2].trim();
		this.link = splitedString[3].trim();
		this.summary = splitedString[4].trim();
		this.media_content = splitedString[5].trim();
		this.tags = splitedString[6].trim();
		this.published = splitedString[7].trim();
		this.published_parsed = splitedString[8].trim();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getLinks() {
		return links;
	}

	public void setLinks(String links) {
		this.links = links;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public String getMedia_content() {
		return media_content;
	}

	public void setMedia_content(String media_content) {
		this.media_content = media_content;
	}

	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	public String getPublished() {
		return published;
	}

	public void setPublished(String published) {
		this.published = published;
	}

	public String getPublished_parsed() {
		return published_parsed;
	}

	public void setPublished_parsed(String published_parsed) {
		this.published_parsed = published_parsed;
	}

	public Set<String> getAllTokens() {
		Set<String> tokens = new HashSet<String>();
		
//		tokens.addAll(TokenExtractor.generateTokens(getLink()));
//		tokens.addAll(TokenExtractor.generateTokens(getLinks()));
//		tokens.addAll(TokenExtractor.generateTokens(getMedia_content()));
//		tokens.addAll(TokenExtractor.generateTokens(getPublished()));
//		tokens.addAll(TokenExtractor.generateTokens(getPublished_parsed()));
//		tokens.addAll(TokenExtractor.generateTokens(getSummary()));
//		tokens.addAll(TokenExtractor.generateTokens(getTags())); //sempre eh G1
		tokens.addAll(TokenExtractor.generateTokens(getTitle()));
		
		return tokens;
	}
	
}
