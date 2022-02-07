package NewsAPI;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import DataStructures.TokenExtractor;

public class R7API implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String id;
	private String title;
	private String title_detail;
	private String links;
	private String link;
	private String summary;
	private String summary_detail;
	private String published;
	private String published_parsed;
	
	private final String split1 = "<<>>";
	
	public R7API(String stringStandardFormat) {
		String[] splitedString = stringStandardFormat.split(split1);
		this.title = splitedString[0].trim();
		this.id = splitedString[1].trim();
		this.links = splitedString[2].trim();
		this.link = splitedString[3].trim();
		this.title_detail = splitedString[4].trim();
		this.summary = splitedString[5].trim();
		this.summary_detail = splitedString[6].trim();
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
	

	public String getTitle_detail() {
		return title_detail;
	}

	public void setTitle_detail(String title_detail) {
		this.title_detail = title_detail;
	}

	public String getSummary_detail() {
		return summary_detail;
	}

	public void setSummary_detail(String summary_detail) {
		this.summary_detail = summary_detail;
	}

	public Set<String> getAllTokens(boolean useAttSelection) {
		Set<String> tokens = new HashSet<String>();
		
		tokens.addAll(TokenExtractor.generateTokens(getTitle()));
		tokens.addAll(TokenExtractor.generateTokens(getSummary_detail()));
		
		if (!useAttSelection) {//just applied without att selection
			tokens.addAll(TokenExtractor.generateTokens(getPublished()));
			tokens.addAll(TokenExtractor.generateTokens(getLink()));
			tokens.addAll(TokenExtractor.generateTokens(getLinks()));
		}

//		tokens.addAll(TokenExtractor.generateTokens(getMedia_content()));
//		tokens.addAll(TokenExtractor.generateTokens(getPublished_parsed()));
//		tokens.addAll(TokenExtractor.generateTokens(getTags())); //sempre eh G1
		
		return tokens;
	}
	
}
