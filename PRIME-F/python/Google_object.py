# -*- coding: utf-8 -*-
import time
import extract_keywords.ExtractKeywords as extractor
from datetime import datetime

class Google_object(object):
    id, link, published, published_parsed, links, title, title_detail, summary, summary_detail = " ", " ", " ", " ", " ", " ", " ", " ", " "
    

    def __init__(self, dict_google_new):
        list_keys = dict_google_new.keys()
        if dict_google_new['title'] is not None:
            self.title = extractor.tokensByYake(self.removeTagGoogle(str(dict_google_new['title'])))
        
        if dict_google_new['title_detail'] is not None:
            self.title_detail = extractor.tokensByYake(str(dict_google_new['title_detail']))
            
        if dict_google_new['links'] is not None:
            for link in dict_google_new['links']:
                self.links = str(link['href'])
            
        if dict_google_new['link'] is not None:
            self.link = str(dict_google_new['link'])
            
        if dict_google_new['id'] is not None:
            self.id = str(dict_google_new['id'])
            
        if dict_google_new['summary'] is not None:
            self.summary = extractor.tokensByYake(str(dict_google_new['summary']))
            
        if dict_google_new['summary_detail'] is not None:
            self.summary_detail = extractor.tokensByYake(extractor.extractInfoFromHTML(str(dict_google_new['summary_detail']['value'])))
        
        if dict_google_new['published'] is not None:
            self.published = str(dict_google_new['published'])
        
        if dict_google_new['published_parsed'] is not None:
            self.published_parsed = time.strftime('%Y-%m-%dT%H:%M:%S', dict_google_new['published_parsed'])
     
    def formatToStandardString(self):
        split1 = "<<>>"
        data = (self.title + split1 + self.id + split1 + self.links + split1 + self.link + split1 + self.title_detail + split1 + self.summary + split1 + self.summary_detail + split1 + self.published + split1 + self.published_parsed)
        return data
    
    def toString(self):
        data = self.formatToStandardString().encode("utf-8")
        return data
    
    def __eq__(self, obj):
        return isinstance(obj, Google_object) and obj.id == self.id
    
    def __hash__(self, *args, **kwargs):
        return id.__hash__()
    
    def removeTagGoogle(self, text):
        list_s = text.split("-")
        cleaned_text = ""
        for i in range(0,len(list_s)-1):
            cleaned_text += list_s[i] + " "
        return cleaned_text
    
    def isPublishedToday(self):
        date_pub = datetime.strptime(self.published, '%a, %d %b %Y %H:%M:%S %Z')
        return date_pub.date() == datetime.today().date()
             