'''
Created on 22 de ago de 2019

@author: Brasileiro
'''
import extract_keywords.ExtractKeywords as extractor
#from bleach._vendor.html5lib.treebuilders.etree_lxml import tostring

class twitter_object(object):
    id, created_at, text, full_text, user_name, user_screen_name, user_description, hashtags, user_cited = " ", " ", " ", " ", " ", " ", " ", " ", " "


    def __init__(self, id_num, created_at, text, full_text, user_name, user_screen_name, user_description, hashtags, user_cited):
        if id_num is not None:
            self.id = id_num
        if created_at is not None:
            self.created_at = created_at
        self.text = extractor.tokensByYake(text)
        self.full_text = extractor.tokensByYake(full_text)
        self.user_name = user_name
        self.user_screen_name = user_screen_name
        self.user_description = user_description
        self.hashtags = hashtags
        self.user_cited = user_cited
     
    def formatToStandardString(self):
        split1 = "<<>>"
        
        user_cited_str = " "
        for user in self.user_cited:
            user_cited_str = user_cited_str + user['name'] + " " + user['screen_name']
            
        if self.user_description == None:
            self.user_description = " "
        
        return self.id + split1 + self.created_at + split1 + self.text + split1 + self.full_text + split1 + self.user_name + split1 + self.user_screen_name + split1 + self.user_description + split1 + self.hashtags + split1 + str(user_cited_str)
    
    def toString(self):
        split1 = "<<>>"
        
        user_cited_str = " "
        for user in self.user_cited:
            user_cited_str = user_cited_str + user['name'] + " " + user['screen_name']
            
        if self.user_description == None:
            self.user_description = " "
        
        data = self.id + split1 + self.created_at + split1 + self.text + split1 + self.full_text + split1 + self.user_name + split1 + self.user_screen_name + split1 + self.user_description + split1 + self.hashtags + split1 + str(user_cited_str)
        data = data.encode("utf-8")
        return data