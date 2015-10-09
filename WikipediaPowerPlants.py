# -*- coding: utf-8 -*-

import sqlite3 as lite
import os
import urllib2
from SPARQLWrapper import SPARQLWrapper, JSON

def createWikipediaAPIRequestURL( pageID ):
  return "https://en.wikipedia.org/w/api.php?action=query&pageids=" + str(pageID) + "&prop=coordinates|revisions|langlinks&rvprop=ids|timestamp|user|comment|content&format=xml&redirects"


def downloadWikipediaAPIResponseData(pageID, language="en", overwrite=False):
    exportURL = createWikipediaAPIRequestURL(pageID)
    destfile = "./API_Responses/" + language + "/" + str(pageID) + ".xml"
    print destfile
    if os.path.isfile(destfile) == False:
        print "not ok"
        req = urllib2.Request(exportURL, headers={ 'User-Agent': 'Powerplant bot https://github.com/cbdavis/wikipedia-power-plants' })
        xml = urllib2.urlopen(req).read()
        
        text_file = open(destfile, "w")
        text_file.write(xml)
        text_file.close()
    return destfile

def queryDBpedia():
    sparql = SPARQLWrapper("http://live.dbpedia.org/sparql")
    sparql.setQuery("""
        select ?wikiPageID as ?pageID max(?wikiPageRevisionID) as ?revisionID where {
                        ?subCat skos:broader+ dbc:Power_stations_by_country . 
                        ?subCat rdfs:label ?categoryName .
                        FILTER (regex(?categoryName, "power station", "i") ||
                        regex(?categoryName, "power plant", "i") ||
                        regex(?categoryName, "CHP plants", "i") ||
                        regex(?categoryName, "Wave farms", "i") ||
                        regex(?categoryName, "Wind farms", "i")) .
                        ?plant dct:subject ?subCat .
                        ?plant dbo:wikiPageID ?wikiPageID . 
                        ?plant dbo:wikiPageRevisionID ?wikiPageRevisionID . 
                        ?plant foaf:isPrimaryTopicOf ?wikipedia . 
                    } group by ?wikiPageID
    """)
    
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results

def main():    
    try:
        con = lite.connect('WikipediaPowerPlants.db')
    
        # This works for the english language Wikipedia
        # What's going on here is that the query does a (massive) hierarchical category traversal, 
        # starting with https://en.wikipedia.org/wiki/Category:Power_stations_by_country
        # It keeps on following all of the subcategories as long as they contain text
        # which is indicated in the regex statements below
    
        #   do we have a table existing?
    
        with con:    
            cur = con.cursor()    
            cur.execute("CREATE TABLE Cars(Id INT, Name TEXT, Price INT)")    
    
            sql = 'create table if not exists PowerPlantArticles (pageID INTEGER, revisionID INTEGER, latitude REAL, longitude REAL, pageText TEXT, language TEXT, timeStamp TEXT, title TEXT, APIResponseText TEXT)'
            cur.execute(sql)        
    
            results = queryDBpedia()
            for result in results["results"]["bindings"]:
                pageID = result["pageID"]["value"]
                revisionID = result["revisionID"]["value"]
                
                # either create new database table if it doesn't exist
                # or check for updates to existing data 
                
                downloadWikipediaAPIResponseData(pageID)
                
        
    except lite.Error, e:
        
        print "Error %s:" % e.args[0]
        sys.exit(1)
        
    finally:
        
        if con:
            con.close()
            

main()