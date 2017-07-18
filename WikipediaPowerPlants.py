# -*- coding: utf-8 -*-

import sqlite3 as lite
import os
import sys
import urllib2
from urllib2 import quote
from lxml import etree
from lxml.etree import tostring
from SPARQLWrapper import SPARQLWrapper, JSON

# these are all the bits we want to grab when getting information about particular pages
def getAPIRequestProperties():
    return "&prop=coordinates|revisions|langlinks&rvprop=ids|timestamp|user|comment|content&format=xml&redirects"

def createWikipediaAPIRequestURLForPageID(pageID, language="en"):
    return "https://" + language + ".wikipedia.org/w/api.php?action=query&pageids=" + str(pageID) + getAPIRequestProperties()

def createWikipediaAPIRequestURLForTitle(title, language="en"):
    return "https://" + language + ".wikipedia.org/w/api.php?action=query&titles=" + title + getAPIRequestProperties()

def downloadWikipediaAPIResponseData(pageID, language="en", overwrite=False):
    exportURL = createWikipediaAPIRequestURLForPageID(pageID)
    destfile = "./API_Responses/" + language + "/" + str(pageID) + ".xml"
    if os.path.isfile(destfile) == False or overwrite == True:
        print destfile
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

#### !!!!! This assumes that only a single page is returned in the API response !!!! #####
# you could be smart and group multiple page requests into one API call
# This function returns a dictionary for a single row in the db table 
# Dealing with multiple pages would require changing the data structure returned
# and updating the calling function to use something like cursor.executemany
def parseAPIResponse(filePath):
    
    utf8_parser = etree.XMLParser(encoding='utf-8')    
    tree = etree.parse(filePath, parser=utf8_parser)
    # check what's in the xml file:     
    #tostring(tree)

    # make sure that we actually have data - 
    # the page may have been deleted by weird people who don't think that power plants are notable :(
    missing = tree.xpath('//page/@missing')
    if len(missing) == 0:
    
        pageID = int(tree.xpath('//page/@pageid')[0])
        revisionID = int(tree.xpath('//revisions/rev/@revid')[0])
      
        lat = tree.xpath('//coordinates/co/@lat')
        lon = tree.xpath('//coordinates/co/@lon')
    
        if len(lat) > 0 and len(lon) > 0:
            lat = float(lat[0])
            lon = float(lon[0])
        else:
            lat = float('nan')
            lon = float('nan')
            
        pageText = tree.xpath('//revisions/rev/text()')[0]
        language = "en"      
        timeStamp = tree.xpath('//revisions/rev/@timestamp')[0]
        title = tree.xpath('//@title')[0]

        langlinks = tree.xpath('//langlinks/ll')
        for langlink in langlinks:
            otherLanguage = langlink.xpath('@lang')[0]
            titleOtherLang = langlink.text
            # go from unicode to URL encoding
            titleOtherLang = quote(titleOtherLang.encode('utf-8'))

            # try to download this page if we don't have it already
            createWikipediaAPIRequestURLForTitle(titleOtherLang, otherLanguage)
            
            # TODO set up code to download - this issue is that files are saved according
            # to pageID.  With the language links, we don't have the pageID yet, but we have the titles            
            # At this point, we don't know the latest revision of the page either, 
            # should just overwrite whatever we have since we have to look at the page anyway.
            # This really should be thought through a bit more
            # We can also just ping the API to get the latest revs for all the non-english pages
            # this would be quite efficient and could be done in blocks of pageIDs
    
        return {'pageID':pageID, 'revisionID':revisionID, 'title':title, 'language':language, 'timeStamp':timeStamp, 'latitude':lat, 'longitude':lon, 'pageText':pageText}
    else:
        print "missing page data for " + filePath
        return 0

def downloadDataAndInsertIntoDatabase(pageID, cursor, overwrite=False):
    # download the data                    
    filePath = downloadWikipediaAPIResponseData(pageID, overwrite=overwrite)
    # parse the response into a list
    articleData = parseAPIResponse(filePath)
    # dump the data as a row in the database table
    if articleData != 0: # make sure that we have valid data
        # if overwrite == True, then want to overwrite the old row in the database
        #if overwrite == True:
        #print "deleting old values"
        # always delete before inserting
    
        if articleData['pageID'] == pageID:
            print filePath
            cursor.execute("DELETE FROM PowerPlantArticles WHERE pageID = " + str(articleData['pageID']) + " AND language = '" + articleData['language'] + "'")
            cursor.execute("INSERT INTO PowerPlantArticles VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [articleData['pageID'], articleData['revisionID'], articleData['title'], articleData['language'], articleData['timeStamp'], articleData['latitude'], articleData['longitude'], articleData['pageText']])

def main():    
    
    defaultDirectory = "./API_Responses/en/"
    if not os.path.exists(defaultDirectory):
        os.makedirs(defaultDirectory)    
    
    try:
        # isolation_level enables auto commit http://stackoverflow.com/questions/22488763/sqlite-insert-query-not-working-with-python
        con = lite.connect('WikipediaPowerPlants.db', isolation_level=None)
    
        # This works for the english language Wikipedia
        # What's going on here is that the query does a (massive) hierarchical category traversal, 
        # starting with https://en.wikipedia.org/wiki/Category:Power_stations_by_country
        # It keeps on following all of the subcategories as long as they contain text
        # which is indicated in the regex statements below
    
        con.row_factory = lite.Row
        cur = con.cursor()    
        sql = 'create table if not exists PowerPlantArticles (pageID INTEGER, revisionID INTEGER, title TEXT, language TEXT, timeStamp TEXT, latitude REAL, longitude REAL, pageText TEXT)'
        cur.execute(sql)        

        results = queryDBpedia()
        
        print str(len(results["results"]["bindings"])) + " results returned from DBpedia SPARQL query"

        for result in results["results"]["bindings"]:
            pageID = int(result["pageID"]["value"])
            revisionID = int(result["revisionID"]["value"])
            
            cur.execute("SELECT * FROM PowerPlantArticles WHERE pageID=" + str(pageID))
            rows = cur.fetchall()                
            
            if (len(rows) == 0) :
                #  we don't have an entry in the database for the pageID, so...
                downloadDataAndInsertIntoDatabase(pageID, cur, overwrite=False)
            else:
                articleDataFromDB = rows[0] # assume only one entry in the DB - this may be stupid.
                if int(articleDataFromDB["revisionID"]) < int(revisionID) :
                    print "For page " + str(pageID) + ", local db at reversionID " + str(articleDataFromDB["revisionID"]) + ", DBpedia Live mentions version " + str(revisionID)
                    downloadDataAndInsertIntoDatabase(pageID, cur, overwrite=True)
        
    except lite.Error, e:
        
        print "Error %s:" % e.args[0]
        sys.exit(1)
        
    finally:        
        con.commit()
        cur.close()
        con.close()
            

# change to the directory which this code is in
os.chdir(sys.path[0])
main()

##### SQL queries for debugging
# Check for duplicate entries per pageID - setting up a primary key based on pageID and language would be a smart idea.
#SELECT pageID, COUNT(*) c FROM PowerPlantArticles GROUP BY pageID HAVING c > 1;
# see what's going on with a single pageID
#select * FROM PowerPlantArticles WHERE pageID=42880740;
