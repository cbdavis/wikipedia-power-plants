# -*- coding: utf-8 -*-

import sqlite3 as lite
import os
import sys
import urllib2
from lxml import etree
from lxml.etree import tostring
from SPARQLWrapper import SPARQLWrapper, JSON

def createWikipediaAPIRequestURL( pageID ):
  return "https://en.wikipedia.org/w/api.php?action=query&pageids=" + str(pageID) + "&prop=coordinates|revisions|langlinks&rvprop=ids|timestamp|user|comment|content&format=xml&redirects"


def downloadWikipediaAPIResponseData(pageID, language="en", overwrite=False):
    exportURL = createWikipediaAPIRequestURL(pageID)
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
def parseAPIResponse(filePath):
    tree = etree.parse(filePath)
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
        if overwrite == True:
            #print "deleting old values"
            cursor.execute("DELETE FROM PowerPlantArticles WHERE pageID = " + str(articleData['pageID']) + " AND language = '" + articleData['language'] + "'")
        cursor.execute("INSERT INTO PowerPlantArticles VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [articleData['pageID'], articleData['revisionID'], articleData['title'], articleData['language'], articleData['timeStamp'], articleData['latitude'], articleData['longitude'], articleData['pageText']])


def main():    
    try:
        con = lite.connect('WikipediaPowerPlants.db')
    
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
            

main()