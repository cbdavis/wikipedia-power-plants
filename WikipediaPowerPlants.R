#never ever ever convert strings to factors
options(stringsAsFactors = FALSE)

library(SPARQL) #get the data from DBpedia Live
library(sqldf) # create/work with a sqlite database

endpoint = "http://live.dbpedia.org/sparql"

# This works for the english language Wikipedia
# not sure yet how this works with other languages (different category structure?)
# What's going on here is that the query does a (massive) hierarchical category traversal, 
# starting with https://en.wikipedia.org/wiki/Category:Power_stations_by_country
# It keeps on following all of the subcategories as long as they contain text
# which is indicated in the regex statements below
queryString = 'select ?wikiPageID as ?pageID max(?wikiPageRevisionID) as ?revisionID where {
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
                } group by ?wikiPageID'


# If we get beyond 10,000 articles, then need to modify the query above to use OFFSET and LIMIT
# in order to download in blocks (due to restrictions in place on the SPARQL endpoint)
queryResults = SPARQL(url=endpoint, query=queryString, format='csv', extra=list(format='text/csv'))
data = queryResults$results

# sometimes 

# make a sqlite database to store all of this
db <- dbConnect(SQLite(), dbname="WikipediaPowerPlants.sqlite")

if (length(dbListTables(db)) == 0){
  # need to create table, otherwise just work with existing table and update it
  data$latitude=NA
  data$longitude=NA
  data$pageText = ""
  data$language = ""
  data$timeStamp = ""
  data$title = ""

  # should create directories for different languages as well
  dir.create("./API_Responses/en/", recursive=TRUE)
  
  options(HTTPUserAgent = "powerplantbot")
  for (i in c(1:nrow(data))){
    exportURL = paste("https://en.wikipedia.org/w/api.php?action=query&pageids=",data$pageID[i],"&prop=coordinates|revisions|langlinks&rvprop=ids|timestamp|user|comment|content&format=xml&redirects", sep="")

    destfile = paste("./API_Responses/en/", data$pageID[i], ".xml", sep="")
    if (!file.exists(destfile)){
      # save the file to disk as a backup
      download.file(exportURL, destfile, method="curl")
    }
    
    # also write the raw text to the data frame, so we can dump this into the sqlite database later
    data$APIResponseText[i] = readChar(destfile, file.info(destfile)$size)

    doc <- xmlTreeParse(destfile, useInternalNodes=TRUE)  
    pages <- getNodeSet(doc, "//page", addFinalizer=FALSE)
    # should only be one page per response
    for (page in pages){
      # make sure that the page isn't missing (not sure why this happens)
      if (!"missing" %in% names(xmlAttrs(page))){
        data$language[i] = "en"
        data$timeStamp[i] = unlist(getNodeSet(page, "./revisions/rev/@timestamp", addFinalizer=FALSE))[[1]]
        data$pageText[i] = xmlValue(getNodeSet(page, "./revisions/rev", addFinalizer=FALSE)[[1]])
        data$title[i] = as.character(getNodeSet(page, "./@title", addFinalizer=FALSE)[[1]])
        
        # DBpedia supplies us with the revision ID, but we overwrite it here
        # This is because this should be the absolute latest version
        # if in a later query we get a higher number, then we know that we need to update.
        data$revisionID[i] = as.character(getNodeSet(page, "./revisions/rev/@revid", addFinalizer=FALSE)[[1]])
        
        lat = unlist(getNodeSet(page, "./coordinates/co/@lat", addFinalizer=FALSE))[[1]]
        lon = unlist(getNodeSet(page, "./coordinates/co/@lon", addFinalizer=FALSE))[[1]]
        # TODO will need to deal with "List of " power stations pages - extract the data from the table rows.
        
        
        if (!is.null(lat) && !is.null(lon)){
          data$latitude[i] = lat
          data$longitude[i] = lon
        }
      }
    }
    free(doc)
    rm(doc)
  }
  dbWriteTable(db, "PowerPlantArticles", data, overwrite=TRUE)
} else { # database is already initialized, look for new stuff
  oldData = sqldf("select wikipedia, pageID, revisionID from PowerPlantArticles", dbname = "WikipediaPowerPlants.sqlite")
  
  # see if we have any new articles 
  # TODO need to fill this in, no new pages detected yet.
  newPageIDs = setdiff(data$pageID, oldData$pageID)
  
  # see if we have any new revisions
  # need to compare pageID and revID - can just do a merge
  mergedData = merge(oldData, data, by="pageID")
  # find cases where the recent data queried from DBpedia has a higher revision number that what we have
  locs = which(mergedData$revisionID.x < mergedData$revisionID.y)
  
  
  
}
