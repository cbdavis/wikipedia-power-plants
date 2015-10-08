#never ever ever convert strings to factors
options(stringsAsFactors = FALSE)

library(SPARQL) #get the data from DBpedia Live
library(sqldf) # create/work with a sqlite database

createInsertStatement <- function(arrayData){
  header = "INSERT INTO PowerPlantArticles VALUES ("
  rawData = ""
  for (j in c(1:length(arrayData))){
    if (class(arrayData[i] == "character")){
      rawData = paste(rawData, '"', arrayData[i], '"')
    } else {
      
    }
  }
  footer = ")"
}


createWikipediaAPIRequestURL <- function(pageID){
  return(paste("https://en.wikipedia.org/w/api.php?action=query&pageids=",pageID,"&prop=coordinates|revisions|langlinks&rvprop=ids|timestamp|user|comment|content&format=xml&redirects", sep=""))
}

downloadWikipediaAPIResponseData <- function(pageID, language="en", overwrite=FALSE){
  exportURL = createWikipediaAPIRequestURL(pageID)
  destfile = paste("./API_Responses/", language, "/", data$pageID[i], ".xml", sep="")
  if (!file.exists(destfile)){
    # save the file to disk as a backup
    download.file(exportURL, destfile, method="curl")
  } else if (overwrite == TRUE){ # download anyway if we have a newer version
    download.file(exportURL, destfile, method="curl")
  }
  return(destfile) # return the path to the file where the data is
}

endpoint = "http://live.dbpedia.org/sparql"

# This works for the english language Wikipedia
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
# This could be adapted for different languages, although it will have to be customized 
# based on how they arrange things.  On the page https://en.wikipedia.org/wiki/Category:Power_stations_by_country
# we can already see links to the same top category in different languages.

# If we get beyond 10,000 articles, then need to modify the query above to use OFFSET and LIMIT
# in order to download in blocks (due to restrictions in place on the SPARQL endpoint)
queryResults = SPARQL(url=endpoint, query=queryString, format='csv', extra=list(format='text/csv'))
data = queryResults$results

# make a sqlite database to store all of this
db <- dbConnect(SQLite(), dbname="WikipediaPowerPlants.sqlite")

if (length(dbListTables(db)) == 0){
  # create a database table
  dbSendQuery(conn = db,
              "CREATE TABLE PowerPlantArticles (
                  pageID INTEGER, 
                  revisionID INTEGER, 
                  latitude REAL, 
                  longitude REAL, 
                  pageText TEXT, 
                  language TEXT, 
                  timeStamp TEXT, 
                  title TEXT, 
                  APIResponseText TEXT)")
  
  # TODO should create directories for different languages as well
  # currently don't know a good way to get all the power plant pages in different languages
  dir.create("./API_Responses/en/", recursive=TRUE)
  
  options(HTTPUserAgent = "powerplantbot")
  for (i in c(1:nrow(data))){
    
    destfile = downloadWikipediaAPIResponseData(data$pageID[i])
    
    doc <- xmlTreeParse(destfile, useInternalNodes=TRUE)  
    pages <- getNodeSet(doc, "//page", addFinalizer=FALSE)
    # should only be one page per response
    for (page in pages){
      if (!"missing" %in% names(xmlAttrs(page))){ # make sure that the page isn't missing (not sure why this happens)
        
        plantData = c()
        plantData$pageID = as.numeric(getNodeSet(page, "./@pageid", addFinalizer=FALSE)[[1]])     

        # DBpedia supplies us with the revision ID, but we overwrite it here
        # This is because this should be the absolute latest version
        # if in a later query we get a higher number, then we know that we need to update.
        plantData$revisionID = as.numeric(getNodeSet(page, "./revisions/rev/@revid", addFinalizer=FALSE)[[1]])     

        lat = unlist(getNodeSet(page, "./coordinates/co/@lat", addFinalizer=FALSE))[[1]]
        lon = unlist(getNodeSet(page, "./coordinates/co/@lon", addFinalizer=FALSE))[[1]]
        
        if (!is.null(lat) && !is.null(lon)){
          plantData$latitude = lat
          plantData$longitude = lon
        } else {
          plantData$latitude = NA
          plantData$longitude = NA  
        }

        plantData$latitude = as.numeric(plantData$latitude)
        plantData$longitude = as.numeric(plantData$longitude)
        
        plantData$pageText = xmlValue(getNodeSet(page, "./revisions/rev", addFinalizer=FALSE)[[1]])
        plantData$language = "en"      
        plantData$timeStamp = unlist(getNodeSet(page, "./revisions/rev/@timestamp", addFinalizer=FALSE))[[1]]      
        plantData$title = as.character(getNodeSet(page, "./@title", addFinalizer=FALSE)[[1]])          
        
        # also write the raw text.  This isn't completely efficient as the pageText contains most of this
        # this may be useful for debugging later
        plantData$APIResponseText = readChar(destfile, file.info(destfile)$size)

        # TODO will need to deal with "List of " power stations pages - extract the data from the table rows.

                
        # write plantData to a row of the table
        dbSendQuery(conn = db,
                    paste("INSERT INTO PowerPlantArticles
                    VALUES (",paste(plantData, collapse=", "),")", sep=""))
        
      }
    }
    free(doc)
    rm(doc)
  }
  dbWriteTable(db, "PowerPlantArticles", data, overwrite=TRUE)
} else { # database is already initialized, look for new stuff
  oldData = sqldf("select title, pageID, revisionID from PowerPlantArticles", dbname = "WikipediaPowerPlants.sqlite")
  
  # see if we have any new articles 
  # TODO need to fill this in, no new pages detected yet.
  newPageIDs = setdiff(data$pageID, oldData$pageID)
  
  # see if we have any new revisions
  # need to compare pageID and revID - can just do a merge
  mergedData = merge(oldData, data, by="pageID")
  # find cases where the recent data queried from DBpedia has a higher revision number that what we have
  locs = which(mergedData$revisionID.x < mergedData$revisionID.y)
  
  # download pages with new revisions
  for (pageID in mergedData$pageID[locs]){
    destfile = downloadWikipediaAPIResponseData(pageID, overwrite = TRUE)
    
    # also write the raw text to the data frame, so we can dump this into the sqlite database later
    data$APIResponseText[i] = readChar(destfile, file.info(destfile)$size)
    
    doc <- xmlTreeParse(destfile, useInternalNodes=TRUE)  
    pages <- getNodeSet(doc, "//page", addFinalizer=FALSE)
    
  }
}

