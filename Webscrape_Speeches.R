rm(list=ls()) 

library(XML)
library(stringi)
library(tidyverse)
library(httr2)
library(janitor)
library(rvest)     
library(purrr)

setwd("C:/Users/tscho/OneDrive/Desktop/Quants2")

#Setting the page to scrape
page <- read_html("https://www.theyworkforyou.com/pwdata/scrapedxml/debates")
#Downloading all links included in the page (speeches)
page <- page %>% html_nodes("a") %>% html_attr('href')

#Transform it to a list
page <- as.list(page)

#Exclude non-relevant links (readtext etc.)
pagesub <- page[-c(1, 2, 3, 4, 5, 6)]

#I ran this code multiple times on my laptop, but I get errors due to
# the size of all 18581 speeches. I think its best to run it 
#in subsets of size 2k 

#pagesub1 <- pagesub[c(1:2000)]
#pagesub2 <- pagesub[c(2001:4000)]
#pagesub3 <- pagesub[c(4001:6000)]
#pagesub4 <- pagesub[c(6001:8000)]
#pagesub5 <- pagesub[c(8001:10000)]
#pagesub6 <- pagesub[c(10001:18575)]




#Attach the Link-prefixes to scraped sub-link information
nms <- purrr::map_chr(pagesub, ~ paste0("https://www.theyworkforyou.com/pwdata/scrapedxml/debates/", .))

#overview: Success
nms

#Prewritten code to convert XML information to dataframe
#Source Kohei Watanabe: https://blog.koheiw.net/?p=33

readFile <- function(fileName) {
  lines <- readLines(fileName, encoding = "UTF-8")
  return(paste(lines, collapse = '\n'))
}

readDebateDir <- function(dir) {
  files <- list.files(dir, full.names = TRUE, recursive = TRUE, pattern = "\\.xml$")
  result <- data.frame()
  for(file in files){
    result <- rbind(result, readDebateXML(file))
  }
  return(result)
}

readDebateXML <- function(file) {
  cat('Reading', file, '\n')
  xml <- xmlParse(readFile(file))
  result <- data.frame()
  for(speech in getNodeSet(xml, '//speech')){
    values <- getSpeech(speech)
    temp <- data.frame(date = values[[1]], 
                       time = values[[2]], 
                       speaker = values[[3]], 
                       personId = values[[4]], 
                       text = values[[5]], 
                       file = file, stringsAsFactors = FALSE)
    result <- rbind(result, temp)
  }
  return(result)
}

getSpeech <- function(speech) {
  
  attribs <- xmlAttrs(speech)
  #print(xmlAttrs(speech, "speakername"))
  if ("speakername" %in% names(attribs)){
    speaker = attribs[['speakername']]
  } else {
    speaker = ""
  }
  if ("person_id" %in% names(attribs)){
    personId = getPersonId(attribs[['person_id']])
  } else {
    personId = ""
  }
  if ("id" %in% names(attribs)){
    date = getDate(attribs[['id']])
  } else {
    date = ""
  }
  if ("time" %in% names(attribs)){
    time = getTime(attribs[['time']])
  } else {
    time = ""
  }
  text <- getSpeechText(speech)
  list(date, time, speaker, personId, text)
}

getSpeechText <- function(x) {
  ps <- unlist(xpathApply(x, './p', xmlValue))
  text <- paste(unlist(ps), collapse = "\n")
  return(stri_trim(text))
}

getTime <- function(x) {
  
  parts <- unlist(stri_split_fixed(x, ':'))
  h <- as.numeric(parts[1])
  m <- as.numeric(parts[2])
  s <- as.numeric(parts[3])
  return(paste(h, m, s, sep = ':'))
}

getDate <- function(x) {
  parts <- unlist(stri_split_fixed(x, '/'))
  date <- stri_sub(parts[3], 1, 10)
  return(date)
}

getPersonId <- function(x) {
  parts <- unlist(stri_split_fixed(x, '/'))
  return(parts[3])
}

###Setting an empty dataframe to be filled

UK_Speech_Data <- NULL

# Running a loop that:
#1. Addresses each Item in the list (each link= each speech)
#2. Reads the data based on the prewritten function
#3. Attaches the data to a dataframe 


for(i in nms) {
   dat <-readDebateXML(i)
   UK_Speech_Data <- rbind(UK_Speech_Data, dat)}

#That takes a (long) while for 18581 links 

write.csv(UK_Speech_Data , file.path("UK_Speech_Data.csv"), row.names=FALSE)   

####loading of the individual sub-files, can be skipped 
#if server is available and data can be downloaded as bulk

Uk1 <- read.csv("UK_Speech_Data1.csv", stringsAsFactors = FALSE)
Uk2 <- read.csv("UK_Speech_Data2.csv", stringsAsFactors = FALSE)
Uk3 <- read.csv("UK_Speech_Data3.csv", stringsAsFactors = FALSE)
Uk4 <- read.csv("UK_Speech_Data4.csv", stringsAsFactors = FALSE)
Uk5 <- read.csv("UK_Speech_Data5.csv", stringsAsFactors = FALSE)
Uk6 <- read.csv("UK_Speech_Data6.csv", stringsAsFactors = FALSE)

#Merging
UK_Speech_Data_final_1 <- rbind.data.frame(Uk1, Uk2 )
UK_Speech_Data_final_2 <- rbind.data.frame(UK_Speech_Data_final_1, Uk3)
UK_Speech_Data_final_3 <- rbind.data.frame(UK_Speech_Data_final_2, Uk4) 
UK_Speech_Data_final_4 <- rbind.data.frame(UK_Speech_Data_final_3, Uk5)
UK_Speech_Data_final_5 <- rbind.data.frame(UK_Speech_Data_final_4, Uk6)
 #Csv
write.csv(UK_Speech_Data_final_5 , file.path("UK_Speech_Data_merged.csv"), row.names=FALSE)   
