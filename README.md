# elastic-workshop
Elastic workshop meetup repository 14 May 2020

This workshop aims to share the knowledge and the experience which is gained in a migration project to use elasticsearch as the search engine for our music app. 

Elasticsearch is used to index artist, song, album, playlist metadata providing full text search capabilities as well as popularity and personal behaviour based scoring functionality so that more popular and more relevant results are boosted in the search results for individual users.

## 1.1 pre-requisites
+ elastic search 7.x +
+ kibana 7.x +
+ spring boot 2.2.x +
+ java 8+

elastic and kibana can be obtained easily from following sites with few alternatives e.g. standalone executable, docker, package managers, etc. 

https://www.elastic.co/downloads/elasticsearch
https://www.elastic.co/downloads/kibana

the maven project provided in this repository will be enough to run spring boot application


## 1.2 introduction to kibana
Beside it's many features, kibana is very useful for sending HTTP requests (index management, document operations, search requests, etc.) to elastic server and visualizing the results in JSON format.

if started with default configuration, kibana will be running on following address

http://localhost:5601/

in order to run elastic requests, dev tools console can be activated in either way

+ http://localhost:5601/app/kibana#/dev_tools/console
+ click on the tools button towards the bottom of the tool bar 

![kibana dev tools console](/doc/kibana_dev_tools_console.png?raw=true)

in order to execute a specific request just click on the send request (play) button to the right of the first line of the command

![kibana dev tool send request](/doc/kibana_dev_tools_send_request.png?raw=true)

in order to list the existing indices in elastic search just run this command from kibana dev tools console


```
GET /_cat/indices?v
```

refer to kibana_console.txt file for full set of requests used for this workshop. you can copy/paste the content of this file to your Kibana dev tools console and run the requests accordingly.
 
## 2 search as you type - analyzers

### 2.1 understanding analyzers 
by default, if a custom setting is not applied, elasticsearch will analyze the input text with the 'standard' analyzer.

```
POST _analyze 
{
  "text": "Hélène Ségara it's !<>#"
}
```

this will produce only three tokens : 

```
{
  "tokens" : [
    {
      "token" : "hélène",
      "start_offset" : 0,
      "end_offset" : 6,
      "type" : "<ALPHANUM>",
      "position" : 0
    },
    {
      "token" : "ségara",
      "start_offset" : 7,
      "end_offset" : 13,
      "type" : "<ALPHANUM>",
      "position" : 1
    },
    {
      "token" : "it's",
      "start_offset" : 14,
      "end_offset" : 18,
      "type" : "<ALPHANUM>",
      "position" : 2
    }
  ]
}
```

having observed that, let's index 4 artist documents in 'content' index

```
POST /content/_bulk
{ "index" : {"_id" : "a1" } }
{ "type": "ARTIST", "artist_id": "a1", "artist_name": "Sezen Aksu","ranking": null }
{ "index" : {"_id" : "a2" } }
{ "type": "ARTIST", "artist_id": "a2", "artist_name": "Selena Gomez","ranking": null }
{ "index" : {"_id" : "a3" } }
{ "type": "ARTIST", "artist_id": "a3", "artist_name": "Shakira","ranking": null }
{ "index" : {"_id" : "a4" } }
{ "type": "ARTIST", "artist_id": "a4", "artist_name": "Hélène Ségara","ranking": null }
```

let's try to search these artists with the prefix 's' because all of them have one word starting with 's'

```
POST /content/_search
{
  "query": {
    "multi_match": {
      "query": "s",
      "fields": [
        "artist_name"
      ]
    }
  }
}
```

unfortunately, we get no matching results, we still need to work on analyzers. it's easy to try and see the results of analyzers in Kibana dev tools console. try following commands and analyze the results

remove all characters which are not alpha-numeric or whitespace. 
```
POST _analyze
{
  "text": "Hélène Ségara it's !<>#",
  "char_filter": [
    {
      "type": "pattern_replace",
      "pattern": "[^\\s\\p{L}\\p{N}]",
      "replacement": ""
    }
  ], 
  "tokenizer": "standard"
}
```

lower case all the characters
```
POST _analyze
{
  "text": "Hélène Ségara it's !<>#",
  "char_filter": [
    {
      "type": "pattern_replace",
      "pattern": "[^\\s\\p{L}\\p{N}]",
      "replacement": ""
    }
  ], 
  "tokenizer": "standard",
  "filter": [
    "lowercase"
  ]  
}
```

apply ascii folding to non-ascii characters
```
POST _analyze
{
  "text": "Hélène Ségara it's !<>#",
  "char_filter": [
    {
      "type": "pattern_replace",
      "pattern": "[^\\s\\p{L}\\p{N}]",
      "replacement": ""
    }
  ], 
  "tokenizer": "standard",
  "filter": [
    "lowercase",
    "asciifolding"
  ]  
}
```

and finally produce more tokens with ngrams like 'h', 'he', 'hel', 'hele', etc.
```
POST _analyze
{
  "text": "Hélène Ségara it's !<>#",
  "char_filter": [
    {
      "type": "pattern_replace",
      "pattern": "[^\\s\\p{L}\\p{N}]",
      "replacement": ""
    }
  ], 
  "tokenizer": "standard",
  "filter": [
    "lowercase",
    "asciifolding",
    {
      "type": "edge_ngram",
      "min_gram": "1",
      "max_gram": "12"
    }
  ]  
}
```

### 2.2 index settings (analyzers) and field mappings 
now we need to delete the index that has been created automatically in the previous step 

```
DELETE /content
```

create the content index with analyzer settings and field mappings. the following call will create an empty index. pay attention to artist_name property and it's additional field 'prefix' which is derived from the artist_name value.

```
PUT /content
{
    "settings": {
        "analysis": {
            "filter": {
                "front_ngram": {
                    "type": "edge_ngram",
                    "min_gram": "1",
                    "max_gram": "12"
                }
            },
            "analyzer": {
                "i_prefix": {
                    "filter": [
                        "lowercase",
                        "asciifolding",
                        "front_ngram"
                    ],
                    "tokenizer": "standard"
                },
                "q_prefix": {
                    "filter": [
                        "lowercase",
                        "asciifolding"
                    ],
                    "tokenizer": "standard"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "type": {
                "type": "keyword"
            },
            "artist_id": {
                "type": "keyword"
            },
            "ranking": {
                "type": "double"
            },
            "artist_name": {
                "type": "text",
                "analyzer": "standard",
                "index_options": "offsets",
                "fields": {
                    "prefix": {
                        "type": "text",
                        "term_vector": "with_positions_offsets",
                        "index_options": "docs",
                        "analyzer": "i_prefix",
                        "search_analyzer": "q_prefix"
                    }
                },
                "position_increment_gap": 100
            }
        }
    }
}
```

now if we repeat the bulk indexing 4 documents and searching with prefix 's' we can find all four of the artist documents.

```
POST /content/_search
{
  "query": {
    "multi_match": {
      "query": "s",
      "fields": [
        "artist_name.prefix"
      ]
    }
  }
}
```

## 3 popularity (ranking) based boosting

### 3.1 listen events 
in order to update the rankings of artists and users' profile for personalized search results, we need to 
+ store the listen events in temporary elasticsearch indices (listen-event-*)
+ process these events at some intervals e.g. hourly, half-daily, daily, weekly, etc. 
+ update artist rankings (content, artist-ranking-*)
+ update user profiles based on listening events (user-profile)

#### 3.1.1 listen-event-* index template
let's create an index template for listen-events so that each index inherits the field mappings and index settings. 

```
PUT _template/listen_events_template
{
  "index_patterns": ["listen-event*"],
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "properties": {
      "artist_id": {
        "type": "keyword"
      },
      "user_id": {
        "type": "keyword"
      },
      "timestamp": {
        "type": "date",
        "format": "date_hour_minute_second_millis"
      }
    }
  }
}
```

#### 3.1.2 artist-ranking-* index template
let's create an index template for aritst-ranking indices so that each index inherits the field mappings and index settings. 

```
PUT _template/artist_rankings_template
{
  "index_patterns": ["artist-ranking*"],
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "properties": {
      "artist_id": {
        "type": "keyword"
      },
      "ranking": {
        "type": "long"
      }
    }
  }
}
```

#### 3.1.3 save listen-events

since this business logic requires partitioning listen-events in periodic indices, we should use spring rest application for managing listen events

```
curl -X POST \
  http://localhost:8080/event/listen-event \
  -H 'Content-Type: application/json' \
  -d '{
	"user_id": "user1",
	"artist_id": "a1"
}'
```

posting these listen-event records will trigger auto indexing events and updating artist rankings, as well as user profiles

once the artist rankings are updated, search results will be boosted depending on the artist ranking. have a look at following classes

+ ElasticSearchService.java
+ EventProcessingService.java

following request will perform search operations with ranking boost algorithm
```
curl -X GET \
  'http://localhost:8080/search/artist?q=s&userid=user1&from=0&size=10' 
```

