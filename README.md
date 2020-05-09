# elastic-workshop
Elastic workshop meetup repository 14 May 2020

This workshop aims to share the knowledge and the experience which is gained in a migration project to use elasticsearch as the search engine for our music app. 

Elasticsearch is used to index artist, song, album, playlist metadata providing full text search capabilities as well as popularity and personal behaviour based scoring functionality so that more popular and more relevant results are boosted in the search results for individual users.

## pre-requisites
+ elastic search 7.x +
+ kibana 7.x +
+ spring boot 2.2.x +
+ java 8+

elastic and kibana can be obtained easily from following sites with few alternatives e.g. standalone executable, docker, package managers, etc. 

https://www.elastic.co/downloads/elasticsearch
https://www.elastic.co/downloads/kibana

the maven project provided in this repository will be enough to run spring boot application


## introduction to kibana
Beside it's many features, kibana is very useful for sending HTTP requests (index management, document operations, search requests, etc.) to elastic server and visualizing the results in JSON format.

if started with default configuration, kibana will be running on following address

http://localhost:5601/

in order to run elastic requests, dev tools console can be activated in either way

+ http://localhost:5601/app/kibana#/dev_tools/console
+ click on the tools button towards the bottom of the tool bar 

![kibana dev tools console](/doc/kibana_dev_tools_console.png?raw=true)

in order to execute a specific request just click on the send request (play) button to the right of the top line of the command

![kibana dev tool send request](/doc/kibana_dev_tools_send_request.png?raw=true)

