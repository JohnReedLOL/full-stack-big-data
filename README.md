# full-stack-big-data

#### Basic tools and proof of concept functionality:

- Full stack big data demo with Play Framework, Akka, Kafka, Akka Streaming, HDFS, batch layer, and CouchDB for querying. 

- Provides simple real time and batch word count. 

- Calculates the difference between real time and batch (in case of data loss) and stores the result for querying.

#### Purpose:

The purpose of this project is to provide a simple seed with all the library support necessary to build a big data application from scratch.

#### Architecture:

##### Data model:

input: String, output: (timestamp, Int), where Int is a wordcount

##### Gui:

 ________  
| Input Text    |
| ------------- |
| text          |
| ...           |
| ...           |
 ________  
 
^ Submit ^

A simple text box with a submit button. Clicking "Submit" brings the user to a query menu.

Time interval: _____ to ______ | Submit |

The user enters the time interval for the query. Data is pulled from the backend to produce a result. The user can compare the "speed" result with the actual result produced from the query:

Count from web framework: X  
Count from streaming layer: Y  
Count from batch layer: Z  

The user can then compare the counts produced by the different layers.

##### URL schema

/* This gets the page with the text box */  
- get  /home    

/* This sends the submitted string to the word count page, changing the state of the system in the backend */  
- post /home    

/* This gets the page with the time interval query */  
- get  /home/time/   

/* This sends startTime and endTime to the time interval query, getting the counts for that time interval */  
- get  /home/time/start="1:10:2"&end="1:10:5"     

##### Environment:

Assumes the user has set up an environment with all the tools availiable. I will be using a compressed (tar.bz) 6Gb virtualbox disk image that can be downloaded pre-installed with all the tools necessary to run. The intent is to use the exact same disk image used to write and develop the seed as the user gets to run the seed.

##### Instructions (not yet available):

Download virtualbox disk image

Open in virtualbox

Open IntelliJ

Run
