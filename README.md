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

##### GUI:

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

#### Environment:

Assumes the user has set up an environment with all the tools availiable. I will be using a compressed (tar.bz) 6Gb virtualbox disk image that can be downloaded pre-installed with all the tools necessary to run. The intent is to use the exact same disk image used to write and develop the seed as the user gets to run the seed.

#### Instructions (not yet available):

Download virtualbox disk image

Open in virtualbox

Open IntelliJ

Run

#### Compilation (not yet available):

Project is broken up into seperately compilable compilation units. Each component is its own compilation unit with two "super units" - ingestion and processing. Ideas for compilation units include web server compilation unit [handles requests], reactive kafka compilation unit [handles relaying], and batch processing compilation unit. 

According to the book "Mythical Man Month", teams tend to split according to the boundaries of the architecture, so teams can naturally divvy up based on independent components that they work on.

"The morale effects are startling. Enthusiasm jumps when there is a running system, even a simple one. Efforts redouble when the first picture from a new graphics software system appears on the screen, even if it is only a rectangle. One always has, at every stage in the process, a working system. I find that teams can grow much more complex entities in four months than they can build."

—FREDERICK P. BROOKS, JR., The Mythical Man-Month

#### Tenents

- "lightweight" - minimize and remove unnecessary extra stuff
- "distributed" - seperate pieces which talk to each other can survive if another piece dies.
- "open source" - should be possible to have community of sites like [Scala Reddit](https://www.reddit.com/r/scala/) and Stack Overflow look at code, report problems or vulnerabilities. 
- "real time" - should be able to provide immediate results in addition to batch.
