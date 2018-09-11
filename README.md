# Moama Search-Engine
A project for crawling, searching and analyzing the web. This project consists of different modules which the important executable ones are crawler that crawls the web and indexes everything into Elasticsearch and HBase, news that fethces all world news from top news websites and console that provides a simple CLI for using our services. There are also elsaticsearch and hbase modules which are used as database connectors and analytical modules like analysis, keywords and page-rank. This project is written completely in Java except for some machine-learning analytical scripts used for web classification wich are written in Python.

## Getting Started
### Installing
Since the only method we provide is building from source code, first of all, you need java 8 or higher and maven to build and run this project. After installing them clone the project and build it using maven. To do so enter the following command in the root directory of the project (aka search-engine)
```bash
mvn clean install 
```
Or you can only build console module (and it's dependencies of course!) 
```bash
mvn clean install -pl console -am 
```
### Running
After doing so, there is a target folder in each module that contains an executable jar file (i.e cralwer-1.0-SNAPSHOT-jar-with-dependencies.jar). To run the CLI go to console module and run the jar file
```bash
cd /console/target
java -jar console-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Commands
You can use ?l command to see all available commands. For example let's use advanced search, assuming we want to see pages that must have hadoop, must not have spark and it's better if they have hbase
```
Console Search> as
Please add you desired necessary words or phrases for search.
Please Finish entering input by typing : -done-
>hadoop
>-done-
Please add you desired forbidden words or phrases for search.
Please Finish entering input by typing : -done-
>spark
>-done-
Please add you desired preferred words or phrases for search.
Please Finish entering input by typing : -done-
>hbase
>-done-
```
Then you will see a list of the best pages that match this query.</br>
Using our specific news section is also as easy as that. For example if you want to find 'brexit and european union' between the news here's how you can do it
```
Console Search> ns
Please add you desired words or phrases for search.
Please Finish entering input by typing : -done-
>brexit and european union
>-done-
```
Finally, you can close the program using exit command.

## Acknowledgments
Special thanks to anyone who helped us develop this tool. Powered by [nimbo.in](http://nimbo.in)
