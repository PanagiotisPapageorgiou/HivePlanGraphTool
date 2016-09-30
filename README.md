# HiveToExaremePlanConverter
The following project aims to aid MADGIK (University of Athens Department of Informatics) Exareme project in using Map-Reduce logic on BigData. 

Exareme BigData project: https://github.com/madgik/exareme

In order to achieve the above a given query is first run through a Hive on Hadoop(YARN) Minicluster and the execution plan for the query is obtained. Afterwards, the Hive Execution Plan is translated through a set of stages into an Exareme-on-HDFS query execution plan.

The project supports as of now a small subset of HiveQL query language and works for 1 Worker Node in the Hadoop Cluster. Ultimately, we will be supporting all of HiveQL Query Language, multiple worker nodes, partitions and more.

The above project was developed as part of my Bachelor Thesis with the assistance of Alexandros Papadopoulos, Ioannis Foufoulas and professor Ioannis Ioannidis for the Department of Informatics of University of Athens (2015-2016).

--Installation Requirements--

-Java7 or above

-Maven3

--How it works--

1)In the class OpGraphProducerMain a setup.sql file can be specified to declare the HiveQL commands one wishes to run in order to setup the experiment (such as creating databases and tables)

2)In the same class another .sql file can be specified to declare the commands that will run on the above data.

3)Once a query is parsed by Hive MiniCluster the user will be given the option to either run the query, extract its operator graph or exit.

4)Extracting the Operator Graph is where our project comes into play. Choosing this option will extract the Query Plan for the specified Query and translate it into an Exareme Execution plan stored into the exaremePlan.json file.

5)You can now test the plan file by feeding it into Exareme. (more on this later)
