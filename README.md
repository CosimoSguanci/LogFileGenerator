# Cosimo Sguanci
## csguan2@uic.edu - 2nd Homework (CS441)

## Installation instructions
To build the JAR for the Hadoop MapReduce jobs of this homework, the first step is to install Scala through sbt (installation files available [here](https://www.scala-lang.org/download/scala3.html)).

The next step is to clone the repository with the following command:

```
git clone https://github.com/CosimoSguanci/LogFileGenerator.git
```

Finally, to build the JAR file:

```
cd LogFileGenerator
sbt assembly
```

By default, the JAR will be placed in `target/scala-3.0.2/LogFileGenerator-assembly-0.1.jar`

## Instructions for running the Hadoop jobs
In order to run the MapReduce jobs it is necessary to have a running Hadoop cluster. A convenient way to achieve this is to install a VM with all the necessary software components already installed, like the Hortonworks Data Platform Sandbox (instructions available [here](https://www.cloudera.com/tutorials/getting-started-with-hdp-sandbox.html)). 
Once the VM has been started, for example by using a hypervisor like VMWare, we can connect to it through SSH:

```
ssh root@sandbox-hdp.hortonworks.com -p 2222
```

Then, we have to transfer both the JAR and the input shards files to the VM, by using the scp command:

```
scp -P 2222 LogFileGenerator-assembly-0.1.jar root@sandbox-hdp.hortonworks.com:~
scp -P 2222 example_shard_log_1.log root@sandbox-hdp.hortonworks.com:~
```

From the SSH terminal started before, we can use HDFS to create the input folder for our application, and move there all our log shards:

```
hadoop fs -mkdir logs/input
hadoop fs -put example_shard_log* logs/input
```

Finally, we are ready to start Hadoop jobs, that can be done with the following command

```
hadoop jar LogFileGenerator-assembly-0.1.jar Tasks.Task1 logs/input logs/output
```

Assuming that we are in the root home directory (~), where we previously placed the JAR file.

To run different tasks, we can change the command by using `Tasks.TaskX`, replacing `X` with the actual identifier of the task that we want to run. This is possible because the fat JAR generated contains `main` methods for all the required tasks for this homework.

The only tasks for which it's necessary to slightly change the procedure is the task number 2, that requires to MapReduce jobs in a pipeline-like style, for this reason we have to specify a second output folder, with the first that will be used also as input to the second job:

```
hadoop jar LogFileGenerator-assembly-0.1.jar Tasks.Task2 logs/input logs/output logs/output2
```



Details about the implemented tasks can be found in the `doc/DOC.md` file, while example of input/output for all the tasks can be found in the `results` folder.
