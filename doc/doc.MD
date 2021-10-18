# Overview
### Creation of shards
In order to create shards, the `RollingPolicy` contained in the `logback.xml` file has been modified as follows:

```
  <rollingPolicy class="ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy">           
      <fileNamePattern>log/LogFileGenerator.%d{yyyy-MM-dd}.%i.log</fileNamePattern>
      <maxFileSize>100KB</maxFileSize>
   </rollingPolicy>
```
By using `SizeAndTimeBasedRollingPolicy` we split the log files in shards of 100KB each. During testing, the `MaxCount` parameter in `application.conf` has been set to 5000: this led to the creation of approximately six shards for each test.


## Task 1
### Generate a CSV file that show the distribution of different log types across (divided by) predefined time intervals, together with the list of Regex instances injected by the log generator 

The interpretation adopted for the first task is the following:

- Time intervals are injected in the configuration (`application.conf`), with the following format:
    ```
      timeIntervals = [
        {
            start = "11:44:27"
            end = "11:44:27.999"
        },
        {
            start = "11:44:28"
            end = "11:44:28.999"
        },
        {
            start = "11:44:29"
            end = "11:44:29.999"
        }
    ]
  ```
  
- These predefined time intervals are those that will be analyzed by the MapReduce task
- The MapReduce task will produce, for each time interval, the distribution (count) of each log message type (e.g., `DEBUG`, `INFO`, `WARN`, `ERROR`)
- Example of output of the task:
  ```
  11:44:27 - 11:44:27.999 - DEBUG,5,<example1>,<example2>,<example3>,<example4>,<example5>
  11:44:27 - 11:44:27.999 - ERROR,2,<example1>,<example2>
  11:44:27 - 11:44:27.999 - INFO,4,<example1>,<example2>,<example3>,<example4>
  11:44:27 - 11:44:27.999 - WARN,3,<example1>,<example2>,<example3>
  11:44:28 - 11:44:28.999 - DEBUG,4,<example1>,<example2>,<example3>,<example4>
  11:44:28 - 11:44:28.999 - INFO,2,<example1>,<example2>
  11:44:28 - 11:44:28.999 - WARN,5,<example1>,<example2>,<example3>,<example4>,<example5>
  11:44:29 - 11:44:29.999 - DEBUG,4,<example1>,<example2>,<example3>,<example4>
  11:44:29 - 11:44:29.999 - ERROR,1,<example1>
  11:44:29 - 11:44:29.999 - INFO,1,<example1>
  11:44:29 - 11:44:29.999 - WARN,2,<example1>,<example2>
  ```

The first line means that in the input logs there are 5 `DEBUG` messages and 2 `ERROR` messages in the time interval `11:44:27 - 11:44:27.999`, and so on.

## Task 2
### Generate a CSV file with time intervals sorted in descending order by number of ERROR log messages contained, with also Regex instances injected by the log generator

For this task, two MapReduce jobs have been concatenated: the first job generates the number of `ERROR` log messages for each predefined time interval, while the second job orders them in descending order (by number of detected log messages).
Time intervals are defined in `application.conf` as shown for Task 1.
Example of output of the task:
```
4,11:44:29 - 11:44:29.999,<example1>,<example2>,<example3>,<example4>
2,11:44:28 - 11:44:28.999,<example1>,<example2>
2,11:44:27 - 11:44:27.999,<example1>,<example2>
```

This means that the `11:44:29 - 11:44:29.999` time interval had four `ERROR` log messages, while the other had two. To achieve this, an additional MapReduce Job have been implemented to swap the key and part of the value (the part that contains the number of occurrences), in addition to the use of a custom implemented `SortComparatorClass`(`DecreasingIntComparator`) to sort the results in descending order by the new key.

## Task 3
### Generate a CSV file with, for each log message type, the distribution (count of instances) across all the logs given as input
This task represents the *aggregation version* of Task1, without the configurable time intervals. In addition, in this case Regex instances for each log message type are not requested.
