# Overview

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