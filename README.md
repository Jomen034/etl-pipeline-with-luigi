# ETL Pipeline with Luigi
This repository is about building ETL pipeline with Luigi use local data and store into local data warehouse

# What is Luigi?
From the [GitHub](https://github.com/spotify/luigi) page, **Luigi is a Python (3.6, 3.7, 3.8, 3.9 tested) package that helps you build complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization, handling failures, command line integration, and much more.**<br>
![image](https://user-images.githubusercontent.com/71366136/115017156-47d5f800-9ee0-11eb-85c1-edc339e8245a.png)

# Get Started
Install `Luigi` with your command prompt

```pip install luigi```

In this task, I also used `pandas` and `sqlitee3`. Install it first if you haven't

```pip install pandas```

```pip install db-sqlite3```

# Run the ETL
_**Intro...**_
Luigi provides user to see the whole process by monitoring into a web-based interface. For this purpose, **run `luigid`** from your command prompt that opened in your file directory

_**Next...**_
As we need to extract the data first that assign in the 1st Task as `Class ExtractData(luigi.Task)`, so after you go to your file directory, run this command first:

```python -m luigi --module etl_pipeline ExtractData --local-scheduler```

This command to tell the system to run only `ExtractData` class first to get our exctracted data. If this command successfully run, you will see this response:

```
DEBUG: Checking if ExtractData() is complete
INFO: Informed scheduler that task   ExtractData__99914b932b   has status   DONE
INFO: Done scheduling tasks
INFO: Running Worker with 1 processes
DEBUG: Asking scheduler for work...
DEBUG: Done
DEBUG: There are no more tasks to run at this time
INFO: Worker Worker(salt=769338663, workers=1, host=PARDEDE-JOMEN, username=jomen, pid=11280) was stopped. Shutting down Keep-Alive thread
INFO:
===== Luigi Execution Summary =====

Scheduled 1 tasks of which:
* 1 complete ones were encountered:
    - 1 ExtractData()

Did not run any tasks
This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====
```
Now the extracted data already in your directory. Next we run the **`TL`** session with this command:

```python -m luigi --module etl_pipeline LoadData --local-scheduler```

This command will execute the `LoadData` class from our `et_pipeline.py`. This class requires the `TransformData` class to be runed first. If the whole process run well, you will see this response:

```
DEBUG: Checking if LoadData() is complete
DEBUG: Checking if TransformData() is complete
INFO: Informed scheduler that task   LoadData__99914b932b   has status   PENDING
INFO: Informed scheduler that task   TransformData__99914b932b   has status   DONE
INFO: Done scheduling tasks
INFO: Running Worker with 1 processes
DEBUG: Asking scheduler for work...
DEBUG: Pending tasks: 1
INFO: [pid 10020] Worker Worker(salt=055082844, workers=1, host=PARDEDE-JOMEN, username=jomen, pid=10020) running   LoadData()
INFO: [pid 10020] Worker Worker(salt=055082844, workers=1, host=PARDEDE-JOMEN, username=jomen, pid=10020) done      LoadData()
DEBUG: 1 running tasks, waiting for next task to finish
INFO: Informed scheduler that task   LoadData__99914b932b   has status   DONE
DEBUG: Asking scheduler for work...
DEBUG: Done
DEBUG: There are no more tasks to run at this time
INFO: Worker Worker(salt=055082844, workers=1, host=PARDEDE-JOMEN, username=jomen, pid=10020) was stopped. Shutting down Keep-Alive thread
INFO:
===== Luigi Execution Summary =====

Scheduled 2 tasks of which:
* 1 complete ones were encountered:
    - 1 TransformData()
* 1 ran successfully:
    - 1 LoadData()

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====
```

