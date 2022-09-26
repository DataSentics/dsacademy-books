# DataSentics academy: Books task continued

In the [previous task](https://www.notion.so/datasentics/GDC-Academy-PySpark-final-task-75402ced1f8a45c0b2189de0b6259e69), you had to just get to the required answers in (almost) any way possible. Now you have to write a nice, more production-like code.

## Task description

- Modify your existing solution based on what you have learned about data engineering in databricks. Imagine that the input data can incrementally change every day and the customer has business analysts who will want to write analytical queries against a clean, up-to-date state of the data every morning. The queries can be of the kinds you had to answer before (e.g., top rated books in the last ten years...). You need to create an ETL pipeline that will (ideally incrementally) reprocess the data every day.

### More info and hints how to approach this 

- Create a new metastore database, called `<your_name>_books`, in which all your tables will reside.
- All data should be saved in your azure storage container. You can organize the data in the container in any way you see fit. I.e., you should have there the three input [book dataset CSVs](http://www2.informatik.uni-freiburg.de/~cziegler/BX/) and all the derived tables.
- Create a multi-hop architecture using pyspark.
- Name the tables in some sensible ways, like `silver_users_cleansed`.
- Split your solution into multiple notebooks. It might be good to create a separate notebook for each output table. I.e., each notebook should write just one table (but can read multiple inputs). And the name of the notebook can be derived from the name of the output table.
- Create a parsed layer = the raw data converted to Delta.
- Create a cleansed layer = clean the data. (Inspecting the data, in what ways it is unclean, can be done in a separate notebook, which will not be used in the ETL pipeline.)
- Create a gold layer for the analytical queries from previous task.
        - Aggregate stats for books, authors and readers. TODO
- Orchestrate the whole ETL pipeline using a multi-task databricks job (workflow). Do not create a schedule for it, but think about how and when would you schedule it. You can store the definition of this job also as code as part of your solution. The notebooks can be e.g. in a folder called `src` and the job in a folder called `databricks-infra`.

## Technical notes

- Create and work in your own branch, derived from the `main` branch.
- You will submit your work by creating a pull request from your branch to `main`.
- You will learn how to better write nice code by having your code checked using the `flake8` linter when you push into the repository. It checks code style compliance to [PEP8](https://peps.python.org/pep-0008/), with some databricks-related exceptions that are defined in the `setup.cfg` file. You will likely get rule violation messages. You can read more about what the error codes mean e.g. [here](https://www.flake8rules.com/).
- You may push your work directly from Databricks Repos, but you may find it is better to use some local IDE like PyCharm or VSCode and git from local computer. The IDEs can check your code using `flake8` before you push it.
- To move code between databricks and local PC, you can use the Databricks CLI (doing it manually is a lot more work when you work with more than one file).