**Lesson 3: Designing and Building a Data Pipeline using Apache Airflow**

**Duration:** 90 minutes

**Authors:** Claudio Canales

**Objectives:**

-   ✅ Understand the core concepts of Apache Airflow: DAGs, Operators, Tasks, and the Airflow UI.
-   ✅ Grasp the importance of data pipelines in automating data workflows.
-   ✅ Design a simple yet functional data pipeline using Airflow DAGs.
-   ✅ Implement a data pipeline that extracts, transforms, and loads data (ETL).
-   ✅ Use `PythonOperators` to define and execute Python functions as tasks in Airflow.
-   ✅ Effectively use SQLite as both a data source and destination within an Airflow pipeline, including connection setup.
-   ✅ Monitor, manage, and troubleshoot workflows using the Airflow UI.
-   ✅ Understand basic error handling and data validation concepts within the context of a data pipeline.

**Prerequisites:**

-   Basic understanding of Python programming (especially Python 3.11, including concepts like functions, data structures, and error handling).
-   Familiarity with SQL concepts (creating tables, inserting data, querying data).
-   Access to the provided workspace with:
    -   Python 3.11.9 pre-installed.
    -   Apache Airflow (version 2.6 or later) pre-installed and configured to run in standalone mode.
    -   Necessary Python libraries (pandas, sqlite3, etc.) pre-installed.
-   Basic understanding of command-line interfaces (navigating directories, running commands).

**Lab Scenario:**

We'll build a data pipeline that simulates a realistic, albeit simplified, ETL (Extract, Transform, Load) scenario for our e-commerce company, **ShopSmart**. The pipeline will:

1.  **Extract:** Simulate extracting product data from a CSV file that might be provided by an external system.
2.  **Transform:** Clean and transform the product data using pandas to prepare it for analysis. This includes handling potential issues like missing values and adding new, derived fields.
3.  **Load:** Load the transformed data into an SQLite database, a common choice for development, testing, and smaller-scale data warehousing.
4.  **Validate:** Verify that the data has loaded correctly.

**Bridge:** In our previous sessions, we discussed the importance of data management, data quality, and the principles of designing and optimizing data pipelines. Now, we'll put that knowledge into action by building a functional pipeline using Apache Airflow, a leading workflow orchestration tool. This lab will demonstrate how Airflow puts the theory into practice.

**Lab Steps:**

**Part 1: Workspace Setup and Airflow Installation (25 minutes)**

1.  **Access the Workspace:**
    *   Access the AWS Workspace.

2. **Make Python and pip 3.11 version default**
    *   Open a terminal (or command prompt) in the workspace and run:

        ```bash
        echo "alias python='python3.11' && alias pip='python3.11 -m pip'" >> ~/.bashrc && source ~/.bashrc
        ```

2.1  **Verify Python Version:**
    *   Ensure that the output shows Python 3.11.9:

        ```bash
        python --version
        pip --version
        ```

3.1  **Update sqlite:**
    *   Unfortunately AWS Workspace is including a very old version which we need to install running the following script:

        ```bash
        cd ~/ && \
        wget https://www.sqlite.org/2024/sqlite-autoconf-3450000.tar.gz && \
        tar xzf sqlite-autoconf-3450000.tar.gz && \
        cd sqlite-autoconf-3450000 && \
        ./configure && \
        make && \
        sudo make install && \
        echo 'export LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"' >> ~/.bashrc && \
        source ~/.bashrc
        sudo ldconfig /usr/local/lib
        ```

3.2  **Install Apache Airflow and pandas:**
    *   Install Airflow version 2.10.4 with the SQLite provider, pandas, and the correct constraints for Python 3.11 (It will take about 2 minutes):

        ```bash
        pip install pandas "apache-airflow[sqlite]==2.10.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
        ```

    *   **Explanation:**
        -   We use `pip` to install Airflow from the Python Package Index (PyPI).
        -   `"apache-airflow[sqlite]==2.10.4"` specifies the Airflow package with the `sqlite` extra, which installs dependencies needed to work with SQLite databases. We are explicitly installing version 2.10.4.
        -   `--constraint` ensures that compatible versions of all dependencies are installed, avoiding potential conflicts.

4.  **Initialize the Airflow Database:**

    ```bash
    airflow db init
    ```

5.  **Create an Airflow User:**

    ```bash
    airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
    ```

6.  **Start Airflow:**

    *   In the terminal, start Airflow in standalone mode:

        ```bash
        airflow standalone
        ```

    *   **Explanation:** `airflow standalone` is a convenient way to run Airflow for development and testing. It starts all the necessary components (webserver, scheduler, database) in a single process.
    *   **Note:** The command will output the Airflow Webserver URL and the credentials for the admin user. You'll need these to log in to the Airflow UI.

    **Sample Output:**

    ```
    ...
    [2023-10-27 10:00:00,000] {webserver.py:123} INFO - Airflow Webserver is running at http://localhost:8080
    [2023-10-27 10:00:00,000] {standalone.py:456} INFO - You can use the following URL: http://localhost:8080
    ...
    ```

7.  **Access Airflow UI:**

    *   Open a web browser and go to `http://localhost:8080`.
    *   Log in using the credentials provided by `airflow users create`.

8.  **Airflow UI Overview:**

    *   **DAGs View:**
        *   This is the main view where all defined DAGs are listed.
        *   Highlight the DAG name, schedule, status (running, success, failed), and the last run's information.
        *   Explain the concept of "pausing" and "unpausing" DAGs.
    *   **Show the Details, Graph and Code View:**
        *   Select a sample DAG (if any are preloaded) and show the Graph View.
        *   This visualizes the DAG's structure, showing tasks and their dependencies.
        *   Point out how the colors represent task status (e.g., green for success, red for failed, etc.).
        *   Explain all these tabs briefly, indicating that they provide more detailed information about the task's configuration and execution.
    *   **Admin > Connections:**
        *   This is where you configure connections to external systems (databases, APIs, cloud services).
        *   Show the list of existing connections (if any).
        *   Mention the different connection types supported by Airflow.
    *   **Browse > Task Instances:**
        *   This is where you can view all task instances across all DAGs.
        *   Show the available filters, especially the ones for date.

**Part 2: Create a SQLite Database and Connection (15 minutes)**

1.  **Create a Database Directory and File:**

    *   **Important:** Open a different terminal where you started Airflow.
    
    *   Create a new directory called `data` in your home directory:

        ```bash
        mkdir data
        ```

    *   Inside the `data` directory, create an empty `demo.db` file:

        ```bash
        touch ./data/demo.db
        ```

2.  **Configure SQLite Connection in Airflow:**

    *   In the Airflow UI, go to `Admin` > `Connections`.
    *   Click the `+` (Create) button to add a new connection.
    *   Fill in the following details:
        *   **Conn Id:** `my_sqlite_conn`
        *   **Conn Type:** `Sqlite`
        *   **Host:** `/home/du_XXXXXX-YYYYYYYYYY/data/demo.db` (In the second terminal, run pwd your du_XXXXXX-YYYYYYYYYY folder name)
        *   **Schema:** (Leave this blank for SQLite)
    *   Leave other fields blank or at their default values.
    *   Click `Save`.

**Part 3: Design the Data Pipeline (10 minutes)**

1.  **DAG Structure and the ETL Process:**

    *   A DAG (Directed Acyclic Graph) defines a workflow as a set of tasks and their dependencies. "Directed" means dependencies have a direction, "Acyclic" means there are no loops.
    *   Explain the ETL process (Extract, Transform, Load) and how it relates to the DAG we're building.
    *   Discuss the three main tasks of our pipeline: `extract_data`, `transform_data`, and `load_data`, plus the `validate_data` task.

    Here's a diagram of our ETL pipeline:

    ```mermaid
    graph LR
        A[extract_data] --> B(transform_data);
        B --> C(load_data);
        C --> D(validate_data);
    ```

2.  **Task Details and Responsibilities:**

    *   **extract_data:**
        *   Creates a pandas DataFrame with some sample product data (including a missing value in the `name` column) and saves it to `extracted_data.csv`.
    *   **transform_data:**
        *   Reads the `extracted_data.csv` file into a pandas DataFrame.
        *   Fills missing values in the `name` column with "Unknown Product".
        *   Calculates a `discounted_price` column.
        *   Saves the transformed data to `transformed_data.csv`.
    *   **load_data:**
        *   Retrieves the value of the `database_name` variable defined in Airflow.
        *   Establishes a connection to the `demo.db` SQLite database (or the database specified by the variable) within the directory defined by the `data_dir` Airflow variable.
        *   Reads the `transformed_data.csv` file into a DataFrame.
        *   Uses the `to_sql()` method to load the DataFrame into the `products` table.
        *   Closes the database connection.
    *   **validate_data:**
        *   Connects to the SQLite database.
        *   Queries the `products` table.
        *   Prints the result using `print()`.

**Part 4: Implement the Data Pipeline in Airflow (45 minutes)**

1.  **Create the DAG File:**

    *   In the same workspace terminal where you created the demo.db file, navigate to the /airflow directory with `cd ~/airflow`, and create the dags folder using `mkdir dags`.
    *   Navigate into it with `cd dags`.
    *   Open Visual Studio Code:

        ```bash
        code .
        ```

    *   Once it opens, create a new file inside the dags folder by right-clicking on it and selecting New File, save it as `etl_pipeline.py`. This is where you'll write the code for your DAG.

2.  **Import Necessary Modules:**

    ```python
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.sqlite.operators.sqlite import SqliteOperator
    from airflow.models import Variable
    from datetime import datetime
    import pandas as pd
    import sqlite3
    import os
    ```

3.  **Define Default Arguments and Environment Variables:**

    ```python
    default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,  # Keep it simple for the lab
    }
    ```

4.  **Create the DAG Instance:**

    ```python
    with DAG(
        dag_id='etl_pipeline',
        default_args=default_args,
        schedule_interval='@once',  # Run the pipeline only once
        catchup=False,  # Don't run past schedules
    ) as dag:
    ```

5. **Add DATA_DIR Variable at the Top:**
    ```python:etl_pipeline.py
    # Get data_dir from Airflow variables at the top level
    DATA_DIR = Variable.get("data_dir")
    ```

6. **Update the Extract Task:**
    ```python:etl_pipeline.py
    def extract_data_func(**kwargs):
        """Simulates extracting data from a CSV or uses sample data based on configuration."""
        product_data_source = Variable.get("product_data_source")

        try:
            if product_data_source == "sample_data":
                data = {'product_id': [1, 2, 3, 4],
                        'name': ['Product A', 'Product B', None, 'Product D'],
                        'price': [10, 20, 15, 25]}
                df = pd.DataFrame(data)
                extracted_data_path = f"{DATA_DIR}/extracted_data.csv"
                
                # Create directory if it does not exist
                os.makedirs(os.path.dirname(extracted_data_path), exist_ok=True)

                df.to_csv(extracted_data_path, index=False)
                print(f"Sample data extracted to {extracted_data_path}")
            else:
                raise NotImplementedError(f"Data source '{product_data_source}' not yet implemented.")
            return extracted_data_path
        except Exception as e:
            print(f"Error during data extraction: {e}")
            raise
    ```

7. **Define the `transform_data` Task with Error Handling:**

    ```python
        def transform_data_func(ti, **kwargs):
            """Transforms the extracted data."""
            extracted_data_path = ti.xcom_pull(task_ids='extract_data')
            transformed_data_path = f"{DATA_DIR}/transformed_data.csv"

            try:
                if not os.path.exists(extracted_data_path):
                    raise FileNotFoundError(f"Extracted data file not found: {extracted_data_path}")

                df = pd.read_csv(extracted_data_path)
                # Handle missing values (fill with a default value)
                df['name'].fillna('Unknown Product', inplace=True)
                # Add a new column: 'discounted_price'
                df['discounted_price'] = df['price'] * 0.9
                df.to_csv(transformed_data_path, index=False)
                print(f"Data transformed and saved to {transformed_data_path}")
                return transformed_data_path
            except FileNotFoundError as fnfe:
                print(fnfe)
                raise
            except Exception as e:
                print(f"Error during data transformation: {e}")
                raise
    

        transform_task = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data_func,
        )
    ```

8. **Define the `load_data` Task with Error Handling:**

    ```python
        def load_data_func(ti, **kwargs):
            """Loads the transformed data into SQLite."""
            transformed_data_path = ti.xcom_pull(task_ids='transform_data')
            db_name = Variable.get("database_name")
            db_path = f"{DATA_DIR}/{db_name}"
            
            try:
                if not os.path.exists(transformed_data_path):
                    raise FileNotFoundError(f"Transformed data file not found: {transformed_data_path}")
                conn = sqlite3.connect(db_path)
                df = pd.read_csv(transformed_data_path)
                df.to_sql('products', conn, if_exists='replace', index=False)
                conn.close()
                print(f"Data loaded into 'products' table in {db_path}")

            except FileNotFoundError as fnfe:
                print(fnfe)
                raise
            except Exception as e:
                print(f"Error during data loading: {e}")
                raise
    

        load_task = PythonOperator(
            task_id='load_data',
            python_callable=load_data_func,
        )
    ```

9. **Define the `validate_data` task:**

    ```python
        validate_task = SqliteOperator(
            task_id='validate_data',
            sqlite_conn_id='my_sqlite_conn',
            sql="""
                SELECT COUNT(*) FROM products;
            """
        )
    ```

10. **Set Task Dependencies:**

    ```python
        extract_task >> transform_task >> load_task >> validate_task
    ```

**Part 5: Parameterize the DAG (15 minutes)**

1.  **Create Airflow Variables:**

    *   Go to `Admin` > `Variables` in the Airflow UI.
    *   Create these variables:
        *   **Key:** `product_data_source` **Val:** `sample_data`
        *   **Key:** `database_name` **Val:** `demo.db`
        *   **Key:** `data_dir` **Val:** `/home/du_XXXXXX-YYYYYYYYYY/data`

2.  **Important Note About Variables:**
    *   The code now uses a global `DATA_DIR` variable that's set at DAG load time
    *   All file paths are constructed using this `DATA_DIR` variable
    *   The database connection uses the combination of `data_dir` and `database_name` variables

**Part 6: Run and Monitor the Pipeline (15 minutes)**

1.  **Save the DAG File:**
    *   Make sure you have saved the updated `etl_pipeline.py` file in the `~/airflow/dags` directory.

2.  **Unpause and Trigger the DAG:**
    *   Go back to the Airflow UI in your web browser.
    *   Go to the DAGs tab.
    *   Click the toggle switch to unpause (turn on) the `etl_pipeline` DAG if it's paused.
    *   Click on the `etl_pipeline` DAG's name to go to its details page (the Graph View).
    *   Click the "Trigger DAG" button (it looks like a play button) in the top right corner.
    *   A small popup might appear asking for confirmation; click "Trigger".

3.  **Monitor Execution:**

    *   **Graph View:**
        *   Observe the DAG run in the Graph View. The squares representing the tasks will change colors as they are executed:
            *   **Light Green (Queued):** The task is waiting to be run.
            *   **Dark Green (Running):** The task is currently executing.
            *   **Dark Red (Failed):** The task has failed.
            *   **Light Red (Up for Retry):** The task failed but will be retried.
            *   **Turquoise (Scheduled):** Task is scheduled but dependencies are not met.
            *   **White (No Status):** Task has not run yet.
        *   You can click on a task square to view its details (logs, etc.).
    *   **Tree View:**
        *   Switch to the Tree View to see a hierarchical representation of the DAG run. You can expand the DAG run to see the status of each task instance.

4.  **View Task Logs:**

    *   Click on a task instance (a colored square in either the Graph View or Tree View).
    *   Go to the "Logs" tab to see the output generated by the task.
    *   **Important:** Pay close attention to the logs to understand what is happening in each step.

5.  **Verify Results:**

    *   Once the DAG run is successful (all tasks are dark green), use an SQLite browser (or the `sqlite3` command-line tool) to connect to the `demo.db` database:
        *   **Important:** Use the same terminal where you started airflow and navigated to `~/airflow/dags`.
        ```bash
        sqlite3 $DATA_DIR/demo.db
        ```
    *   Run the following SQL query to verify that the data has been loaded correctly:

        ```sql
        SELECT * FROM products;
        ```

        You should see the `product_id`, `name`, `price`, and `discounted_price` columns with the transformed data.
    *   Exit the `sqlite3` shell by typing `.exit`.
    *   Go to the task logs for `validate_data` and check the output. You should see the number of rows that were added.

**Part 7: Further Learning**

*   **Add a Data Validation Step:**
    *   Create a new task (e.g., `validate_data_quality`) that performs data validation checks on the transformed data *before* loading it into the database.
    *   Examples of validation checks:
        *   **Check for null values:** Ensure that certain columns don't have any missing values.
        *   **Check data types:** Verify that columns have the expected data types (e.g., `price` should be numeric).
        *   **Check value ranges:** Ensure that values fall within acceptable ranges (e.g., `price` should be greater than 0).
        *   **Check for duplicates:** Make sure there are no duplicate rows based on a primary key.

*   **Use XComs for Inter-Task Communication:**
    *   Use XComs (cross-communication) to pass more complex data between tasks instead of writing to intermediate files. You already have some examples in the code.

*   **Explore Different Operators:**
    *   Experiment with other Airflow operators like `BashOperator` (to run shell commands), `EmailOperator` (to send notifications), or operators for interacting with cloud services (e.g., `S3Hook`, `BigQueryHook`).

*   **Implement Dynamic DAG Generation:**
    *   Learn how to dynamically generate DAGs based on configuration files or external data sources. This is useful when you have many similar pipelines that only differ in certain parameters.

**Conclusion:**

*   **Review Key Concepts:**
    *   DAGs, Operators (especially `PythonOperator`), Tasks, and task dependencies.
    *   The ETL process (Extract, Transform, Load).
    *   Using SQLite as a data source and destination.
    *   The Airflow UI for monitoring and managing workflows.
    *   Error handling and data validation.
    *   Parameterizing DAGs with Airflow Variables.
    *   The importance of installing Airflow with constraints using pip.

*   **Importance of Data Pipelines:**
    *   Discuss how data pipelines are essential for automating data-related tasks in real-world scenarios.
    *   Mention examples of how data pipelines are used in various industries (e.g., e-commerce, finance, healthcare).

*   **Further Exploration:**
    *   Encourage students to explore more advanced Airflow features mentioned above.
    *   Suggest building more complex pipelines that interact with other data sources and destinations (e.g., cloud storage, APIs, other databases).
    *   Recommend exploring the Airflow documentation and community resources for further learning.
    *   Point students to online courses, tutorials, and articles about Apache Airflow and data engineering.
