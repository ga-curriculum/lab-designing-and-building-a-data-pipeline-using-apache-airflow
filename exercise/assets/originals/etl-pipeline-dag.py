from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import sqlite3
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,  # Keep it simple for the lab
}

# Create the DAG instance
with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    schedule_interval='@once',  # Run the pipeline only once
    catchup=False,  # Don't run past schedules
) as dag:

    # Define the extract_data task with Error Handling and Parameterization
    def extract_data_func(**kwargs):
        """Simulates extracting data from a CSV or uses sample data based on configuration."""
        product_data_source = Variable.get("product_data_source")
        data_dir = Variable.get("data_dir") # Get data_dir from Airflow variables

        try:
            if product_data_source == "sample_data":
                data = {'product_id': [1, 2, 3, 4],
                        'name': ['Product A', 'Product B', None, 'Product D'],
                        'price': [10, 20, 15, 25]}
                df = pd.DataFrame(data)
                extracted_data_path = f"{data_dir}/extracted_data.csv" # Use data_dir here
                
                # Create directory if it does not exist
                os.makedirs(os.path.dirname(extracted_data_path), exist_ok=True)

                df.to_csv(extracted_data_path, index=False)
                print(f"Sample data extracted to {extracted_data_path}")
            else:
                # Future extension: Add logic here to read from other sources
                # For example, read from an external CSV file specified by product_data_source
                raise NotImplementedError(f"Data source '{product_data_source}' not yet implemented.")
            return extracted_data_path
        except Exception as e:
            print(f"Error during data extraction: {e}")
            raise

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_func,
    )

    # Define the transform_data task with Error Handling
    def transform_data_func(ti, **kwargs):
        """Transforms the extracted data."""
        extracted_data_path = ti.xcom_pull(task_ids='extract_data')
        data_dir = Variable.get("data_dir") # Get data_dir from Airflow variables
        transformed_data_path = f"{data_dir}/transformed_data.csv" # Use data_dir here

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

    # Define the load_data task with Error Handling and Parameterization
    def load_data_func(ti, **kwargs):
        """Loads the transformed data into SQLite."""
        transformed_data_path = ti.xcom_pull(task_ids='transform_data')
        data_dir = Variable.get("data_dir")
        db_name = Variable.get("database_name")
        db_path = f"{data_dir}/{db_name}"

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

    # Define the validate_data task
    validate_task = SqliteOperator(
        task_id='validate_data',
        sqlite_conn_id='my_sqlite_conn',
        sql="""
            SELECT COUNT(*) FROM products;
        """
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task >> validate_task
