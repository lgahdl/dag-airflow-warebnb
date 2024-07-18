#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage DAG params to model a trigger UI with a user form.

This example DAG generates greetings to a list of provided names in selected languages in the logs.
"""

from __future__ import annotations

import datetime
import pandas as pd
import sqlalchemy
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

with DAG(
    dag_id=Path(__file__).stem,
    dag_display_name="Transfer Sheet to CockroachDB(PG)",
    schedule=None,
    start_date=None,
    tags=["params"],
    params={
        "sheet_link": Param(
            "https://docs.google.com/spreadsheets/d/1PiSWxC-MHhwhraXVenRNvZ--akOlzvVwkYyZwmJYavY/export?format=csv",
            type="string",
            description="type the address to the spreadsheet with export?format=csv in the end",
            title="Sheet Link",
        ),
        "pg_params": Param(
            ["round-fairy-7625.g8z.gcp-us-east1.cockroachlabs.cloud", "26257", "defaultdb", "warebnb", "postgres", "9ArGo5tbh6WR6w-IrVfjMw"],
            type="array",
            description="PostgreSQL Params",
            title="PostgreSQL Params",
        )
    },
) as dag:


    @task(task_id="create_general_dataframe", task_display_name="Create Dataframes")
    def create_general_dataframe(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        sheet_link = dag_run.conf["sheet_link"]
        df = pd.read_csv(sheet_link, dtype={
            "id":"int64", 
            "neighbourhood":"string",
            "latitude": "string",
            "longitude": "string",
            "host_acceptance_rate":"string",
            "host_is_superhost":"string",
            "host_neighbourhood":"string",
            "host_listings_count":"string",
            "property_type":"string",
            "room_type":"string",
            "accommodates":"string",
            "bathrooms":"float32",
            "bedrooms":"string",
            "beds":"string",
            "number_of_reviews":"string",
            "review_scores_rating":"string",
            "review_scores_accuracy":"string",
            "review_scores_cleanliness":"string",
            "review_scores_checkin":"string",
            "review_scores_communication":"string",
            "review_scores_location":"string",
            "review_scores_value":"string",
            "price":"string",
            }, na_values='')
        return {"df": df}

    @task(task_id="generate_dimension_host_df", task_display_name="Generate Dimension Host Dataframe")
    def generate_dimension_host_df(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        df = ti.xcom_pull(task_ids='create_general_dataframe')["df"]
        df_dimension_host = df.loc[:,("id","host_acceptance_rate", "host_is_superhost", "host_neighbourhood", "host_listings_count")]
        df_dimension_host["id_dimension_host"] = df_dimension_host["id"]
        df_dimension_host['host_is_superhost'] = df_dimension_host['host_is_superhost'].map({'t': True, 'f': False})
        df_dimension_host['host_acceptance_rate'] = df_dimension_host['host_acceptance_rate'].fillna('0%')
        df_dimension_host['host_acceptance_rate'] = df_dimension_host['host_acceptance_rate'].str.replace('%', '').astype(float) / 100
        df_dimension_host['host_listings_count'] = df_dimension_host['host_listings_count'].fillna('1')
        df_dimension_host['host_listings_count'] = df_dimension_host['host_listings_count'].astype(int)
        df_dimension_host_with_correct_id_name = df_dimension_host[["id_dimension_host","host_acceptance_rate", "host_is_superhost",
                                                                     "host_neighbourhood", "host_listings_count"]]
        return {"df_dimension_host": df_dimension_host_with_correct_id_name}

    @task(task_id="generate_dimension_local_df", task_display_name="Generate Dimension Local Dataframe")
    def generate_dimension_local_df(**kwargs):
        def convert_lat_long(value):
            # Garantir que o valor seja uma string
            str_value = str(value).replace(".","").replace("-","")
        
            # Inserir o ponto decimal na posição correta
            if len(str_value) >= 8:
                formatted_value = str_value[:2] + '.' + str_value[2:9]
            else:
                formatted_value = str_value[:2] + '.' + str_value[2:].ljust(7, '0')
            
            # Converter para float, adicionar o sinal negativo e formatar com 5 casas decimal
            return -float(formatted_value)
        ti: TaskInstance = kwargs["ti"]
        df = ti.xcom_pull(task_ids='create_general_dataframe')["df"]
        df_dimension_local = df.loc[:,("id", "neighbourhood_cleansed", "latitude", "longitude")]
        df_dimension_local["id_dimension_local"] = df_dimension_local["id"]
        df_dimension_local["neighbourhood"] = df_dimension_local["neighbourhood_cleansed"]
        df_dimension_local["latitude"] = df_dimension_local["latitude"].apply(convert_lat_long)
        df_dimension_local["longitude"] = df_dimension_local["longitude"].apply(convert_lat_long)
        df_dimension_local_with_correct_id_name = df_dimension_local[["id_dimension_local","neighbourhood", "latitude", "longitude"]]
        return {"df_dimension_local": df_dimension_local_with_correct_id_name}

    @task(task_id="generate_dimension_property_df", task_display_name="Generate Dimension Property Dataframe")
    def generate_dimension_property_df(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        df = ti.xcom_pull(task_ids='create_general_dataframe')["df"]
        df_dimension_property = df.loc[:,("id", "property_type", "room_type", "accommodates", "bathrooms_text", "bedrooms", "beds")]
        df_dimension_property["id_dimension_property"] = df_dimension_property["id"]
        df_dimension_property['accommodates'] = pd.to_numeric(df_dimension_property['accommodates'], errors='coerce')
        df_dimension_property['bathrooms'] = df_dimension_property['bathrooms_text'].str.extract(r'(\d+(\.\d+)?)')[0]
        df_dimension_property['bathrooms'] = pd.to_numeric(df_dimension_property['bathrooms'], errors='coerce')
        df_dimension_property['beds'] = pd.to_numeric(df_dimension_property['beds'], errors='coerce')
        df_dimension_property_with_correct_id_name = df_dimension_property[["id_dimension_property", "property_type", "room_type", "accommodates", "bathrooms", "beds"]]
        return {"df_dimension_property": df_dimension_property_with_correct_id_name}


    @task(task_id="generate_dimension_reviews_df", task_display_name="Generate Dimension Reviews Dataframe")
    def generate_dimension_reviews_df(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        df = ti.xcom_pull(task_ids='create_general_dataframe')["df"]
        df_dimension_reviews = df.loc[:, ("id","review_scores_rating","number_of_reviews", "review_scores_accuracy", "review_scores_cleanliness", "review_scores_checkin", "review_scores_communication", "review_scores_location", "review_scores_value")]
        df_dimension_reviews["id_dimension_reviews"] = df_dimension_reviews["id"]
        df_dimension_reviews["number_of_reviews"] = pd.to_numeric(df_dimension_reviews["number_of_reviews"], errors='coerce')
        df_dimension_reviews["review_scores_rating"] = pd.to_numeric(df_dimension_reviews["review_scores_rating"], errors='coerce').round(2)
        df_dimension_reviews["review_scores_accuracy"] = pd.to_numeric(df_dimension_reviews["review_scores_accuracy"], errors='coerce').round(2)
        df_dimension_reviews["review_scores_cleanliness"] = pd.to_numeric(df_dimension_reviews["review_scores_cleanliness"], errors='coerce').round(2)
        df_dimension_reviews["review_scores_checkin"] = pd.to_numeric(df_dimension_reviews["review_scores_checkin"], errors='coerce').round(2)
        df_dimension_reviews["review_scores_communication"] = pd.to_numeric(df_dimension_reviews["review_scores_communication"], errors='coerce').round(2)
        df_dimension_reviews["review_scores_location"] = pd.to_numeric(df_dimension_reviews["review_scores_location"], errors='coerce').round(2)
        df_dimension_reviews["review_scores_value"] = pd.to_numeric(df_dimension_reviews["review_scores_value"], errors='coerce').round(2)
        df_dimension_reviews_with_correct_id_name = df_dimension_reviews[["id_dimension_reviews", "number_of_reviews", "review_scores_rating", "review_scores_accuracy", "review_scores_cleanliness", "review_scores_checkin", "review_scores_communication", "review_scores_location", "review_scores_value"]]
        return {"df_dimension_reviews": df_dimension_reviews_with_correct_id_name}

    @task(task_id="generate_fact_price_df", task_display_name="Generate Fact Price Dataframe")
    def generate_fact_price_df(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        df = ti.xcom_pull(task_ids='create_general_dataframe')["df"]
        df_fact_price = df.loc[:,("id", "price")]
        df_fact_price["id_fact_price"] = df_fact_price["id"]
        df_fact_price["price"] = df_fact_price["price"].str.replace('$', '')
        df_fact_price["price"] = df_fact_price["price"].str.replace(',', '')
        df_fact_price["price"] = pd.to_numeric(df_fact_price["price"]).round(2)
        df_fact_price_with_correct_id_name = df_fact_price[["id_fact_price", "price"]]
        return {"df_fact_price": df_fact_price_with_correct_id_name}


    @task(task_id="insert_dimension_host", task_display_name="Insert Dimension Host in the Database")
    def insert_dimension_host(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        df_dimension_host: pd.DataFrame = ti.xcom_pull(task_ids='generate_dimension_host_df')["df_dimension_host"]

        pg_params = dag_run.conf["pg_params"]
        db_params={
            "host": pg_params[0],
            "port":pg_params[1],
            "database":pg_params[2],
            "schema": pg_params[3],
            "user": pg_params[4],
            "password":pg_params[5]
        }
        engine = sqlalchemy.create_engine(f'cockroachdb://{db_params["user"]}:{db_params["password"]}' +
                                          f'@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
        df_dimension_host.to_sql(name='dimension_host', con=engine, if_exists='append', index=False, schema=db_params["schema"])
        return {"code": 200}
    
    @task(task_id="insert_dimension_local", task_display_name="Insert Dimension Local in the Database")
    def insert_dimension_local(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        df_dimension_local: pd.DataFrame = ti.xcom_pull(task_ids='generate_dimension_local_df')["df_dimension_local"]

        pg_params = dag_run.conf["pg_params"]
        db_params={
            "host": pg_params[0],
            "port":pg_params[1],
            "database":pg_params[2],
            "schema": pg_params[3],
            "user": pg_params[4],
            "password":pg_params[5]
        }
        engine = sqlalchemy.create_engine(f'cockroachdb://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
        df_dimension_local.to_sql(name='dimension_local', con=engine, if_exists='append', index=False, schema=db_params["schema"])
        return {"code": 200}
    
    @task(task_id="insert_dimension_property", task_display_name="Insert Dimension Property in the Database")
    def insert_dimension_property(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        df_dimension_property: pd.DataFrame = ti.xcom_pull(task_ids='generate_dimension_property_df')["df_dimension_property"]
        pg_params = dag_run.conf["pg_params"]
        db_params={
            "host": pg_params[0],
            "port":pg_params[1],
            "database":pg_params[2],
            "schema": pg_params[3],
            "user": pg_params[4],
            "password":pg_params[5]
        }
        engine = sqlalchemy.create_engine(f'cockroachdb://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
        df_dimension_property.to_sql(name='dimension_property', con=engine, if_exists='append', index=False, schema=db_params["schema"])
        return {"code": 200}
    
    @task(task_id="insert_dimension_reviews", task_display_name="Insert Dimension Reviews in the Database")
    def insert_dimension_reviews(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        df_dimension_reviews: pd.DataFrame = ti.xcom_pull(task_ids='generate_dimension_reviews_df')["df_dimension_reviews"]

        pg_params = dag_run.conf["pg_params"]
        db_params={
            "host": pg_params[0],
            "port":pg_params[1],
            "database":pg_params[2],
            "schema": pg_params[3],
            "user": pg_params[4],
            "password":pg_params[5]
        }
        engine = sqlalchemy.create_engine(f'cockroachdb://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
        df_dimension_reviews.to_sql(name='dimension_reviews', con=engine, if_exists='append', index=False, schema=db_params["schema"])
        return {"code": 200}
    
    @task(task_id="insert_fact_price", task_display_name="Insert Fact Price in the Database")
    def insert_fact_price(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        df_fact_price: pd.DataFrame = ti.xcom_pull(task_ids='generate_fact_price_df')["df_fact_price"]
        pg_params = dag_run.conf["pg_params"]
        db_params={
            "host": pg_params[0],
            "port":pg_params[1],
            "database":pg_params[2],
            "schema": pg_params[3],
            "user": pg_params[4],
            "password":pg_params[5]
        }
        engine = sqlalchemy.create_engine(f'cockroachdb://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
        df_fact_price.to_sql(name='fact_price', con=engine, if_exists='append', index=False, schema=db_params["schema"])
        return {"code": 200}

    create_general_dataframe_task = create_general_dataframe()
    generate_dimension_host_df_task = generate_dimension_host_df()
    generate_dimension_local_df_task =generate_dimension_local_df()
    generate_dimension_property_df_task = generate_dimension_property_df()
    generate_dimension_reviews_df_task = generate_dimension_reviews_df()
    generate_fact_price_df_task = generate_fact_price_df()
    insert_dimension_host_task = insert_dimension_host()
    insert_dimension_local_task = insert_dimension_local()
    insert_dimension_property_task = insert_dimension_property()
    insert_dimension_reviews_task = insert_dimension_reviews()
    insert_fact_price_task = insert_fact_price()
    create_general_dataframe_task >> [
        generate_dimension_host_df_task,  generate_dimension_local_df_task, generate_dimension_property_df_task,
        generate_dimension_reviews_df_task, generate_fact_price_df_task
    ]
    generate_dimension_host_df_task >> insert_dimension_host_task
    generate_dimension_local_df_task >> insert_dimension_local_task
    generate_dimension_property_df_task >> insert_dimension_property_task
    generate_dimension_reviews_df_task >> insert_dimension_reviews_task 
    [insert_dimension_host_task, insert_dimension_local_task, insert_dimension_property_task, insert_dimension_reviews_task] >> insert_fact_price_task