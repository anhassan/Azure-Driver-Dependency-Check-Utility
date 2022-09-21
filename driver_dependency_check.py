# Databricks notebook source
import os
from pathlib import Path
from datetime import datetime
from pyspark.sql.functions import col
import pytz


def get_file_modified_times(table_loc,file_modified_tm):
  for child in Path(table_loc).iterdir():
      
        if child.is_file():
            file_modified_tm.append(datetime.fromtimestamp(os.path.getmtime(child)))
        else:
            get_file_modified_times(child,file_modified_tm)
  

def get_non_delta_refresh_time(table_loc):  
    
    file_modified_tm = []
    get_file_modified_times(table_loc,file_modified_tm)
            
    return max(file_modified_tm)


def get_delta_refresh_time(table_loc):
  return spark.sql("DESCRIBE DETAIL '{}'".format(table_loc))\
              .select("lastModified")\
              .collect()[0]["lastModified"]

def get_table_refresh_time(table_name):
    table_meta_df = spark.sql("DESCRIBE EXTENDED {}".format(table_name))
    
    try:
        table_type = table_meta_df.where(col("col_name")=="Provider")\
                                  .collect()[0]["data_type"]
        table_loc = table_meta_df.where(col("col_name")=="Location")\
                                 .collect()[0]["data_type"]
        
        if "dbfs" not in table_loc:
          table_loc_list = table_meta_df.where(col("col_name")=="Location").collect()
          table_loc = table_loc_list[-1]["data_type"]
          
    except Exception as error:
        table_type = table_meta_df.where(col("col_name") == "Type")\
                                  .collect()[0]["data_type"]
        if table_type == "VIEW":
          return datetime.min
        else:
          raise("Error : {}".format(error))    
    
    if table_type == "delta":
      refresh_time = get_delta_refresh_time(table_loc)
    else:
      refresh_time = get_non_delta_refresh_time(table_loc.replace("dbfs:","/dbfs"))
    
    return refresh_time.astimezone(tz=pytz.timezone('US/Central'))
  

# COMMAND ----------

def get_all_ingested_tables(schemas=[]):
  if len(schemas) == 0:
    schemas_df = spark.sql("SHOW DATABASES")
    schemas = [row.databaseName for row in schemas_df.collect()]
  tables = []
  for schema in schemas:
    tables_df  = spark.sql("SHOW TABLES IN {}".format(schema))
    tables_schema = ["{}.{}".format(schema,row.tableName).upper() for row in tables_df.collect()]
    tables.extend(tables_schema)
  return tables

# COMMAND ----------

import base64

def get_code(repo,path,project="Quantum",organization="EntDataProd"):
  
  url = "https://dev.azure.com/{}/{}/_apis/git/repositories/{}/items?path={}&includeContent={}&api-version=6.1-preview.1" \
         .format(organization,project,repo,path,True,True)
  pat_token = "t*******************************************4q"

  authorization = str(base64.b64encode(bytes(':'+pat_token, 'ascii')), 'ascii')

  headers = {
      'Content-type': 'application/json',
      'Authorization': 'Basic' + authorization
  }
  try:
      response = requests.get(url,headers=headers)
      code = response.content.decode("ascii")
  except:
      code = ""
  return code

# COMMAND ----------

from datetime import datetime, date
from statistics import mean
import datetime as dt

def get_avg_tm(time_objs,avg):
  time_objs = time_objs if avg else [time_objs[0]]
  avg_seconds = mean([(datetime.combine(date.min,time_obj) - datetime.min).total_seconds() for time_obj in time_objs])
  str_time = str(dt.timedelta(seconds=avg_seconds))
  return str_time[0:str_time.rfind(".")]
  

# COMMAND ----------


import json
import requests

def get_workflow_meta(repo_name,workflow_name,avg):
  
    databricks_instance_name = dbutils.secrets.get(scope="databricks-credentials",key="databricks-instance-name")
    headers={"Authorization": "Bearer {}".format(dbutils.secrets.get(scope="switch-ownership",key="databricks-auth-token"))}

    jobs_list_response = json.loads(requests.get("https://{}/api/2.1/jobs/list".format(databricks_instance_name),headers = headers).content)["jobs"]
    
    try:
      job_id = [job["job_id"] for job in jobs_list_response if job["settings"]["name"] == workflow_name][0]
    except Exception as error:
      raise Exception("Workflow : {} does not exists in Repo : {}".format(workflow_name,repo_name))
      
    job_meta_response = json.loads(requests.get("https://{}/api/2.1/jobs/get?job_id={}".format(databricks_instance_name,job_id),headers = headers).content)
    notebook_full_paths = [element["notebook_task"]["notebook_path"] + ".py" for element in job_meta_response["settings"]["tasks"] ]
    notebook_paths = [path[path.find(repo_name) + len(repo_name):] for path in notebook_full_paths]
    
    try:
      job_runs_meta = json.loads(requests.get("https://{}/api/2.1/jobs/runs/list?job_id={}".format(databricks_instance_name,job_id),headers = headers).content)["runs"]
    except Exception as error:
      print("No runs exist for Workflow : {} ".format(workflow_name))
      return notebook_paths,""
      
    job_exec_datetimes = [datetime.utcfromtimestamp(int(element["start_time"])/1000) for element in job_runs_meta]
    job_exec_times = [obj.time() for obj in job_exec_datetimes]
    job_exec_avg_time = get_avg_tm(job_exec_times,avg)
    job_exec_avg_datetime = datetime.combine(job_exec_datetimes[0].date(),datetime.strptime(job_exec_avg_time,"%H:%M:%S").time()) 
    
    return notebook_paths,job_exec_avg_datetime


# COMMAND ----------


import pandas as pd

def get_workflow_dependencies(repo_name,workflow_name,init_workflow_exec_datetime="",avg=False):
  
  workflow_notebooks,workflow_exec_datetime_hist = get_workflow_meta(repo_name,workflow_name,avg)
  
  if isinstance(workflow_exec_datetime_hist,str) and len(workflow_exec_datetime_hist) == 0 and len(init_workflow_exec_datetime) == 0 :
    raise Exception("Please add a placeholder time for workflow execution date time for Workflow : {} as it has no Runs".format(workflow_name))
    
  workflow_exec_datetime = datetime.strptime(init_workflow_exec_datetime, "%Y-%m-%d %H:%M:%S") if len(init_workflow_exec_datetime) != 0 \
                           else workflow_exec_datetime_hist.astimezone(tz=pytz.timezone('US/Central'))
  
  if datetime.now().date() > workflow_exec_datetime.date():
    print("WARNING Workflow : {} did not get executed today....".format(workflow_name))
  
  ingested_tables = get_all_ingested_tables()
  workflow_tables = []
  tables_refresh_tms = []
  for notebook_path in workflow_notebooks:
    notebook_code = get_code(repo_name,notebook_path).upper()
    notebook_tables = [table for table in ingested_tables if table in notebook_code]
    workflow_tables += notebook_tables
    tables_refresh_tms += [get_table_refresh_time(table) for table in notebook_tables]

  change_tables = [workflow_tables[index] for index,refresh_time in enumerate(tables_refresh_tms) if refresh_time.time() > workflow_exec_datetime.time()]
  dependency_report_data = [[workflow_tables[index],tables_refresh_tms[index],workflow_name,workflow_exec_datetime] for index,table in enumerate(workflow_tables)]
  dependency_report_data_enriched = [report_data + ["Requires Change"] if report_data[1].time() > report_data[3].time() else report_data + ["No Change"] for report_data in dependency_report_data]
  dependency_report_df = spark.createDataFrame(pd.DataFrame(dependency_report_data_enriched,columns=["Table","RefreshTime","WorkflowName","WorkflowExecutionTime","ChangeStatus"]))
  
  return workflow_tables,change_tables,dependency_report_df


# COMMAND ----------

print("Loaded Driver Dependency Check Utility")
