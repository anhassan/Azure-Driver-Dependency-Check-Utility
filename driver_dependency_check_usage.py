# Databricks notebook source
# MAGIC %run ./driver_dependency_check

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Use Case 1 : No Workflow Exists***

# COMMAND ----------

workflow_name = "semantic_entity-se_product_dummy"
repo_name = "semantic_entity"

workflow_tables,change_tables,dependency_report_df = get_workflow_dependencies(repo_name,workflow_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Use Case 2 : No Workflow Runs Exist and No PlaceHolder time provided***

# COMMAND ----------

workflow_name = "semantic_entity-se_product_clone"
repo_name = "semantic_entity"

workflow_tables,change_tables,dependency_report_df = get_workflow_dependencies(repo_name,workflow_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Use Case 3 : No Workflow Runs Exist and but PlaceHolder time provided***

# COMMAND ----------

workflow_name = "semantic_entity-se_product_clone"
repo_name = "semantic_entity"

workflow_tables,change_tables,dependency_report_df = get_workflow_dependencies(repo_name,workflow_name,"2022-09-21 07:30:00")
display(dependency_report_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Use Case 4  : Standard - Workflow and Workflow Runs Exist***

# COMMAND ----------

workflow_name = "semantic_entity-se_product"
repo_name = "semantic_entity"

workflow_tables,change_tables,dependency_report_df = get_workflow_dependencies(repo_name,workflow_name)
display(dependency_report_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Use Case 5  : Workflow and Workflow Runs Exist but Workflow Execution Time needs to be changed***

# COMMAND ----------

workflow_name = "semantic_entity-se_product"
repo_name = "semantic_entity"

workflow_tables,change_tables,dependency_report_df = get_workflow_dependencies(repo_name,workflow_name,"2022-09-21 08:30:00")
display(dependency_report_df)
