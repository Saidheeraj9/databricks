# Databricks notebook source
# dbutils.fs.mount(
#     source= 'wasbs://raw@storedata1.blob.core.windows.net',
#     mount_point="/mnt/raw",
#     extra_configs={
#         'fs.azure.account.key.storedata1.blob.core.windows.net': 'eHVZr1Jznk5gkSgQ7czsuEyopr1EdDR02eAbO0fM0bllEi5LhcTcj/thQm152f2i8RXSeGvF1wvi+AStQCTO5Q=='
#     }
# )

# COMMAND ----------

df_cleanedsales = spark.read.format("csv").option("header", "true").load("/mnt/raw/dbo/cleaned_sales_data.csv")


# COMMAND ----------

df_external = spark.read.format("csv").option("header", "true").load("/mnt/raw/dbo/external_data.csv")
df_integrated = spark.read.format("csv").option("header", "true").load("/mnt/raw/dbo/integrated_data.csv")
df_kpi = spark.read.format("csv").option("header", "true").load("/mnt/raw/dbo/kpi_metrics.csv")
df_appointment = spark.read.format("csv").option("header", "true").load("/mnt/raw/dbo/visualization_data.csv")

# COMMAND ----------

display(df_cleanedsales)

# COMMAND ----------

display(df_cleanedsales.join(df_integrated,'cleaned_sales_id',"inner"))

# COMMAND ----------

display(df_cleanedsales.join(df_integrated,'cleaned_sales_id'))

# COMMAND ----------

display(df_cleanedsales.join(df_integrated,'cleaned_sales_id',how="left"))

# COMMAND ----------

display(df_cleanedsales.join(df_integrated,'cleaned_sales_id',how="right"))

# COMMAND ----------

display(df_cleanedsales.join(df_integrated,'cleaned_sales_id',how="cross"))

# COMMAND ----------

display(df_cleanedsales.join(df_integrated,'cleaned_sales_id',how="cross"))

# COMMAND ----------

