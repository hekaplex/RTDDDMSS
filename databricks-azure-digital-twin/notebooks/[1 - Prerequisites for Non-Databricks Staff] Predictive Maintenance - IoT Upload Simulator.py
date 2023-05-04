# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC #### 1. Retrieve sample data

# COMMAND ----------

# MAGIC %pip install azure-storage-blob

# COMMAND ----------

# DBTITLE 1,Load example data from IoT Device
import pandas as pd

storage_account = "pawaritstorageaccount"
vibration_reports_path = f"https://{storage_account}.blob.core.windows.net/public/digital-twin-gtm/data/model_development/vibration_reports.csv"

full_df = pd.read_csv(vibration_reports_path)

# COMMAND ----------

no_fault_df = full_df[full_df["fault"].str.contains("Normal")].drop("fault", axis=1)
fault_df = full_df[full_df["fault"].str.contains("Ball_007_1")].drop("fault", axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 2. Upload as different files (simulating different devices) to an Azure Blob Storage Container

# COMMAND ----------

# DBTITLE 1,If you want to access a specific container, use a SAS token
sas_token = "?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-08T03:06:04Z&st=2023-04-12T19:06:04Z&spr=https&sig=5jxBrv9M0aF0X1F%2FHOXB633xwmooZZqUpFIT5xfivw0%3D" # TODO: use a SAS token or storage account key to upload data to your storage container

if sas_token == Ellipsis:
  print("Please provide a SAS token for your landing zone's storage container.")
  dbutils.notebook.exit("Please provide a SAS token for your landing zone's storage container.")

# COMMAND ----------

# DBTITLE 1,Use the Azure SDK to interact with the storage container
from azure.storage.blob import ContainerClient
storage_account = "aminkinmctadls"
blob_storage_account = "aminkinmctadls" # TODO: please change to your own storage account
blob_storage_container = "aminkinmctadlsfs" # TODO: please change to your own storage container
url = f"https://{storage_account}.blob.core.windows.net/" + blob_storage_container

container_client = ContainerClient.from_container_url(
    container_url=url,
    credential=sas_token
)

# COMMAND ----------

url


# COMMAND ----------

type(no_fault_df)

# COMMAND ----------

import pyspark.pandas
sparkdf_fault = spark.createDataFrame(fault_df)
sparkdf_fault.write.format("delta").mode("overwrite").save("/tmp/demodata_fault")
sparkdf_no_fault = spark.createDataFrame(no_fault_df)
sparkdf_no_fault.write.format("delta").mode("overwrite").save("/tmp/demodata_nofault")

# COMMAND ----------

spark_no_fault_df = spark.read.format("delta").load("/tmp/demodata_nofault")
spark_fault_df = spark.read.format("delta").load("/tmp/demodata_fault")

# COMMAND ----------

# DBTITLE 1,Now, let's upload our arbitrary files to this container
import time
no_fault_df = spark_no_fault_df.toPandas()
fault_df = spark_fault_df.toPandas()

max_uploads = 50
upload_interval = 1 # in seconds
t_failure = 30 # seconds until "Mixing Station failure"
i_failure = int(t_failure / upload_interval) # iteration for failure to occur at

for i in range(1, max_uploads+1):
  
  chosen_df = no_fault_df if (i < i_failure) else fault_df
  feature_df = chosen_df.sample(n=1, random_state=i).reset_index(drop=True)
  
  container_client.upload_blob(
    name=f"/digital-twin/data/landing_zone/device_upload_{str(i).zfill(4)}.csv", 
    data=feature_df.to_csv(index=False), 
    overwrite=True
  )
  
  time.sleep(upload_interval)

# COMMAND ----------

# DBTITLE 1,[Clean-Up] delete the files we previously uploaded for the demo
import pandas as pd
def get_blob_list():
  return [
    {k: b[k] for k in ["name", "last_modified"]} for b in 
    container_client.list_blobs(
      name_starts_with="digital-twin/data/landing_zone/"
    )
  ]
  
# Uncomment the code below to delete previously uploaded files

blob_list_before = get_blob_list()
print("BEFORE:")
print(pd.DataFrame(blob_list_before))

try:
  blobs_deleted = 0
  for blob in blob_list_before:
    container_client.delete_blob(blob["name"])
    blobs_deleted += 1
  print()
  print("Blobs Deleted:", blobs_deleted)
  print()
except:
  pass

print("AFTER:")
print(pd.DataFrame(get_blob_list()))

# COMMAND ----------

