%python

df_spark = spark.sql("select *from part_00000_tid_3129013980174629215_6547fa11_12e7_4a0b_85d3_27bdddeca58a_6_1_c000_csv")

df_spark.show()


%python
storage_account_name = "hrdbtsfordbricks1"
storage_account_access_key = "k/8FF04RbamDlQksj0zqo7v2xeglfQiUpuUjf5wGpxKDQLmzR8mWAZNKb1rYol1eBAZBL5Xag6AO+AStbepuYQ=="
container_name = "tsblobfordbricks1"


%python
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key
)

%python
container_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
blob_folder = f"{container_path}/tsblobfordbricks1"
print(blob_folder)

%python

output_container_path = f"wasbs://tsblobfordbricks1@hrdbtsfordbricks1.blob.core.windows.net/tsblobfordbricks1"

df_spark.coalesce(1).write \
  .format("com.databricks.spark.csv") \
  .mode("overwrite") \
  .option("header", "true") \
  .option("delimiter", "|") \
  .option("emptyValue", "") \
  .save(output_container_path)