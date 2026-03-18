import pandas as pd
import dask.dataframe as dd
import time
import os

file = "../2019-Oct.csv"

# =====================================
#  METHOD 1 : PANDAS WITH CHUNKS
# =====================================
print("\n===== METHOD 1 : Pandas Chunk Processing =====")

start = time.time()

chunksize = 100000
total_purchases = 0

for chunk in pd.read_csv(file, chunksize=chunksize):

    purchases = chunk[chunk["event_type"] == "purchase"]
    total_purchases += len(purchases)

end = time.time()

pandas_time = end - start

print("Total purchases:", total_purchases)
print("Execution time:", pandas_time, "seconds")


# =====================================
#  METHOD 2 : DASK
# =====================================
print("\n===== METHOD 2 : Dask Processing =====")

start = time.time()

df = dd.read_csv(file)

purchases = df[df["event_type"] == "purchase"]

total = purchases.shape[0].compute()

end = time.time()

dask_time = end - start

print("Total purchases:", total)
print("Execution time:", dask_time, "seconds")


# =====================================
#  METHOD 3 : COMPRESSION

# =====================================
print("\n===== METHOD 3 : Compression =====")

start = time.time()

df = pd.read_csv(file)


compressed_file = "2019-Oct-compressed.csv.gz"

df.to_csv(compressed_file, index=False, compression="gzip")

end = time.time()

compression_time = end - start

original_size = os.path.getsize(file) / (1024*1024)
compressed_size = os.path.getsize(compressed_file) / (1024*1024)

print("Execution time:", compression_time, "seconds")
print("Original size:", round(original_size,2), "MB")
print("Compressed size:", round(compressed_size,2), "MB")


# =====================================
# COMPARISON
# =====================================

print("\n===== COMPARISON =====")

print("Pandas chunk time :", round(pandas_time,2), "seconds")
print("Dask time :", round(dask_time,2), "seconds")
print("Compression time :", round(compression_time,2), "seconds")

print("Storage before compression :", round(original_size,2), "MB")
print("Storage after compression :", round(compressed_size,2), "MB")