# Load libraries
#from azure.storage.blob import BlobClient
import pandas as pd
import glob
# Define parameters
connectionString = "DefaultEndpointsProtocol=https;AccountName=storagedatalakeefrei;AccountKey=PplLR3u7llr4SHSK//n51fnRB2OYarlzXD0qVUxtys4GD8AhGitWG6PLbl9mHoWrEVcCy3CMPgjJ+AStppRGIA==;EndpointSuffix=core.windows.net"
containerName = "lab1output"
outputBlobName	= "totalstocks.csv"

# Establish connection with the blob storage account
blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)

# Load  dataset from the task node

csv_files = glob.glob('*.{}'.format('csv'))
df = pd.DataFrame()
#append all files together
for file in csv_files:
            df_temp = pd.read_csv(file)
            df= df.append(df_temp, ignore_index=True)
df.head()



# Save the subset of the dataframe locally in task node
df.to_csv(outputBlobName, index = False)

with open(outputBlobName, "rb") as data:
    blob.upload_blob(data)

"""
import pandas as pd
import glob
csv_files = glob.glob('*.{}'.format('csv'))
df_append = pd.DataFrame()
#append all files together
for file in csv_files:
            df_temp = pd.read_csv(file)
            df_append = df_append.append(df_temp, ignore_index=True)
df_append.head()
"""
