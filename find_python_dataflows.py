hdb_address = ''
hdb_port = '443'
hdb_user = ''
hdb_password = ''
selected_space = ''

from hdbcli import dbapi # To connect to SAP HANA Database underneath SAP Datasphere to fetch Remote Table metadata
import pandas as pd

def get_dataflow_metadata(hdb_address, hdb_port, hdb_user, hdb_password, selected_space): 
    conn = dbapi.connect(
		address=hdb_address,
		port=hdb_port,
		user=hdb_user,
		password=hdb_password
	)
    cursor = conn.cursor()
    
    total_statement = f'''SELECT REPOSITORY_OBJECT_TYPE, NAME, DEPLOYED_AT FROM "{selected_space}$TEC".DEPLOYED_METADATA WHERE REPOSITORY_OBJECT_TYPE  = 'DWC_DATAFLOW';'''

    #print(selected_space)
    #print(total_statement)

    st = total_statement
    
    cursor.execute(st)
    
    fetched_data = cursor.fetchall()
    df = pd.DataFrame(fetched_data, columns=['TYPE', 'NAME', 'DEPLOYED_AT'])    
    return df

def get_json_dataflow(hdb_address, hdb_port, hdb_user, hdb_password, selected_space, dataflow_name):
    conn = dbapi.connect(
        address=hdb_address,
        port=hdb_port,
        user=hdb_user,
        password=hdb_password
    )
    cursor = conn.cursor()
    
    total_statement = f'''SELECT JSON FROM "{selected_space}$TEC".DEPLOYED_METADATA WHERE REPOSITORY_OBJECT_TYPE  = 'DWC_DATAFLOW' AND NAME = '{dataflow_name}';'''

    #print(total_statement)

    st = total_statement
    
    cursor.execute(st)
    
    fetched_data = cursor.fetchall()
    
    if fetched_data:
        return fetched_data[0][0]  # Return the JSON data
    else:
        return None

# Get a list of all Dataflows in the selected space
fetched_data = get_dataflow_metadata(hdb_address, hdb_port, hdb_user, hdb_password, selected_space) 
#print(fetched_data)

# Loop through the Dataflows and get the JSON for each Dataflow

for index, row in fetched_data.iterrows():
    dataflow_name = row['NAME']
    print(f"Processing Dataflow: {dataflow_name}")
    
    # Get the JSON for the Dataflow
    json_data = get_json_dataflow(hdb_address, hdb_port, hdb_user, hdb_password, selected_space, dataflow_name)
    #print(json_data)  # Print the JSON data for the Dataflow
    
    if json_data and "PythonOperator" in json_data:
        # If the Dataflow contains a PythonOperator, add it to the DataFrame
        fetched_data.at[index, 'ContainsPython'] = True
        
python_dataflows = fetched_data[fetched_data['ContainsPython'] == True]
print(python_dataflows)        
