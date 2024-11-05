import pandas as pd
import requests
import pysolr
import json

base_url = "http://localhost:8989/solr/"


def createCollection(p_collection_name):
    url = "admin/collections?action=create&name="+p_collection_name+"&numShards=1"
    res = requests.get(base_url+url)

    if(res.status_code==200):
        print("Collection created")
    else:
        print("Collection Creation Failed")
        print(res.content)

def indexData(p_collection_name, p_exclude_column):
    path = 'empdata.csv'  
    df = pd.read_csv(path,encoding='ISO-8859-1')

    try:
        errcol = [ 'Exit Date']
        for col in errcol:
            df = df.drop(columns=[col])
                
        df = df.drop(columns=[p_exclude_column])
    except:
        print(f"cant delete column in  dataset. Maybe it does not exist")
    
    dictionary = df.to_dict(orient='records')

    url = p_collection_name+"/update?commit=true"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(base_url+url, headers=headers, data=json.dumps(dictionary))

    if response.status_code == 200:
        print(f"Data indexed successfully")
    else:
        print(f"Failed to index data")
        print(response.content)


def searchByColumn(p_collection_name, p_column_name, p_column_value):
    solr = pysolr.Solr(base_url+p_collection_name, always_commit=True)
    try:
        query = p_column_name+":"+p_column_value
        res = solr.search(query)

        if res:
            print(f"Found {len(res)} result(s):")
            for result in res:
                print(result)
                print()
        else:
            print("No results found.")
    
    except Exception as e:
        print(f"An error occurred : {str(e)}")

def getEmpCount(p_collection_name):
    url = "admin/collections?action=create&name="+p_collection_name+"&numShards=1"
    res = requests.get(base_url+url)

    solr = pysolr.Solr(base_url+p_collection_name, always_commit=True)
    try:
        results = solr.search('*:*', **{'rows': 0})
        cnt = results.hits 

        print("Total employee count in "+p_collection_name+" is : ")
        print(cnt)
        return cnt
    
    except Exception as e:
        print(f"An error occurred : {str(e)}")

                
def delEmpById(p_collection_name, p_employee_id):
    solr = pysolr.Solr(base_url+p_collection_name, always_commit=True)

    try:
        solr.delete(id=p_employee_id)  
        solr.commit()  
        
        print(f"Employee with ID '{p_employee_id}' deleted from collection '{p_collection_name}'.")
        
    except Exception as e:
        print("An error occurred while deleting employee: ")
        print(e)

def getDepFacet(p_collection_name):
    print("Count of employees grouped by department :")
    url = p_collection_name+"/select?q=*:*&group=true&group.field=Department"
    try:
        response = requests.get(base_url+url)
        grpdata = response.json()
        
        if "grouped" in grpdata and "Department" in grpdata["grouped"]:
            department_groups = grpdata["grouped"]["Department"]["groups"]
            department_counts = {group["groupValue"]: group["doclist"]["numFound"] for group in department_groups}
            
            for department, count in department_counts.items():
                print(f"{department}: {count} employees found")
        else:
            print("No grouped data found in "+p_collection_name)
            return None
            
    except Exception as e:
        print(f"An error occurred: {e}")

v_nameCollection = "mohan"
v_phoneCollection = "8218"
createCollection(v_nameCollection)
createCollection(v_phoneCollection)
getEmpCount(v_nameCollection)
indexData(v_nameCollection,"Department")
indexData(v_phoneCollection, "Gender")
getEmpCount(v_nameCollection)
delEmpById (v_nameCollection ,"E02003")
getEmpCount(v_nameCollection)
searchByColumn(v_nameCollection,"Department","IT")
searchByColumn(v_nameCollection,"Gender" ,"Male")
searchByColumn(v_phoneCollection,"Department","IT")
getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)

