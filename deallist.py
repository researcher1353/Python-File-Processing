#importing req'd Libraries
import pandas as pd
import numpy as np
from datetime import date, time, datetime
from os import getpid

# Opening Excel File
def open_file(filename,s_name):
    data=pd.read_excel(filename,sheet_name=s_name)
    return data

# Opening .csv File
def open_file_csv(filename):
    data=pd.read_csv(filename)
    return data

# Indexing from 1
def index(dataframe):
    array=dataframe.iloc[:,-1]
    index=[]
    
    for i in range(len(array)):
        index.append(i+1)
    
    index=pd.DataFrame(index,columns=['Index'])
    dataframe=pd.concat([index,dataframe],axis=1)
    
    return dataframe

# Creating a Lookup Table as a Set of Dictionaries
def dict_set(dataframe,index):
    key=list(dataframe.iloc[:,index+1])
    value=list(dataframe.iloc[:,index])
    dict1={}
    
    for i in range(len(key)):
        dict1[key[i]]=value[i]
    
    return dict1

# Exchanging the numeric Values with Categorical Ones
def exchange(dataframe1,index1,dataframe2,index2):
    
    label=dict_set(dataframe2,index2)
    
    value=dataframe1.iloc[:,index1]
    
    dataframe3=dataframe1.copy()
    
    for i in range(len(value)):
        dataframe3.iloc[i,index1]=label[value[i]]
        
    return dataframe3

# Analyzing Errors and Writing to a .csv File 
def error(dataframe):
    
    err=0
    error_dict={}
    
    # Case 1: If the numerical values are a NaN value, then it gives an error
    numeric_values=dataframe.iloc[:,2:7]
   
    for i in range(1,5):
        error_list=list(numeric_values.iloc[:,i])
        
        for j in range(len(error_list)):
            if str(error_list[j])=='nan':
                err_code=1
                err+=1
                error_dict["Code "+str(err_code)]=str(err)+" values are found as NaN value..."
    
    err=0

    ans='y'
    
    while ans=='y' or ans=='Y':
        try:
            value=input("Enter Your Value Type (Deal,Country,Currency,Company)=")
        
            # Case 2: If the Input Value as Deal is not on the Dataframe
            
            if value=="deal" or value=="Deal" or value=="DEAL":    
                deal=input("Enter the Deal Name= ")
                deal_values=list(dataframe.iloc[:,0])
                
                if deal not in deal_values:
                    err_code=2
                    error_dict["Code "+str(err_code)]="Input "+deal+" is not in the deal list..."
                
            # Case 3: If the Input Value of Country is not on the dataframe
            
            elif value=="country" or value=="Country" or value=="COUNTRY":    
                country=input("Enter the Country= ")
                country_values=list(dataframe.iloc[:,7])
                
                if country not in country_values:
                    err_code=3
                    error_dict["Code "+str(err_code)]="Input "+country+" is not on the country list..."
            
            # Case 4: If the Input Value of Currency is not on the dataframe or not matched correctly with the country
            
            elif value=="currency" or value=="Currency" or value=="CURRENCY":    
                currency=input("Enter the Currency= ")
                currency_values=list(dataframe.iloc[:,8])
                
                if currency not in currency_values:
                    err_code=4
                    error_dict["Code "+str(err_code)]="Input "+currency+" is not on the currency list..."
                
            # Case 5: If the Input Value of Currency is not on the dataframe or not matched correctly with the country
            
            elif value=="company" or value=="Company" or value=="COMPANY":    
                company=input("Enter the Company= ")
                company_values=list(dataframe.iloc[:,-1])
                
                if company not in company_values:
                    err_code=5
                    error_dict["Code "+str(err_code)]="Input "+company+" is not on the currency list..."
                
        except ValueError:
            print("Please enter a valid value...")
        
        ans=input('Would you like to proceed (y/N)?=')
        
        if ans=='n' or ans=="N":
            break
    
    errorCode=[]
    explanation=[]
    
    for key in error_dict:
        errorCode.append(key)
        explanation.append(error_dict[key])
        
    out_data=np.column_stack((errorCode,explanation))
    out_data=pd.DataFrame(out_data,columns=['Error Code','Explanation'])
    out_data.to_csv("error.csv",index=False)
    
    return error_dict

# Output data to a .csv file of datetime, process ID and hash values as extra columns
def outfile(dataframe):
    array=dataframe.iloc[:,-1]
    
    datetime_list=[]
    pID_list=[]
    hash_list=[]
    
    for i in range(len(array)):
        datetime_list.append(datetime.now())
        pID_list.append(getpid())
        hash_list.append(hash(array[i]))
        
    combine_df=np.column_stack((datetime_list,pID_list,hash_list))
    combine_df=pd.DataFrame(combine_df,columns=['Timestamp','Process ID','Hash'])
    dataframe2=dataframe.copy()
    dataframe2=pd.concat([dataframe2,combine_df],axis=1)
    
    # to .csv file
    dataframe2.to_csv("output.csv",index=False)
    
    # to .parquet file
    dataframe2.to_parquet('output.parquet')
    
# Output data to a .csv file of datetime, process ID and hash values as extra columns
def outfile2(dataframe):
    array=dataframe.iloc[:,-1]
    
    datetime_list=[]
    pID_list=[]
    hash_list=[]
    
    for i in range(len(array)):
        datetime_list.append(datetime.now())
        pID_list.append(getpid())
        hash_list.append(hash(array[i]))
        
    combine_df=np.column_stack((datetime_list,pID_list,hash_list))
    combine_df=pd.DataFrame(combine_df,columns=['Timestamp','Process ID','Hash'])
    dataframe2=dataframe.copy()
    dataframe2=pd.concat([dataframe2,combine_df],axis=1)
    
    # to .csv file
    dataframe2.to_csv("output2.csv",index=False)
    
    # to .parquet file
    dataframe2.to_parquet('output2.parquet')

# Calling Main Program from the Excel File
def main_excel():
    # Calling the Excel Worksheets
    filename="deallist.xlsx"
    s_name="deal_list"
    lookup="lookup"
    df1=open_file(filename,s_name)
    df1=index(df1)
    df2=open_file(filename,lookup)
    
    # Creating Company, Country and Currency Lookup Tables as Dictionaries
    company_dict=dict_set(df2,0)
    country_dict=dict_set(df2,2)
    currency_dict=dict_set(df2,4)
    
    # Exchanging Numeric Values with Categorical Values
    df3=exchange(df1,-3,df2,2)
    df3=exchange(df3,-2,df2,4)
    df3=exchange(df3,-1,df2,0)
    
    # Analysing Errors
    e=error(df3)
    
    outfile(df3)
    
# Calling Main Program from .csv Files
def main_csv():
    # Calling the Excel Worksheets
    filename1="deallist.csv"
    filename2="companies.csv"
    filename3="countries.csv"
    filename4="currencies.csv"
    df1=open_file_csv(filename1)
    df1=index(df1)
    
    df2=open_file_csv(filename2)
    df3=open_file_csv(filename3)
    df4=open_file_csv(filename4)
    
    # Creating Company, Country and Currency Lookup Tables as Dictionaries
    company_dict=dict_set(df2,0)
    country_dict=dict_set(df3,0)
    currency_dict=dict_set(df4,0)
    
    # Exchanging Numeric Values with Categorical Values
    df5=exchange(df1,-3,df3,0)
    df5=exchange(df5,-2,df4,0)
    df5=exchange(df5,-1,df2,0)
    
    # Analysing Errors
    e=error(df5)
    
    outfile2(df5)

# Calling Main Program
if __name__=="__main__":
        
    selection=input("Enter your selection Excel(e)/csv(c)=")
    
    if selection=="e" or selection.upper():
        main_excel()
        
    elif selection=="c" or selection.upper():
        main_csv()
        
    else:
        print("Invalid Selection...")
