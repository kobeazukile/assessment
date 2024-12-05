#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import pandas as pd
import os

# file paths
BASE_PATH = r'C:\Users\akobe292\Downloads'
DETAILS_FILE = os.path.join(BASE_PATH, 'customer_details (1).json')
ADDRESS_FILE = os.path.join(BASE_PATH, 'customer_address (1).json')
OUTPUT_DETAILS = os.path.join(BASE_PATH, 'customer_details.csv')
OUTPUT_ADDRESS = os.path.join(BASE_PATH, 'customer_address.csv')

def clean_json_file(file_path):
    """Clean and fix JSON file content"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    content = content.replace('[F,]', '["F", null]')
    content = content.replace('[,M]', '[null, "M"]')
    
    return json.loads(content)

def process_gender(gender_array):
    """Extract gender from array format"""
    if not gender_array:
        return None
    if isinstance(gender_array, list):
        if 'F' in str(gender_array):
            return 'F'
        elif 'M' in str(gender_array):
            return 'M'
    return None

def create_customer_details_csv(details_file, address_file, output_file):
    """Process customer details and create CSV"""
    try:
        details_data = clean_json_file(details_file)
        
        # Convert to DataFrame
        details_df = pd.DataFrame(details_data)
        
        details_df['gender'] = details_df['gender'].apply(process_gender)
        
        details_df = details_df.drop_duplicates(subset=['id'])
        
        with open(address_file, 'r') as f:
            address_data = json.load(f)
        address_ids = set(item['id'] for item in address_data)
        
        details_df['address_exists'] = details_df['id'].isin(address_ids)
        
        details_df['id'] = details_df['id'].astype(int)
        details_df['age'] = details_df['age'].astype(int)
        details_df['balance'] = details_df['balance'].astype(float)
        details_df['address_exists'] = details_df['address_exists'].astype(bool)
        
        details_df = details_df.rename(columns={'id': 'cust_id'})
        
        column_order = [
            'cust_id',
            'name',
            'age',
            'province',
            'gender',
            'balance',
            'address_exists'
        ]
        
        final_df = details_df[column_order]
        
        # Save to CSV
        final_df.to_csv(output_file, index=False)
        print(f"Successfully created: {output_file}")
        return final_df
    
    except Exception as e:
        print(f"Error processing details file: {str(e)}")
        raise

def create_customer_address_csv(address_file, output_file):
    """Process customer addresses and create CSV"""
    try:
        with open(address_file, 'r') as f:
            address_data = json.load(f)
        
        # Flatte JSON 
        flattened_data = []
        for item in address_data:
            address_dict = {
                'cust_id': item['id'],
                'address_street': item['address']['street'],
                'address_city': item['address']['city'],
                'address_code': item['address']['code'],
                'email': '',
                'phone': ''
            }
            
            # cleaning contacts
            for contact in item['contacts']:
                if contact['type'] == 'email':
                    address_dict['email'] = contact['value']
                elif contact['type'] == 'phone':
                    address_dict['phone'] = contact['value']
            
            flattened_data.append(address_dict)
        
        address_df = pd.DataFrame(flattened_data)
        
        column_order = [
            'cust_id',
            'address_street',
            'address_city',
            'address_code',
            'email',
            'phone'
        ]
        
        final_address_df = address_df[column_order]
        
        # Save to CSV 
        final_address_df.to_csv(output_file, index=False, sep=';')
        print(f"Successfully created: {output_file}")
        return final_address_df
    
    except Exception as e:
        print(f"Error processing address file: {str(e)}")
        raise

def main():
    print("Starting ETL process...")
    print(f"Reading files from: {BASE_PATH}")
    
    try:
        print("\nProcessing customer details...")
        details_df = create_customer_details_csv(DETAILS_FILE, ADDRESS_FILE, OUTPUT_DETAILS)
        
        print("\nProcessing customer addresses...")
        address_df = create_customer_address_csv(ADDRESS_FILE, OUTPUT_ADDRESS)
        
        print("\nValidation Summary:")
        print(f"Total unique customers: {len(details_df)}")
        print(f"Customers with addresses: {details_df['address_exists'].sum()}")
        print(f"Unique provinces: {details_df['province'].unique()}")
        
        print("\nProcessed customer details:")
        print(details_df)
        print("\nProcessed customer addresses:")
        print(address_df)
        
        print("\nOutput files created in:", BASE_PATH)
        print("1.", OUTPUT_DETAILS)
        print("2.", OUTPUT_ADDRESS)
        
    except Exception as e:
        print(f"\nError during ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    main()


# In[ ]:




