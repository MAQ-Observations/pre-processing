import requests
import os
from datetime import datetime, timedelta, timezone
import time

def prepare_files(t1,t2,directory,END_POINT, today_yesterday):
    date_list = dates_between(t1,t2)
 
    #API_KEY = '834805e0-8b16-424b-9889-341d5e037fd8'   #Old API-key for sjoerdlohuis.nl/maq
    API_KEY = '2ece4599-3157-40bf-ae13-351e1eb3ae92'    #New API-key for maq-observations.nl
    HOST_KL = 'https://maq-observations.nl'                     #Website URL

    array_files_n = get_file_names(os.path.join(directory),date_list) #Get all file names in the directory to post

    #print("url ---->", HOST_KL + END_POINT)
    
    #Do the uploading
    for file in array_files_n:
        for date in date_list:
            if date in file:
                post_file(HOST_KL, END_POINT, API_KEY, os.path.join(directory,file), today_yesterday)
                print('-----Finish upload------>>>', file)
                
def post_file(HOST_KL, END_POINT, API_KEY, path, today_yesterday):
    hyml_data = ''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'ApiKey {API_KEY}',
        'Content-Type': 'text/csv'
    }
    
    with open(path, 'r') as f:
        lines = f.readlines()
        
    current_time = datetime.now(timezone.utc)
    time_hours_earlier = current_time - timedelta(hours=48)
    
    if today_yesterday == True:
        # Collect the first 2 lines and the lines within the past 2 hours
        filtered_lines = lines[:2] + [line for line in lines[2:] if is_within_past_hours(line, time_hours_earlier, current_time)]
        hyml_data = ''.join(filtered_lines)
        if len(filtered_lines) > 2:
            #print('Posting file contents to MAQ-Observations database')
            post = requests.post(HOST_KL + END_POINT, data=hyml_data, headers=headers)
            print(post)
            if post.status_code == 504:
                print('Receiving <Response [504]>, retrying once')
                time.sleep(1)
                post = requests.post(HOST_KL + END_POINT, data=hyml_data, headers=headers)
                print(post)
        else:
            print('Data does not fall within x hours from now')
            
    else:
        post_in_quarters = False
        
        if post_in_quarters == True:
            #print('Posting file contents to MAQ-Observations database')
            # Include the first two lines as headers for each quarter
            header_lines = lines[:2]
            # Split the data into quarters
            quarter_length = len(lines) // 4
            first_quarter_data = ''.join(header_lines + lines[2:quarter_length])
            second_quarter_data = ''.join(header_lines + lines[quarter_length:2*quarter_length])
            third_quarter_data = ''.join(header_lines + lines[2*quarter_length:3*quarter_length])
            fourth_quarter_data = ''.join(header_lines + lines[3*quarter_length:])
            # POST each quarter
            post_first_quarter = requests.post(HOST_KL + END_POINT, data=first_quarter_data, headers=headers)
            print("First quarter posted:", post_first_quarter)
            if post_first_quarter.status_code == 504:
                print('Receiving <Response [504]> for first quarter, retrying once')
                time.sleep(1)
                post_first_quarter = requests.post(HOST_KL + END_POINT, data=first_quarter_data, headers=headers)
                print("First quarter posted after retry:", post_first_quarter)
            post_second_quarter = requests.post(HOST_KL + END_POINT, data=second_quarter_data, headers=headers)
            print("Second quarter posted:", post_second_quarter)
            if post_second_quarter.status_code == 504:
                print('Receiving <Response [504]> for second quarter, retrying once')
                time.sleep(1)
                post_second_quarter = requests.post(HOST_KL + END_POINT, data=second_quarter_data, headers=headers)
                print("Second quarter posted after retry:", post_second_quarter)
            post_third_quarter = requests.post(HOST_KL + END_POINT, data=third_quarter_data, headers=headers)
            print("Third quarter posted:", post_third_quarter)
            if post_third_quarter.status_code == 504:
                print('Receiving <Response [504]> for third quarter, retrying once')
                time.sleep(1)
                post_third_quarter = requests.post(HOST_KL + END_POINT, data=third_quarter_data, headers=headers)
                print("Third quarter posted after retry:", post_third_quarter)
            post_fourth_quarter = requests.post(HOST_KL + END_POINT, data=fourth_quarter_data, headers=headers)
            print("Fourth quarter posted:", post_fourth_quarter)
            if post_fourth_quarter.status_code == 504:
                print('Receiving <Response [504]> for fourth quarter, retrying once')
                time.sleep(1)
                post_fourth_quarter = requests.post(HOST_KL + END_POINT, data=fourth_quarter_data, headers=headers)
                print("Fourth quarter posted after retry:", post_fourth_quarter)
            
        else:
            hyml_data = ''.join(lines)
            
            post = requests.post(HOST_KL + END_POINT, data=hyml_data, headers=headers)
            print(post)
            if post.status_code == 504:
                print('Receiving <Response [504]>, retrying once')
                time.sleep(1)
                post = requests.post(HOST_KL + END_POINT, data=hyml_data, headers=headers)
                print(post)
            
                

def get_file_names(datapath,date_list):
    # Read infiles
    A_files = []

    for root, dirs, files in os.walk(datapath):
        for filename in files:
            for date in date_list:
                if date in filename:
                    #print('--found ----> ', filename)
                    A_files.append(filename)
    return sorted(A_files)
    
# Function to check if a line's datetime falls within the past hours
def is_within_past_hours(line, start_time, end_time):
    try:
        # Extract the datetime string from the line and parse it
        line_datetime_str = line[:19]
        line_datetime = datetime.strptime(line_datetime_str, '%Y-%m-%d %H:%M:%S')
        line_datetime = line_datetime.replace(tzinfo=timezone.utc)
        # Check if the datetime falls within the past hours
        return start_time <= line_datetime <= end_time
    except ValueError:
        # If parsing fails, return False
        return False

def dates_between(t1, t2):
    date_format_input = "%Y-%m-%d"
    date_format_output = "%Y%m%d"
    start_date = datetime.strptime(str(t1), date_format_input)
    end_date = datetime.strptime(str(t2), date_format_input)

    # Generate list of dates between start_date and end_date
    date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    # Format the dates back to string format
    date_strings = [date.strftime(date_format_output) for date in date_list if date <= end_date]

    return date_strings

    
