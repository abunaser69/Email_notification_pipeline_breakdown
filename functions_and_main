import pandas as pd
import numpy as np
import re
import os
import io
import errno
from os import listdir
from os.path import isfile,join
import glob
import shutil 
import operator
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import smtplib


def read_all_csv(input_dir):
    
    if len(glob.glob(input_dir + '/*.dat')) != 0:
        temp_dir = os.path.join(input_dir, "temp")
        shutil.rmtree(temp_dir)
        os.makedirs(temp_dir)
        list_of_dat_file = [shutil.copy2(f, temp_dir) for f in os.listdir(input_dir)  if '.dat' in f ]
        list_file_name = [os.path.basename(f) for f in list_of_dat_file ]
        
        for f in list_of_dat_file:
            pre, ext = os.path.splitext(f)
            os.rename(f, pre + '.gz')
            
        zip_list = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if '.gz' in f ]
        read_csv_list = [pd.read_csv(z, compression = 'gzip') for z in zip_list]
        raw_df = pd.concat(read_csv_list)
        return raw_df, list_file_name
    
    else:
        raw_df = []
        list_file_name = []
        return raw_df, list_file_name
       
       
 def process_audit_status(df):
    
    #replacing None with NaN
    df.replace('None', np.nan, inplace=True)
    #Get the empty columns
    empty_cols = [col for col in df.columns if df[col].isnull().all()]
    #removing empty cols
    df.drop(empty_cols, axis=1, inplace=True)
    #replacing Nan with empty string
    df.replace(np.nan, "", inplace=True)
    #shift system_created_date in front  
    df.insert(0, 'system_created_date', df.pop('system_created_date'))
    return df
    
def email_decision_values(df):
    
    df1 = df.copy(deep=True)
    # Get the boolean for matched string
    matched_boolean = df['col1'].str.contains("key_word", 
    case = False, na = False) == True
    #count the true value from the boolean
    total_no_of_key_word = matched_boolean.values.sum()
    #select the row count 
    df2 = df1[['row_count_col', 'key_word']]
    # All the empty cells are droped
    df3 = df2[df2.any(axis=1)].reset_index(drop=True)
    #counting non zero values of row_count clumns
    row_count_not_equal_0 = df3['row_count'].ne(0).sum()
    #counting nonzero values of extract completed column 
    extract_completed_not_equal_0 = df3['key_word'].ne(0).sum()
    return  total_no_of_completed_successfully, row_count_not_equal_0, extract_completed_not_equal_0
    
 def get_decisions(left_arg, right_arg, condition):
    
    if condition(left_arg, right_arg):
        email = int(False)
    else:
        email = int(True)
    return email
    
def export_csv(df):
    
    with io.StringIO() as buffer:
        df.to_csv(buffer)
        value = buffer.getvalue()
    return value
    

def send_email(list_file_name, para1, para2, processed_df, input_dir):
    From = ''
    recipients = ['']
    To = ", ".join(recipients)
    Subject = "Daily Load Email Alert" 
    
    # Build the message using the email library and send using smtplib
    msg = MIMEMultipart()
    msg['From'] = From
    msg['To'] = To
    msg['Subject'] = Subject
    #get the date 
    date = datetime.today()
    # convert to readable format yyyy-mm-dd
    date = date.strftime("%Y-%m-%d")  
    
    # write an email message
    txt1 = (f"No files in the folder {input_dir}")
    
    txt2 = (f" Email  Alert Date: {date} \n\n"
       f" List of file(s) processed: {list_file_name} \n\n"
       f" The reason(s) for this email: \n\n"
       f" 1) Ebsx Dialy run notebook did not run successfully : {para1} \n\n"
       f" 2) Extract not completed in case of non-zero row count : {para2} \n\n"
       f" Here,  Yes = 1; No = 0 \n\n")
    
    if list_file_name == None and daily_successful_run == None and daily_extract_completed == None and processed_df == None:
        # add text contents
        msg.attach(MIMEText(txt1))
    else:
        msg.attach(MIMEText(txt2))
        #Attach the processed file(s)
        Exporters = {'Processed.csv': export_csv}
            
        for filename in Exporters:
            attachment = MIMEApplication(Exporters[filename](processed_df))
            attachment['Content-Disposition'] = 'attachment; filename = "{}"'.format(filename)
            msg.attach(attachment)
    
    # Initialise connection to mail server
    smtp = smtplib.SMTP('.....', 'port')
    # say hello to the server
    smtp.ehlo() 
    # Use TLS encryption
    smtp.starttls() 

    if daily_successful_run !=0 or daily_extract_completed !=0:
        # send email to the group 
        smtp.sendmail(From, To, msg.as_string())
        print("An email has been sent")
    else:
        print('No need to send email')
       
    # Exit the mail server
    smtp.quit()


def move_to_processed_dir(input_dir, processed_dir):
    
    file_list_to_move = [file for file in os.listdir(input_dir) if os.path.isfile(file)]
    
    for file in file_list_to_move:
        shutil.move(os.path.join(input_dir, file), os.path.join(processed_dir, file))
        
    #to check all the files are move to the processed dir, create list again    
    file_list_to_move = [file for file in os.listdir(input_dir) if os.path.isfile(file)]
    
    if len(file_list_to_move) == 0:
        print("All the files in folder moved to the Processed folder")
    else:
        print("Some files are still in the folder")
        
        
 def delete_older_files(root_dir, days):
    
    files_list = os.listdir(root_dir)
    current_time = datetime.utcnow().timestamp()
    
    for file in files_list:
        file_path = os.path.join(root_dir, file)
        if os.path.isfile(file_path):
            if (current_time - os.stat(file_path).st_mtime) > (days * 24 * 60 * 60):
                print(file_path)
                os.remove(file_path) 
  
  def main():
    
    #get input and processed dir
    input_dir, processed_dir = personal_setup()
    
    #get the raw data and list of file name
    raw_df, list_file_name = read_all_csv(input_dir)
    
    if len(raw_df) == 0 :
        send_email(None, None, None, None, input_dir)
        quit()
        
    else:
        #get the processed dataframe
        processed_df = process_audit_status(raw_df)
        #get the email decision values
        successfully_completed_count, nonzero_row_count, nonzero_extract_count =  email_decision_values(processed_df)
        #get the decison values
        expected_count = 2
        daily_successful_run = get_decisions(successfully_completed_count, expected_count, operator.ge)
        daily_extract_completed = get_decisions(nonzero_row_count, nonzero_extract_count, operator.eq)
        #send email
        send_email(list_file_name, daily_successful_run, daily_extract_completed, processed_df,input_dir)
        #move the files to the processed folder after processing
        move_to_processed_dir(input_dir, processed_dir)
        #delete older than 60 day files in the processed folder
        delete_older_files(processed_dir, 60)
    
# execute
if __name__ == '__main__':
    main()

