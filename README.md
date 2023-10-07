# SJSU_ChatGPT_Tweet_Analyser

chatgpt1.csv : Recommended Dataset for the project

data_prep.ipynb:  This file has python script used for cleaning and loading the data from csv to sql.

Project-final.sql: This file has all queries, stored procedure, and triggers.

chatgpt_app.py: This file is an .py file used for developing a user interface application. 

Results.pdf: This file includes all the results of queries, stored procedure, and triggers.


How to run the Chatgpt_app:

Python app is created to run locally and connect to the remote AWS RDS instance to send the
predefined SQL queries and run stored procedures remotely to retrieve the relevant information.

#1
chatgpt_app.py -h – Shows the help menu

#2
chatgpt_app.py -p – Should prompt for password and the password is labproject
![image](https://github.com/SahanaTP/SJSU_ChatGPT_Tweet_Analyser/assets/32634047/f6794765-9cf5-40a3-b0b8-8b133c592cb9)
