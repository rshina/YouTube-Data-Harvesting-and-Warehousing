**YouTube Data Harvesting and Warehousing**

Skills take away From This Project(Python scripting, Data Collection, MongoDB, Streamlit, API integration, Data Management using MongoDB  and SQL)

YouTube Channel Information Extraction and Analysis

This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel. The extracted data is stored in a MongoDB database, then migrated to a SQL data warehouse. The application enables users to search for channel details and join tables to view data in the Streamlit app.

**Features**

    *Extracts information on a YouTube channel using the Google API
    
    *Stores the extracted data in a MongoDB database
    
    *Migrates the data from MongoDB to a SQL data warehouse
    
    *Enables users to search for channel details
    
    *Provides the ability to join tables and view data in the Streamlit app

**Usage**

    *Open the Streamlit application in your browser
    *Enter the YouTube channel ID(s)
    *Click on the "submit" button to extract information from the YouTube channel
    *Click the 'choose a table' button to get the result in the form of table
    *Next from the multiselect dropdown select the Query ,result will be in table

**Required Libraries to Install**

    pip install google-api-python-client, pymongo,  pymysql, pandas,streamlit
	

**Tools used for**

    Visual Studio Code or jupitor notbook Python 3.11.0 or higher. MySQL. MongoDB. Youtube DATA API

 
**Process**


   **Extract the particular youtube channel data by using the youtube channel id, with the help of the youtube API developer console.
   
   **After the extraction process, takes the required details from the extraction data and transform it into dictionary or JSON format
   
   **After the transformation process,Create a connection to the MOGODB server and  JSON format data is stored in the MongoDB database
   
   **Create a connection to the MySQL server and access the specified MySQL DataBase by using pymysql library and access tables.
   
   **Filter and process the collected data from the tables depending on the given requirements by using SQL queries and transform the processed data into a DataFrame format.
   
   **Finally, create a Dashboard by using Streamlit and give dropdown options on the Dashboard to the user and select a question from that menu to analyse the data and show  the output in Dataframe Table 


   
**User Guide**


   ** Enter any channel_id in "Enter a youtube channel id" and click "submit" ,will get all the information about that perticular channel.
   
   **Select any query from multiselect option ,will get answer for the query as table
   
   **Choose a table(channel,playlist,vedio,comment) ,will get information about selected one as a table



**Contributing**

Contributions are welcome! If you find any issues or have suggestions for improvement, please open an issue or submit a pull request.
