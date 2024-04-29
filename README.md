# Youtube_Data_Harvesting
"YouTube Data Harvesting" gathers channel details, video info, and comments using YouTube Data API. Data is stored in MySQL and presented via a Streamlit web app, facilitating analysis and insights.
Youtube-data-Harvesting-and-Warehousing-project

YouTube Data Harvesting and Warehousing using Python
Overview : This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MYSQL databases, and enables users to search for channel details and join tables to view data in the Streamlit app.
PYTHON: Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.
GOOGLE API CLIENT: The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.
STREAMLIT: Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.
MYSQL: MySql is an open-source, advanced, and highly scalable database management system (DBMS) known for its reliability and extensive features.
Workflow : Connect to the YouTube API this API is used to retrieve channel, videos, comments data. I have used the Google API client library for Python to make requests to the API. The user will able to extract the Youtube channel's data using the Channel ID. Once the channel id is provided the data will be extracted using the API. Once the data is retrieved from the YouTube API, After collected data for multiple channels,it is then migrated/transformed it to a structured MySQL as data warehouse. Then used SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. With the help of SQL query I have got many interesting insights about the youtube channels. Finally, the retrieved data is displayed in the Streamlit app. Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a SQL data.


Python libraries:
1.Google Api Client
2.pymongo
3.Pandas
4.mysqlconnector
5.Streamlit
