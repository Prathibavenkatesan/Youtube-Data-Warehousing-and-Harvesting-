# Youtube-Data-Warehousing-and-Harvesting 
## Objective:
   	 The aim was to build a user-friendly application where data from multiple YouTube channels could be easily accessed, stored in a __MongoDB__ database as a data lake and then migrated to a __SQL__ database for further analysis.
      	-> Access and analyze data from multiple YouTube channels.
      	-> Store the data in a MongoDB database as a data lake.
      	-> Migrate the data from MongoDB to a SQL database for structured querying.
      	-> Perform searches and execute queries on the stored data using various SQL search options.
			
## Features:
	- Data Harvesting: Extract channel metadata, video details, and comments using the YouTube Data API.
	- Data Warehousing: Store the extracted data in a SQL database for easy access and analysis.
	- Interactive Analysis: Use Streamlit to execute SQL queries, visualize data, and generate insights.
	- Query Examples: Predefined SQL queries for common analytics tasks such as top videos, channel statistics, and comment analysis.
	 
## Technologies:
		1.Python
		2.Pandas
		3.YouTube Data API
		4.SQL (MySQL)
		5.Streamlit

## Implementation:

**Setting up the Streamlit app**:
			 `Streamlit is a powerful tool for building data visualization and analysis tools quickly. In this project, users can enter a YouTube channel ID and view relevant details like channel name, subscriber count, total video count, playlist IDs, and video statistics.`
    
**Connecting to the YouTube API**:
			 Using the YouTube API, I was able to retrieve detailed data from multiple YouTube channels. The Google API client library for Python was a great choice for making API requests. The users get the relavant information for the channel details by giving the input of channel ID.
		
**Storing Data in a MongoDB Data Lake**:
			After retrieving the data, I stored it in a MongoDB database, which serves as a data lake. MongoDB is excellent for handling unstructured and semi-structured data, making it ideal for storing a wide range of YouTube data.

**Migrating Data to a SQL Data Warehouse**:
			SQL is a relational database that is well-suited for querying and analyzing structured data.After strored into the MongoDB,Create a connection to the MySQL server and access the specified MySQL DataBase by using pymysql library and access tables.
	
**Querying the SQL Data Warehouse**:
			 I created some meaningful insights.
		
**Displaying Data in the Streamlit App**:
			The final step was to display the data in a user-friendly format.The project provides comprehensive data analysis capabilities using Plotly and Streamlit. With the integrated Plotly library, users can create interactive and visually appealing charts and graphs to gain insights from the collected data.

**Conclusion**:
	 The YouTube Data Harvesting and Warehousing project provides a powerful tool for retrieving, storing, and analyzing YouTube channel and video data.
		
 ***Contributing***:
	Contributions to this project are welcome! If you encounter any issues or have suggestions for improvements, please feel free to submit a pull request.
      
`
