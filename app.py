import dotenv
import os
import streamlit as st
import pandas as pd
import BusinessInsightsGenerator 

class BusinessInsightApp:
    def __init__(self, api_key:str, db_connection_string: str):
        self.api_key = api_key
        self.db_connection_string = db_connection_string
        self.businessAssistant = BusinessInsightsGenerator.BusinessInsightsGenerator(connection_string= self.db_connection_string,
                                                                                     api_key= self.api_key)
    
    def GetBusinessInsights(self):
        ## Application details
        st.title("Business Insights")  # Correct way to set the title
        st.write("Connection String:", self.db_connection_string)  # Correct way to display the connection string
        user_query = st.chat_input("Enter Your business query!")
        if user_query:
            st.text(user_query)
            is_valid_question, reasoning, reframed_question = self.businessAssistant.validate_query_context(user_query)
            st.write(is_valid_question, reasoning, reframed_question)
            if is_valid_question==False and reframed_question is not None:
                st.write(reframed_question)

            if is_valid_question == True:
                sql_query = self.businessAssistant.generate_sql_query(user_query)
                result,narrative = self.businessAssistant.get_result(sql_query)
                # st.write(response)
                st.subheader("Generated SQL query")
                st.code(sql_query, language='sql')
                st.subheader("Result")
                st.dataframe(result) 

def main():
    # Fetching the environment variables
    CONNECTION_STRING = os.getenv("CONNECTION_STRING")
    API_KEY = os.getenv("API_KEY")

    # Creating the app instance
    app = BusinessInsightApp(api_key=API_KEY, db_connection_string= CONNECTION_STRING)

    # Get and display business insights
    app.GetBusinessInsights()




if __name__ == "__main__":
    main()

    
        
    