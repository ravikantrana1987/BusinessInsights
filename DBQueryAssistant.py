import streamlit as st
from langchain_groq import ChatGroq
from langchain.schema import HumanMessage, SystemMessage
from langchain.prompts import PromptTemplate
from langchain.output_parsers import CommaSeparatedListOutputParser
import pyodbc
from fuzzywuzzy import fuzz
import pandas as pd
from typing import List, Dict, Tuple
import json, dotenv,os, re

class DBQueryAssistant:
    def __init__(self, connection_string: str, groq_api_key: str):
        self.conn_str = connection_string
        # self.llm = ChatGroq(api_key=groq_api_key, model="llama-3.1-8b-instant")
        self.llm = ChatGroq(api_key=groq_api_key, model="mixtral-8x7b-32768")
        self.db_schema = self._get_db_schema()
        self.value_cache = {}
        
    # def _get_db_schema(self) -> Dict:
    #     """Extract database schema and relationships"""
    #     schema = {}
    #     with pyodbc.connect(self.conn_str) as conn:
    #         cursor = conn.cursor()
            
    #         # Get tables and their columns
    #         tables = cursor.tables(tableType='TABLE')
    #         # st.text(tables)
    #         # for table in tables:
    #         #     st.text(table)

    #         for table in tables:
    #             table_name = table.table_name
    #             columns = cursor.columns(table=table_name)
    #             schema[table_name] = {
    #                 'columns': [column.column_name for column in columns],
    #                 'relationships': []
    #             }
            
    #         st.code(schema, language='json')

    #         # Get foreign key relationships
    #         for table_name in schema:
    #             fks = cursor.foreignKeys(table=table_name)
    #             for fk in fks:
    #                 schema[table_name]['relationships'].append({
    #                     'referenced_table': fk.referenced_table_name,
    #                     'column': fk.fk_column_name,
    #                     'referenced_column': fk.pk_column_name
    #                 })

    #         st.code(schema, language='json')        
    #     return schema


    def _get_db_schema(self) -> Dict:
        """Extract database schema and relationships"""
        schema = {
            "tables":{},
            "relationships":[]
        }
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()

                # Retrieve database tables
                tables = cursor.execute(
                    """
                        SELECT 
                            t.name AS table_name,
                            c.name AS column_name, 
                            ty.name AS data_type
                        FROM 
                            sys.tables t
                        INNER JOIN 
                            sys.columns c 
                            ON t.object_id = c.object_id
                        INNER JOIN 
                            sys.types ty 
                            ON c.user_type_id = ty.user_type_id
                        ORDER BY 
                            t.name, 
                            c.column_id
                    """).fetchall()
                
                
                for table, column, data_type in tables:
                    if table not in schema["tables"]:
                        schema["tables"][table]={"columns":{}}
                    schema["tables"][table]["columns"][column] = data_type
                

                # Retrieve all the foreign key relationships
                relationships = cursor.execute(
                    """
                        SELECT 
                            OBJECT_NAME(f.parent_object_id) AS TableName,
                            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
                            OBJECT_NAME(f.referenced_object_id) AS ReferenceTableName,
                            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName
                        FROM 
                            sys.foreign_keys AS f
                        INNER JOIN 
                            sys.foreign_key_columns AS fc
                            ON f.OBJECT_ID = fc.constraint_object_id

                    """).fetchall()
                
                for table, column, referenced_table, referenced_column in relationships:
                    relationship = {
                        "table": table,
                        "column": column,
                        "referenced_table" : referenced_table,
                        "referenced_column" : referenced_column
                    }

                    if relationship not in schema["relationships"]:
                        schema["relationships"].append(relationship)
                st.text(schema)
                return schema
        except Exception as ex:
            print(f"Unable to retrieve the database schema: {str(ex)}")
            return None
         

    def _get_column_values(self, table: str, column: str) -> List[str]:
        """Cache and retrieve distinct values from a column"""
        cache_key = f"{table}.{column}"
        if cache_key not in self.value_cache:
            with pyodbc.connect(self.conn_str) as conn:
                query = f"SELECT DISTINCT CAST([{column}] AS VARCHAR(MAX)) as {column} FROM {table}"
                st.write(query)
                df = pd.read_sql(query, conn)
                self.value_cache[cache_key] = df[column].tolist()
        # st.write(self.value_cache)
        return self.value_cache[cache_key]

    # def validate_query_context(self, user_question: str) -> bool:
    #     """Check if the question is related to the database context"""
    #     system_prompt = f"""You are a database query validator. Given the following database schema:
    #     {json.dumps(self.db_schema, indent=2)}
        
    #     Determine if the user question is related to querying this database.
    #     Return only 'true' or 'false'."""
        
    #     messages = [
    #         SystemMessage(content=system_prompt),
    #         HumanMessage(content=user_question)
    #     ]
        
    #     response = self.llm.invoke(messages)
    #     return response.content.strip().lower() == 'true'


    # def validate_query_context(self, user_question: str) -> bool:
    #     """Check if the question is related to the database context"""
    #     # Create a more detailed system prompt with table information
    #     tables_info = []
    #     for table_name, table_info in self.db_schema['tables'].items():
    #         columns = ", ".join(table_info['columns'].keys())
    #         tables_info.append(f"Table '{table_name}' with columns: {columns}")

    #     st.text(table_info)
        
    #     system_prompt = f"""You are a database query validator. You have access to a database with the following tables:
    #     {chr(10).join(tables_info)}
        
    #     Your task is to determine if the following user question can be answered using this database schema.
    #     Return ONLY 'true' if the question is related to the database tables and their data, or 'false' if it's completely unrelated.
    #     A question is related if it asks about any tables, their columns, or the data they might contain."""
        
    #     messages = [
    #         SystemMessage(content=system_prompt),
    #         HumanMessage(content=f"Question: {user_question}\nIs this question related to the database schema? Answer only 'true' or 'false'.")
    #     ]
        
    #     response = self.llm.invoke(messages)
    #     st.text(response.content.strip().lower())
    #     return response.content.strip().lower() == 'true'


    def validate_query_context(self, user_question: str) -> Tuple[bool, str]:
        """Check if the question is related to the database context"""
        # Create a comprehensive system prompt
        tables_info = []
        for table_name, table_info in self.db_schema['tables'].items():
            columns = ", ".join(f"{col} ({dtype})" for col, dtype in table_info['columns'].items())
            tables_info.append(f"Table '{table_name}' contains: {columns}")
        
        relationships_info = []
        for rel in self.db_schema['relationships']:
            relationships_info.append(
                f"Table '{rel['table']}' is related to '{rel['referenced_table']}' "
                f"through {rel['column']} = {rel['referenced_column']}"
            )
        
        system_prompt = f"""
        You are a database query validator analyzing if questions can be answered using this database:
            Tables:
            {chr(10).join(tables_info)}

            Relationships:
            {chr(10).join(relationships_info)}

            A question is considered related to the database if it:
            1. Mentions any table names (even in different forms like 'category' for 'categories')
            2. Asks about any columns or their data
            3. Asks about relationships between tables
            4. Asks about counts, summaries, or analytics of the data
            5. Uses business terms that clearly map to the database structure

            Analyze the question and return a JSON with two fields:
            - "is_related": true/false
            - "reasoning": brief explanation of why"""
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Question: {user_question}")
        ]
        st.text(system_prompt)
        try:
            response = self.llm.invoke(messages)
            result = json.loads(response.content)
            return result['is_related'], result['reasoning']
        except:
            # Fallback to a more permissive validation
            question_lower = user_question.lower()
            table_names = [table.lower() for table in self.db_schema['tables'].keys()]
            
            # Check if any table name or its singular/plural form is mentioned
            for table in table_names:
                if (table in question_lower or 
                    table[:-1] in question_lower or  # singular
                    table + 's' in question_lower):  # plural
                    return True, f"Question mentions table {table}"
            
            # Check for common analytical terms
            analytical_terms = ['how many', 'count', 'list', 'show', 'find', 'get', 'what', 'which']
            if any(term in question_lower for term in analytical_terms):
                return True, "Question appears to be an analytical query"
                
            return False, "Question doesn't appear to be related to the database"
    

    def find_similar_values(self, table: str, column: str, user_input: str, threshold: int = 80) -> List[str]:
        # st.text("find_similar_values")
        """Find similar values using fuzzy matching"""
        st.write("Table:", table, "Column", column)
        values = self._get_column_values(table, column)
        similar_values = []
        
        for value in values:
            similarity = fuzz.ratio(str(user_input).lower(), str(value).lower())
            if similarity >= threshold:
                similar_values.append((value, similarity))
                
        return [value for value, _ in sorted(similar_values, key=lambda x: x[1], reverse=True)]

    def generate_sql_query(self, user_question: str, corrections: Dict = None) -> str:
        """Generate SQL query using LLM"""
        system_prompt = f"""You are a SQL query generator. Given the following database schema:
        {json.dumps(self.db_schema, indent=2)}
        
        Generate a SQL query for the user question. If corrections are provided, use those values.
        Return only the SQL query without any explanations."""
        
        question = user_question
        if corrections:
            question += f"\nUse these corrections: {json.dumps(corrections)}"
            
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=question)
        ]
        
        response = self.llm.invoke(messages)
        validated_sql = self._validate_and_clean_query(response.content.strip())
        return validated_sql
        # return response.content.strip()
    
    def _validate_and_clean_query(self, query: str) -> str:
        cleanedQuery = self.remove_think_tags(query)
        query = cleanedQuery.replace('```sql', '').replace('```', '').replace(";",'').replace("\\",'').replace('`','').strip()
        
        
        referenced_tables = set()
        for table in self.db_schema["tables"].keys():
            if table.lower() in query.lower():
                referenced_tables.add(table)
        
        if not referenced_tables:
            raise ValueError("Generated query does not reference any valid tables")
        
        return query
    
    def remove_think_tags(self, text: str) -> str:
        # Use regular expression to remove content between <think> and </think>
        return re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)

def main():
    dotenv.load_dotenv()    
    st.title("Intelligent Database Query Assistant")
    
    # Initialize the assistant (in practice, get these from environment variables)
    connection_string = os.getenv("CONNECTION_STRING")
    
    groq_api_key = os.getenv("API_KEY")
    assistant = DBQueryAssistant(connection_string, groq_api_key)
    
    # User input
    user_question = st.text_input("Enter your question:", "")
    
    if user_question:
        # First, validate if the question is database-related
        if not assistant.validate_query_context(user_question):
            st.error("Sorry, your question doesn't appear to be related to the database. Please ask a question about the data available in the system.")
            return
            
        # Generate initial SQL query
        sql_query = assistant.generate_sql_query(user_question)
        st.text("Generated Query")
        st.code(sql_query,language="sql")
        
        # Execute query and check for empty results
        try:
            with pyodbc.connect(connection_string) as conn:
                df = pd.read_sql(sql_query, conn)

            st.text((df[''] == 0).all())
            st.text(df.columns)
            st.text(len(df.columns))
            st.text(df.shape)
            if df.columns.empty:
                st.text("Empty column")

            if df.empty or (len(df.columns) == 1 and (df[''] == 0).all()):
                # Try to identify potential mismatches
                corrections = {}
                st.text(assistant.db_schema.items())
                st.text('----')
                for table_name, table_info in assistant.db_schema['tables'].items():
                    # st.text("table_name")
                    # st.text(table_name)
                    # st.text(table_info)
                    # st.text("column")
                    # st.text(table_info['columns'])
                    # st.text('----')

                    for column in table_info['columns']:
                        # Look for words in the user question that might be values
                        words = user_question.split()
                        for word in words:
                            similar_values = assistant.find_similar_values(table_name, column, word)
                            if similar_values:
                                corrections[f"{table_name}.{column}"] = similar_values
                
                if corrections:
                    st.warning("No results found. Did you mean one of these?")
                    
                    selected_corrections = {}
                    for field, values in corrections.items():
                        selected = st.selectbox(
                            f"Select correct value for {field}:",
                            options=values
                        )
                        if selected:
                            selected_corrections[field] = selected
                    
                    if st.button("Run with corrections"):
                        corrected_query = assistant.generate_sql_query(user_question, selected_corrections)
                        with pyodbc.connect(connection_string) as conn:
                            df = pd.read_sql(corrected_query, conn)
                        st.write("Results:")
                        st.dataframe(df)
                        st.code(corrected_query, language="sql")
                else:
                    st.error("No results found and no similar values could be suggested.")
            else:
                st.write("Results:")
                st.dataframe(df)
                st.code(sql_query, language="sql")
                
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
            error_message = f"Error executing query: {str(e)}"
            st.error(error_message)  # Display error in Streamlit app
            print(error_message)

if __name__ == "__main__":
    main()