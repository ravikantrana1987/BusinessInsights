import DatabaseConnectionManager
from langchain_groq import ChatGroq
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser, JsonOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.pydantic_v1 import BaseModel, Field
import json
import pandas as pd
from typing import Dict, Tuple
import re
import PromptManager

class BusinessInsightsGenerator:
    def __init__(self, connection_string: str, api_key: str):
        self.connection_string = connection_string
        
        self.llm = ChatGroq(api_key=api_key, model="llama-3.1-8b-instant", temperature=0.1)
        # self.llm = ChatGroq(api_key=api_key, model="deepseek-r1-distill-llama-70b", temperature=0.1)
        # self.llm = ChatGroq(api_key=api_key, model="mixtral-8x7b-32768", temperature=0.1)
        self.db_connection_manager = DatabaseConnectionManager.DatabaseConnectionManager(self.connection_string)
        self.schema_details = self.db_connection_manager.get_schema_info()
        self.prompt_manager = PromptManager.PromptManager()
        self.question:str
        # print(self.schema_details)

    def _get_query_generation_prompt(self):
        return PromptTemplate(
            input_variables=["schema","question"],
            template="""
                Given the following MS SQL Server database schema:
                {schema}

                Generate a SQL query to answer this question:
                "{question}"
                
                Important - Don't use unnecessary JOIN, if not needed
                Important - output should only contain the SQL query, it should not contain explaination and what you think or understand part.
         
                Requirements:
                0. Important - generated sql query should contain relationship column names in the "SELECT " sections of query which is provided in the database shema relationships section.
                1. Use only the tables and columns that exist in the schema
                2. Use appropriate JOINs based on the relationships provided
                3. Include relevant aggregations and groupings
                4. Apply appropriate date filters if time-based analysis is needed
                5. Optimize the query for performance
                6. Think to use LEFT JOINs in the SQL query where its important to include the data (not neccessarily).
                7. Think to apply LIKE caluse where need in the result filteration (not neccessarily) Don't apply where exact keyword is provided.
                8. provide an appropriate alias name to the column, that should be precise and should not contain any puntuation marks like ('_','-') etc., you can use Pascal case
                9. If you are using the aggregate operation in the SQL query, it should include the Column name in output sql query.
                10. Apply proper formating to the generated SQL Query
                11. Instead of LIMIT use TOP in the SQL Query

                Note : SQL query should only contains required columns only.
                Important to note - Provide only SQL Query do not add any explanations 
                Important - Generated SQL query should not contain reference column names in the "SELECT" part of the query, it should not be the part of the output of the result.
                Important - If you are using the aggregate operation in the SQL query, it should include the Column name against which aggregate operation has been done in output sql query.
                Important - Don't use unnecessary JOIN, if not needed

                Output: Only the SQL query, without explanations or additional text.
            """
        )

    def generate_sql_query(self, question:str) -> str:
        self.question = question
        prompt = self._get_query_generation_prompt()

        # create chain
        query_chain = (
            {"schema": RunnablePassthrough(), "question": RunnablePassthrough()}
            | prompt
            | self.llm
            | StrOutputParser()
        )
        try:
            schema_json = json.dumps(self.schema_details, indent=2)
            # print(schema_json)
            sql_query = query_chain.invoke({
                "schema": schema_json,
                "question": question
            })
            validated_sql_query = self._validate_and_clean_query(sql_query)
            # print("Generated SQL Query:")
            # print(sql_query)
            # print("cleanded returned SQL Query:")
            # print(validated_sql_query)

            # print("++++++++ Result +++++")
            return validated_sql_query

            # result,narrative =self.get_result(validated_sql_query)
        except Exception as ex:
            print(f"Query generation failed: {str(ex)}")
    
    def _validate_and_clean_query(self, query: str) -> str:
        cleanedQuery = self.remove_think_tags(query)
        query = cleanedQuery.replace('```sql', '').replace('```', '').replace(";",'').replace("\\",'').strip()        
        
        referenced_tables = set()
        for table in self.schema_details["tables"].keys():
            if table.lower() in query.lower():
                referenced_tables.add(table)
        
        if not referenced_tables:
            raise ValueError("Generated query does not reference any valid tables")
        
        return query
    
    def remove_think_tags(self, text: str) -> str:
        # Use regular expression to remove content between <think> and </think>
        return re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    
    def get_result(self, query: str) -> Tuple[pd.DataFrame, str]:
        try:             
            df = self.db_connection_manager.execute_query(query)
            narrative = self._generate_narrative_insights(df) 
            print(df.to_json(orient='records'))
            # print(narrative)
            return df, narrative
        except Exception as e:
            print(f"Query execution failed: {str(e)}")
            return None, None
    
    def _generate_narrative_insights(self, df: pd.DataFrame) -> str:
        try:
            # Create a prompt template for insights generation
            insights_template = PromptTemplate(
                input_variables=["data_description","question"],
                template="""
                Analyze this dataset and provide key insights in natural language:
                {data_description} based on the asked question "{question}"
                
                Focus on:
                1. Key trends and patterns
                2. Notable changes or anomalies
                3. Business implications
                """
            )

            # Create the chain
            insights_chain = (
                {"data_description": RunnablePassthrough(), "question": RunnablePassthrough()}
                | insights_template
                | self.llm
                | StrOutputParser()
            )

            # Prepare data description
            data_description = df.describe().to_json()

            # Generate narrative 
            narrative = insights_chain.invoke({
                "data_description": data_description,
                "question":self.question
            })

            narrative = self.remove_think_tags(narrative)
            
            # print(narrative)
            
            return narrative
        except Exception as e:
            return f"Narrative generation failed: {str(e)}"
        
    def validate_query_context(self, user_question: str) -> Tuple[bool, str, str]:
        """Check if the question is related to the database context"""
        # Create a comprehensive system prompt
        tables_info = []
        for table_name, table_info in self.schema_details['tables'].items():
            columns = ", ".join(f"{col} ({dtype})" for col, dtype in table_info['columns'].items())
            tables_info.append(f"Table '{table_name}' contains: {columns}")
        
        relationships_info = []
        for rel in self.schema_details['relationships']:
            relationships_info.append(
                f"Table '{rel['table']}' is related to '{rel['referenced_table']}' "
                f"through {rel['column']} = {rel['referenced_column']}"
            )
        
        system_prompt = self._generate_user_question_context_validator_prompt(tables_info, relationships_info)
        print("system_prompt")
        print(system_prompt)
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Question: {user_question}")
        ]
        try:
            parser = JsonOutputParser(pydantic_object = UserQueryContext)
            chain = self.llm | parser
            response = chain.invoke(messages)
            return response['is_related'], response['reasoning'], response['reframed_question']
        except:
            # Fallback to a more permissive validation
            question_lower = user_question.lower()
            table_names = [table.lower() for table in self.schema_details['tables'].keys()]
            
            # Check if any table name or its singular/plural form is mentioned
            for table in table_names:
                if (table in question_lower or 
                    table[:-1] in question_lower or  # singular
                    table + 's' in question_lower):  # plural
                    return True, f"Question mentions table - {table}"
            
            # Check for common analytical terms
            analytical_terms = ['how many', 'count', 'list', 'show', 'find', 'get', 'what', 'which']
            if any(term in question_lower for term in analytical_terms):
                return True, "Question appears to be an analytical query"
                
            return False, "Question doesn't appear to be related to the database"        
        
    # Prompt methods
    def _generate_user_question_context_validator_prompt(self, table_info, relationship_info):
        try:
            prompt = self.prompt_manager.get_prompt(
                "user_question_context_validator",
                tables_info=chr(10).join(table_info),
                relationship_info=chr(10).join(relationship_info)
            )
            return prompt
        except Exception as e:
            print(f"Error generating prompt: {str(e)}")
            raise
        
class UserQueryContext(BaseModel):
    reasoning: str = Field(description="Provide Reasoning of the user query context")
    is_related:bool = Field(description="Based on Reasoning, provide is it realted to Business")
    reframed_question:str = Field(description="Based on Reasoning, provide user to reframe the question")
