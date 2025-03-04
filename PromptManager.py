class PromptManager:
    def __init__(self):
        # Define prompts
        self.prompts = {
            # 1. user_question_context_validator
            "user_question_context_validator": """
                You are an expert database query validator analyzing whether a question can be answered using the provided database. Below are the details of the database schema:

                Tables:
                {tables_info}

                Relationships:
                {relationship_info}

                A question is considered related to the database if it satisfies one or more of the following conditions:
                0. All the administrative request should not be accepted.
                1. Mentions table names: This includes variations or synonyms (e.g., 'category' for 'categories').
                2. Asks about columns or their data: For example, requesting specific data from a column.
                3. Inquires about relationships between tables: For instance, questions about how tables are linked or refer to each other.
                4. Requests aggregates or summaries: This includes queries like counts, averages, or any other form of analytics.
                5. Uses business terminology: The question uses terms clearly related to the database structure (e.g., 'sales' mapping to a sales table).
                6. Queries matching table or column names: The question involves table or column names that match the database schema directly.
                7. Context of the question: Even if the question mentions a table or column, carefully analyze its context. Does the question aim to fetch data, or is it merely descriptive? For example, "What is Product?" may not request data extraction but is more of a general inquiry.
                8. Question related to list or view the Database, table, its schema or any request to insert, update or delete the record from the database, It should be avoided.
                9. If use ask all the tables or its relationship, should not provide the administative details of database, table, its relationship to the users.

                Your task is to analyze the given question and return a JSON response with the following fields:
                    NOTE - rethink all the above instruction to provide the below response
                
                - "reasoning": A brief explanation of why the question is or isn't related to the database. The reasoning should include terms like 'database', 'table', 'column', etc., but should not explicitly expose actual table or column names.
                - "is_related": Based on the reasoning i.e in "reasoning" and revist all the above instruction to provide the response, provide value as - true/false — Indicates whether the question is related to the database. 
                - "reframed_question": If "is_related" is "false" smiply reject the request and If the request is not related to administrative tasks, suggest a polite and relevant way the user can rephrase their question. Focus on the business context rather than database specifics. For example, if the user asks about data deletion, suggest they focus on what information or records they are trying to access or modify instead.

                Note: Ensure that the user feels comfortable to reframe their request in a way that suits the scope of the AI’s capabilities, without directly referring to any database details or technical terms. Do not include the prompt instructions in the final output.

                Example Denial Message:
                "Apologies, I am unable to perform administrative actions such as deleting or modifying records in the database. For this request, please reach out to your system administrator or follow the internal process. Let me know if I can help with anything else."

            """,
            
            # 2. SQL query_generation_prompt
            "sql_query_generation_prompt": """
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

                Note : SQL query should only contains required columns only.
                Important to note - Provide only SQL Query do not add any explanations 
                Important - Generated SQL query should not contain reference column names in the "SELECT" part of the query, it should not be the part of the output of the result.
                Important - If you are using the aggregate operation in the SQL query, it should include the Column name against which aggregate operation has been done in output sql query.
                Important - Don't use unnecessary JOIN, if not needed

                Output: Only the SQL query, without explanations or additional text.

            """
        }

    
    def get_prompt(self, prompt_name, **kwargs):
        """
        Retrieve and format a prompt with the provided parameters
        """

        try:
            print("Retrieved Parametes")
            # print(**kwargs)
            prompt_template = self.prompts.get(prompt_name)
            print("Get template")
            print(prompt_template)
            if prompt_template:
                formatted_prompt = prompt_template.format(**kwargs)
                print("Formatted")
                print(formatted_prompt)
                return formatted_prompt
            else:
                raise ValueError(f"Prompt '{prompt_name}' not found")
        except Exception as ex:
            print(f"Prompt execution failed: {str(ex)}")
        