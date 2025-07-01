from openai import OpenAI
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
import pandas as pd
import os
import streamlit as st
import re
from typing import Dict, Any, List, Optional, Union
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize the OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Initialize the LLM
def get_llm(messages, model="gpt-3.5-turbo", temperature=0.1):
    """
    Get a completion from the OpenAI API.
    
    Args:
        messages: List of message dictionaries with 'role' and 'content'
        model: The model to use (default: gpt-3.5-turbo)
        temperature: Controls randomness (0.0 to 2.0)
        
    Returns:
        The response from the OpenAI API
    """
    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature
        )
        return response
    except Exception as e:
        print(f"Error in get_llm: {str(e)}")
        raise

def find_table(data, sheet_name: str = None, table_name: str = None):
    """Find a specific table by sheet and table name."""
    results = []
    for table in data:
        current_sheet = table.get('sheet', '').lower()
        current_table = table.get('table', '').lower()
        
        if sheet_name and sheet_name.lower() not in current_sheet:
            continue
            
        if table_name and table_name.lower() not in current_table:
            continue
            
        results.append(table)
    return results

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean DataFrame by removing all-null columns and converting to string."""
    df_cleaned = df.dropna(axis=1, how='all')
    for col in df_cleaned.columns:
        if not pd.api.types.is_numeric_dtype(df_cleaned[col]):
            df_cleaned[col] = df_cleaned[col].astype(str)
    return df_cleaned

def display_table(table_data: Dict[str, Any], max_rows: int = 100, max_columns: int = 20) -> Optional[str]:
    """
    Display a table using Streamlit's dataframe component.
    """
    if not table_data:
        return "Error: No table data provided."
    
    data = None
    for key in ['data', 'sample_data', 'table_data', 'values']:
        if key in table_data and table_data[key]:
            data = table_data[key]
            break
    
    if not data:
        return "No data available for this table."
    
    try:
        df = pd.DataFrame(data)
        
        if 'columns' in table_data and table_data['columns']:
            columns = table_data['columns']
            df.columns = columns if len(columns) == len(df.columns) else columns + [
                f'Column_{i}' for i in range(len(columns), len(df.columns))
            ]
        
        df = clean_dataframe(df)
        
        if df.empty:
            return "No valid data to display after cleaning."
        
        total_rows = len(df)
        total_columns = len(df.columns)
        
        with st.container():
            info = []
            if 'sheet' in table_data and table_data['sheet']:
                info.append(f"**Sheet:** {table_data['sheet']}")
            if 'table' in table_data and table_data['table']:
                info.append(f"**Table:** {table_data['table']}")
            
            if info:
                st.markdown(" | ".join(info))
            
            dim_info = f"Showing {min(total_rows, max_rows)} of {total_rows} rows"
            if total_columns > max_columns:
                dim_info += f", {max_columns} of {total_columns} columns"
            
            if total_rows > max_rows or total_columns > max_columns:
                dim_info += " (use more specific queries to see more data)"
                
            st.caption(dim_info)
            
            if not df.empty:
                display_rows = min(max_rows, total_rows)
                display_cols = min(max_columns, total_columns)
                
                st.dataframe(
                    df.iloc[:display_rows, :display_cols],
                    use_container_width=True,
                    height=min(500, (min(display_rows, 15) + 1) * 35 + 3),
                    hide_index=True
                )
        
        return None
    
    except Exception as e:
        error_msg = f"Error displaying table: {str(e)}"
        print(f"Debug - {error_msg}")
        import traceback
        traceback.print_exc()
        return error_msg

def generate_summary(df: pd.DataFrame, table_name: str = None, sheet_name: str = None) -> str:
    """
    Generate a summary of the DataFrame using the LLM.
    """
    try:
        data_preview = df.head(10).to_string()
        
        prompt = f"""Please analyze the following data and provide a concise summary:
        
        Table: {table_name or 'Unnamed Table'}
        Sheet: {sheet_name or 'N/A'}
        Shape: {df.shape[0]} rows x {df.shape[1]} columns
        
        First 10 rows:
        {data_preview}
        
        Please provide:
        1. The type of data in this table
        2. Key columns and their purposes
        3. Any notable patterns or insights
        4. Potential use cases for analysis"""
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a data analyst assistant that provides clear, concise summaries of tabular data."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Could not generate summary: {str(e)}"

def analyze_table(analysis_data: List[Dict], question: str) -> str:
    """
    Analyze table data from multiple sheets and answer questions using OpenAI's API.
    
    Args:
        analysis_data: List of dictionaries containing table data with metadata
        question: User's question about the data
        
    Returns:
        str: Generated analysis response with rich formatting
    """
    try:
        if not analysis_data or not isinstance(analysis_data, list):
            return "Error: No valid data provided for analysis"
        
        # Count total sheets and tables
        sheets = {}
        total_tables = 0
        for data in analysis_data:
            sheet_name = data.get('sheet', 'Unknown Sheet')
            if sheet_name not in sheets:
                sheets[sheet_name] = 0
            sheets[sheet_name] += 1
            total_tables += 1
        
        # Prepare system message with instructions for analyzing multiple sheets
        system_message = {
            "role": "system",
            "content": f"""# Multi-Sheet Excel Data Analysis Assistant

You are an expert data analyst AI that helps users understand and work with Excel data across {len(sheets)} sheets and {total_tables} tables. Your responses should be:

## Response Guidelines:
- **Accuracy**: Base responses strictly on the provided data from all sheets and tables
- **Clarity**: Use clear, concise language with proper Markdown formatting
- **Insightful**: Provide meaningful analysis and insights across all sheets and tables
- **Structured**: Organize information with headers, lists, and tables
- **Helpful**: Offer explanations and context for non-technical users
- **Honest**: Acknowledge data limitations when present

## Data Analysis Approach:
1. **Understand the Data**:
   - Review all provided tables and their structures across all sheets
   - Note column names, data types, and sample values
   - Identify relationships between tables and sheets
   - Pay attention to the sheet and table names for context

2. **Analyze the Query**:
   - Carefully read and understand the user's question
   - Identify which sheets and tables are relevant to the question
   - Consider any calculations or transformations needed across sheets
   - If specific sheet/table is mentioned, focus on that data

3. **Provide Response**:
   - Start with a direct answer to the query
   - Include supporting data and calculations from relevant sheets/tables
   - Clearly indicate which sheet and table each piece of data comes from
   - Explain your reasoning and methodology
   - Highlight any limitations or assumptions
   - Suggest follow-up questions or analyses

4. **Formatting**:
   - Use Markdown for clear formatting
   - Include tables for tabular data (use markdown tables)
   - Use bullet points for lists
   - **Bold** important information
   - Use headers to organize different sections
   - Always mention the sheet and table names when referencing data
   - Use code blocks for data samples or calculations

5. **Important Notes**:
   - The data comes from {len(sheets)} sheets: {', '.join(sheets.keys())}
   - Each sheet may contain multiple tables
   - Pay attention to the table names and sheet names in the data
   - If the user asks about a specific sheet or table, make sure to reference the correct one"""
        }
        
        # Prepare the data for the prompt
        prompt_parts = [f"# USER QUESTION:\n{question}\n"]
        
        # Add summary of available data
        prompt_parts.append("## AVAILABLE DATA SUMMARY")
        prompt_parts.append(f"- Total Sheets: {len(sheets)}")
        prompt_parts.append(f"- Total Tables: {total_tables}")
        
        # Group data by sheet
        sheets_data = {}
        for data in analysis_data:
            sheet_name = data.get('sheet', 'Unknown Sheet')
            if sheet_name not in sheets_data:
                sheets_data[sheet_name] = []
            sheets_data[sheet_name].append(data)
        
        # Add information about each sheet and its tables
        for sheet_name, tables in sheets_data.items():
            prompt_parts.append(f"\n## 📑 SHEET: {sheet_name}")
            prompt_parts.append(f"**Tables in this sheet:** {len(tables)}")
            
            for table_data in tables:
                table_name = table_data.get('table', 'Table')
                total_rows = table_data.get('total_rows', 0)
                columns = table_data.get('columns', [])
                sample_data = table_data.get('sample_data', [])
                
                prompt_parts.append(f"\n### Table: {table_name}")
                prompt_parts.append(f"- **Total Rows**: {total_rows:,}")
                prompt_parts.append(f"- **Columns**: {', '.join(columns)}")
                
                # Add a sample of the data (first 3 rows)
                if sample_data and len(sample_data) > 0:
                    try:
                        sample_df = pd.DataFrame(sample_data)
                        if not sample_df.empty:
                            prompt_parts.append("\n**Sample Data (first 3 rows):**")
                            prompt_parts.append(sample_df.head(3).to_markdown(index=False))
                    except Exception as e:
                        print(f"Error formatting sample data: {str(e)}")
                prompt_parts.append("\n---")
        
        # Add the user's question again for clarity
        prompt_parts.append(f"\n# QUESTION TO ANSWER:\n{question}")
        
        # Add instructions for the response
        prompt_parts.append("""
## INSTRUCTIONS FOR YOUR RESPONSE:
1. Start with a clear, concise answer to the question
2. Reference specific sheets and tables when providing data
3. Include relevant data points and calculations
4. Format your response with markdown for clarity
5. If multiple sheets/tables are relevant, compare and contrast the data
6. If the question is unclear or data is missing, ask for clarification
""")
        
        # Combine all parts into the final prompt
        full_prompt = "\n".join(prompt_parts)
        
        # Debug: Print the prompt length
        print(f"Prompt length: {len(full_prompt)} characters")
        
        # Create the user message
        user_message = {
            "role": "user",
            "content": full_prompt
        }
        
        # Get the response from the model
        response = client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[system_message, user_message],
            temperature=0.2,  # Lower temperature for more factual responses
            max_tokens=4000,  # Increased token limit for comprehensive responses
            top_p=0.9,
            frequency_penalty=0.1,
            presence_penalty=0.1
        )
        
        # Extract and return the response
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in analyze_table: {error_details}")
        return f"Error analyzing table data: {str(e)}\n\nPlease try again with a more specific question or check if the data is properly loaded."

def generate_chat_response(chat_history: List[Dict[str, str]], current_question: str, table_context: pd.DataFrame = None) -> str:
    """Generate a response for the chat interface using the OpenAI API."""
    if table_context is not None and not table_context.empty:
        return analyze_table([{'data': table_context.to_dict('records')}], current_question)
    
    try:
        messages = [
            {
                "role": "system",
                "content": "You are a helpful assistant that helps users with their questions."
            }
        ]
        
        for msg in chat_history[-5:]:
            messages.append({
                "role": msg["role"],
                "content": msg["content"]
            })
        
        messages.append({
            "role": "user",
            "content": current_question
        })
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=messages,
            temperature=0.1,
            max_tokens=1000
        )
        
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        return f"Error generating response: {str(e)}"