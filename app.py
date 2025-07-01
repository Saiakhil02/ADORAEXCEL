import os
import sys
import streamlit as st
import pandas as pd
import time
import json
import requests
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from excel_parser import extract_all_tables  # Updated import
from plotly_graphs import generate_and_render_graph, detect_graph_request  # Replace mermaid_graphs
from models import ExcelFile, ExcelTable, ChatHistory, GraphHistory  # Add GraphHistory
# Load environment variables from .env file in the same directory as app.py
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(env_path)

# Debug: Print current working directory and .env path
print(f"Current working directory: {os.getcwd()}")
print(f"Loading .env from: {env_path}")

# OpenAI API Configuration
try:
    # Try to load from environment
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '').strip()  # Add strip() to remove any whitespace
    
    # Debug output
    print(f"Loaded OPENAI_API_KEY length: {len(OPENAI_API_KEY) if OPENAI_API_KEY else 0}")
    
    if not OPENAI_API_KEY:
        st.error("❌ OPENAI_API_KEY not found in environment variables. Please check your .env file.")
        st.stop()
        
    # Simple validation that the key starts with 'sk-'
    if not OPENAI_API_KEY.startswith('sk-'):
        st.error(f"❌ Invalid API key format. OpenAI API key should start with 'sk-'. Got: '{OPENAI_API_KEY[:10]}...'")
        st.stop()
        
    print("✅ OpenAI API key loaded and validated successfully")
    
    # Test the API key with a simple request to OpenAI
    test_headers = {
        'Authorization': f'Bearer {OPENAI_API_KEY}',
        'Content-Type': 'application/json'
    }
    test_data = {
        'model': 'gpt-3.5-turbo',
        'messages': [{'role': 'user', 'content': 'test'}],
        'max_tokens': 5
    }
    
    try:
        response = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers=test_headers,
            json=test_data,
            timeout=10
        )
        if response.status_code == 200:
            print("✅ OpenAI API key is valid and working")
        else:
            st.error(f"❌ OpenAI API key validation failed with status {response.status_code}: {response.text}")
            st.stop()
    except Exception as e:
        st.error(f"❌ Error validating OpenAI API key: {str(e)}")
        st.stop()
        
except Exception as e:
    st.error(f"❌ Error initializing OpenAI API: {str(e)}")
    st.stop()

# Initialize OpenAI client with GPT model by default
def get_openai_response(messages: List[Dict[str, str]], 
                     model: str = "gpt-3.5-turbo",
                     temperature: float = 0.3,  # Lower temperature for more focused responses
                     max_tokens: int = 1000,  # Adjusted for OpenAI models
                     top_p: float = 0.9) -> str:  # Adjusted for better results with GPT
    """
    Get response from OpenAI API with enhanced error handling and parameters
    
    Args:
        messages: List of message dictionaries with 'role' and 'content'
        model: The model to use (default: gpt-3.5-turbo)
        temperature: Controls randomness (0.0 to 2.0)
        max_tokens: Maximum number of tokens to generate
        top_p: Controls diversity via nucleus sampling (0.0 to 1.0)
        
    Returns:
        str: Generated response content or error message
    """
    try:
        # Get API key from environment
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return "Error: OPENAI_API_KEY environment variable not set"
        
        # Validate parameters
        if not isinstance(messages, list) or not messages:
            return "Error: messages must be a non-empty list"
            
        if temperature < 0 or temperature > 2:
            return "Error: temperature must be between 0 and 2"
            
        if max_tokens <= 0 or max_tokens > 4096:  # Max tokens for most OpenAI models
            return "Error: max_tokens must be between 1 and 4096"
            
        if top_p <= 0 or top_p > 1:
            return "Error: top_p must be between 0 and 1"
            
        # Prepare the API request
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "top_p": top_p,
            "stop": None
        }
        
        # Make the API request
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=60  # Increased timeout for larger responses
        )
        
        # Handle the response
        if response.status_code == 200:
            result = response.json()
            return result["choices"][0]["message"]["content"]
        else:
            error_msg = f"OpenAI API request failed with status {response.status_code}"
            try:
                error_detail = response.json().get("error", {}).get("message", "No error details provided")
                error_msg += f": {error_detail}"
            except:
                error_msg += f": {response.text}"
            return f"Error: {error_msg}"
            
    except requests.exceptions.RequestException as e:
        return f"Error: Request failed - {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"

def analyze_table(analysis_data: List[Dict], prompt: str) -> str:
    """
    Analyze the table data and generate a comprehensive response using OpenAI's API.
    
    Args:
        analysis_data: List of dictionaries containing table data with metadata
        prompt: User's query about the data
        
    Returns:
        str: Generated analysis response with rich formatting
    """
    try:
        # Prepare system message with detailed instructions for the AI
        system_message = {
            "role": "system",
            "content": """# Excel Data Analysis Assistant

You are an expert data analyst AI that helps users understand and work with Excel data. You have COMPLETE ACCESS to ALL DATA in the Excel file.

## CRITICAL INSTRUCTIONS:
1. You have access to the ENTIRE DATASET, not just a sample
2. The data is provided in a JSON format with all rows included
3. Use Python list slicing to access any part of the data:
   - `data[-1]` for the last row
   - `data[-3:]` for the last 3 rows
   - `len(data)` to get total row count
   - `[row['column_name'] for row in data]` to extract a column

## Data Access Examples:
- Last row: `data[-1]`
- Last 3 rows: `data[-3:]`
- Specific column: `[row['column_name'] for row in data]`
- Filtered data: `[row for row in data if condition]`

## Response Guidelines:
- **Accuracy**: Base responses on the complete dataset
- **Completeness**: Analyze all relevant data points
- **Clarity**: Use clear, concise language with Markdown formatting
- **Evidence**: Include specific data points to support your answers
- **Honesty**: If data is insufficient, explain what's missing

## For Specific Queries:
- When asked for "last X rows", use negative indexing: `data[-X:]`
- For row counts, use `len(data)` which matches `total_rows`
- For column operations, use list comprehensions
- Always verify data exists before accessing it

## Important Notes:
- The data preview is just for human readability
- You have access to ALL rows in the JSON data structure
- Use Python's list and dictionary operations to analyze the data
- If a query is about specific rows or ranges, access them directly using the methods above"""
        }
        
        # Prepare data context with better formatting and more details
        def format_data_context():
            context = []
            for idx, table in enumerate(analysis_data, 1):
                table_info = []
                table_name = table.get('table', f'Table {idx}')
                sheet_name = table.get('sheet', 'Unknown Sheet')
                total_rows = table.get('total_rows', 0)
                
                # Table header with metadata
                table_info.append(f"### 📊 {table_name} (Sheet: {sheet_name})\n")
                table_info.append(f"- **Total Rows:** {total_rows:,}\n")
                
                # Add column information with types
                columns = table.get('columns', [])
                column_types = table.get('column_types', {})
                if columns:
                    table_info.append("**Columns:**\n")
                    for col in columns:
                        col_type = column_types.get(col, 'unknown')
                        table_info.append(f"  - `{col}` ({col_type})\n")
                    table_info.append("\n")
                
                # Get all data for analysis
                all_data = table.get('data', [])
                if all_data:
                    total_rows = len(all_data)
                    table_info.append(f"**Complete Data Table ({total_rows} total rows):**\n\n")
                    
                    # Add a note about data availability
                    table_info.append("*Note: You have access to ALL ROWS of data for analysis. "
                                    "The table below shows a preview of the data structure.*\n\n")
                    
                    # For very large tables, limit the display to avoid overwhelming the context window
                    if total_rows > 20:
                        table_info.append(f"*Displaying first 5 rows of {total_rows} total rows. "
                                        "All rows are available for analysis.*\n\n")
                    
                    # Show column headers
                    headers = " | ".join(columns)
                    separators = " | ".join([":---"] * len(columns))
                    table_info.append(f"| {headers} |\n")
                    table_info.append(f"|{separators}|\n")
                    
                    # For small datasets, show all rows
                    # For large datasets, just show a few rows as example
                    max_display_rows = 5 if total_rows > 10 else total_rows
                    for row in all_data[:max_display_rows]:
                        row_data = []
                        for col in columns:
                            cell = str(row.get(col, "")).replace("\n", " ")
                            row_data.append(cell[:50] + ("..." if len(cell) > 50 else ""))
                        table_info.append("| " + " | ".join(row_data) + " |\n")
                    
                    if total_rows > max_display_rows:
                        table_info.append(f"| ... and {total_rows - max_display_rows} more rows ... |\n")
                    
                    table_info.append("\n")
                
                # Add basic statistics if available
                if columns and sample_data:
                    table_info.append("**Quick Statistics:**\n")
                    table_info.append("- Sample size: ", str(sample_size), " rows\n")
                    table_info.append("- Total columns: ", str(len(columns)), "\n\n")
                
                context.append("".join(table_info))
            
            return "\n\n".join(context)
        
        # Prepare the complete data context with all rows
        def get_complete_data_context():
            context = []
            for idx, table in enumerate(analysis_data, 1):
                table_info = []
                table_name = table.get('table', f'Table {idx}')
                sheet_name = table.get('sheet', 'Unknown Sheet')
                total_rows = table.get('total_rows', 0)
                
                # Table header with metadata
                table_info.append(f"### 📊 {table_name} (Sheet: {sheet_name})\n")
                table_info.append(f"- **Total Rows:** {total_rows:,}\n")
                
                # Add column information with types
                columns = table.get('columns', [])
                column_types = table.get('column_types', {})
                if columns:
                    table_info.append("**Columns:**\n")
                    for col in columns:
                        col_type = column_types.get(col, 'unknown')
                        table_info.append(f"  - `{col}` ({col_type})\n")
                    table_info.append("\n")
                
                # Add complete data
                all_data = table.get('data', [])
                if all_data:
                    # For large datasets, we'll include the data in a more efficient format
                    if total_rows > 20:
                        table_info.append(f"**Complete Dataset Summary ({total_rows} rows):**\n\n")
                        table_info.append("*Note: The complete dataset is available for analysis. "
                                      "The AI can access all rows to answer your questions.*\n\n")
                        
                        # Show first few and last few rows to give context
                        for i, row in enumerate(all_data[:3]):
                            row_data = [f"Row {i+1}:"]
                            for col in columns[:5]:  # Limit to first 5 columns for display
                                cell = str(row.get(col, "")).replace("\n", " ")
                                row_data.append(f"{col}: {cell[:30]}" + ("..." if len(cell) > 30 else ""))
                            table_info.append("  - " + " | ".join(row_data) + "\n")
                        
                        if total_rows > 6:
                            table_info.append(f"  - ... {total_rows - 6} more rows ...\n")
                            
                            # Show last 3 rows
                            for i in range(max(3, total_rows-3), total_rows):
                                row = all_data[i]
                                row_data = [f"Row {i+1}:"]
                                for col in columns[:5]:  # Limit to first 5 columns for display
                                    cell = str(row.get(col, "")).replace("\n", " ")
                                    row_data.append(f"{col}: {cell[:30]}" + ("..." if len(cell) > 30 else ""))
                                table_info.append("  - " + " | ".join(row_data) + "\n")
                    else:
                        # For smaller datasets, show all data
                        table_info.append(f"**Complete Data Table ({total_rows} rows):**\n\n")
                        headers = " | ".join(columns)
                        separators = " | ".join([":---"] * len(columns))
                        table_info.append(f"| {headers} |\n")
                        table_info.append(f"|{separators}|\n")
                        
                        for row in all_data:
                            row_data = []
                            for col in columns:
                                cell = str(row.get(col, "")).replace("\n", " ")
                                row_data.append(cell[:30] + ("..." if len(cell) > 30 else ""))
                            table_info.append("| " + " | ".join(row_data) + " |\n")
                    
                    table_info.append("\n")
                
                # Add the complete data in a structured format for the AI
                table_info.append("\n**Complete Data (for analysis):**\n")
                table_info.append("```json\n")
                
                # For analysis, we'll include all data but in a more compact format
                analysis_data = {
                    "table_name": table_name,
                    "sheet_name": sheet_name,
                    "total_rows": total_rows,
                    "columns": columns,
                    "data": all_data  # Include all data for analysis
                }
                
                # Convert to JSON with minimal whitespace to save tokens
                table_info.append(json.dumps(analysis_data, separators=(',', ':')))
                table_info.append("\n```\n")
                
                # Add explicit instructions for specific queries
                if total_rows > 0:
                    table_info.append("\n**Data Access Notes:**\n")
                    table_info.append("- To access the last row: `data[-1]`\n")
                    table_info.append("- To access the last 3 rows: `data[-3:]`\n")
                    table_info.append("- To count rows: `total_rows` or `len(data)`\n")
                    table_info.append("- To get a specific column: `[row['column_name'] for row in data]`\n\n")
                
                context.append("".join(table_info))
            
            return "\n\n".join(context)

        # Prepare the user message with complete data context
        user_message = {
            "role": "user",
            "content": f"""# 📊 Excel Data Analysis Request

## 📂 Data Overview
{format_data_context()}

## ❓ User Query
{prompt}

## 🔍 IMPORTANT DATA ACCESS NOTES:
1. You have access to ALL ROWS in the 'data' array of each table
2. To access data, use these patterns:
   - Last row: `data[-1]`
   - Last 3 rows: `data[-3:]`
   - First 5 rows: `data[:5]`
   - Specific column: `[row['column_name'] for row in data]`
   - Row count: `len(data)`

3. The data preview is for display only - always use the complete dataset in the JSON structure for your analysis
4. For any request about specific rows or ranges, access them directly using the methods above
5. When showing results, include the actual data points from the complete dataset

## 📝 Response Requirements:
- Start with a clear, direct answer to the query
- Include specific data points from the complete dataset to support your answer
- If showing a subset of data, indicate if it's a sample from a larger dataset
- For any calculations, state that they're based on the complete dataset
- If the query is about specific rows (e.g., 'last 3 rows'), show the actual data from those rows
4. **Format the Response**:
   - Start with a direct answer to the question
   - Provide supporting evidence and calculations from the complete dataset
   - Use Markdown formatting for clarity
   - Include relevant data points in your response
   - For large datasets, you can summarize but make it clear you're considering all data

## 🎯 Your Response (based on complete dataset analysis):
"""
        }
        
        # Get response from Groq with Llama 3
        with st.spinner("Analyzing your data..."):
            try:
                response = get_groq_response(
                    [system_message, user_message],
                    model="llama3-70b-8192",  # Using Llama 3 70B model
                    temperature=0.2,  # Lower temperature for more focused, deterministic responses
                    max_tokens=4000,  # Increased token limit for more detailed responses
                    top_p=0.9,  # Slightly higher top_p for better creativity when needed
                )
                
                # Post-process the response
                if not response or not response.strip():
                    return "I couldn't generate a response. The data might be too complex or the question might be unclear."
                
                # Clean up the response
                response = response.strip()
                
                # Ensure the response ends with proper punctuation
                if response and response[-1] not in {'.', '!', '?', ':', ';'}:  
                    response += '.'
                    
                # Add a friendly sign-off if the response is long enough
                if len(response.split()) > 50:  # If response is more than 50 words
                    response += "\n\n*Is there anything specific about this analysis you'd like me to elaborate on?*"
                    
                return response
                
            except Exception as api_error:
                error_msg = str(api_error)
                if "rate limit" in error_msg.lower():
                    return "I'm currently experiencing high demand. Please try again in a moment. If the issue persists, you may want to check your API rate limits."
                elif "timeout" in error_msg.lower():
                    return "The request timed out while processing your data. The dataset might be too large. Please try again with a more specific question or a smaller dataset."
                else:
                    return f"I encountered an error while processing your request: {error_msg}"
        
    except Exception as e:
        error_msg = f"Error analyzing data: {str(e)}"
        st.error(error_msg)
        return "I encountered an error while processing your request. Please try again later or rephrase your question."
from pathlib import Path
from datetime import datetime
import json
from typing import Dict, List, Any, Optional, Tuple
# Import our modules
from models import ExcelFile, ExcelTable, ChatHistory
import database as db
import excel_parser as parser
from serializers import serialize_data, prepare_for_db
from ai_utils import generate_chat_response, analyze_table

# Configuration
UPLOAD_FOLDER = "excel_uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Initialize database
db.init_db()

# Page configuration
st.set_page_config(
    page_title="Excel ChatBot", 
    page_icon="📊", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .file-card { border: 1px solid #ddd; border-radius: 10px; padding: 15px; margin-bottom: 15px; }
    .file-card:hover { box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
    .stButton>button { width: 100%; margin: 5px 0; }
    .chat-message { padding: 10px; border-radius: 5px; margin-bottom: 10px; }
    .chat-message.user { background-color: #e3f2fd; }
    .chat-message.assistant { background-color: #f5f5f5; }
    .table-container { margin: 10px; padding: 10px; border: 1px solid #eee; border-radius: 5px; }
    </style>
""", unsafe_allow_html=True)

def clean_dataframe(df):
    """Clean DataFrame by removing all-null columns and converting to string."""
    # Remove columns where all values are null/empty
    df_cleaned = df.dropna(axis=1, how='all')
    # Convert all data to string for consistent display
    return df_cleaned.astype(str)

def initialize_session_state():
    """Initialize session state variables."""
    if 'page' not in st.session_state:
        st.session_state.page = 'upload'
    if 'selected_file_id' not in st.session_state:
        st.session_state.selected_file_id = None
    if 'selected_sheet' not in st.session_state:
        st.session_state.selected_sheet = None
    if 'tables_data' not in st.session_state:
        st.session_state.tables_data = {}
    if 'processing_file' not in st.session_state:
        st.session_state.processing_file = False
    if 'upload_success' not in st.session_state:
        st.session_state.upload_success = None

# Initialize session state
initialize_session_state()

def show_upload_page():
    """Render the file upload page."""
    if st.session_state.page != 'upload':
        return
    
    st.title("📤 Upload New Excel File")
    
    # Display any existing success message
    if st.session_state.upload_success:
        file_id = st.session_state.upload_success.get('file_id')
        tables_data = st.session_state.upload_success.get('tables_data')
        
        st.success("Excel file processed successfully!")
        st.subheader("Tables Found")
        
        # Display table summary
        for sheet_name, tables in tables_data.items():
            with st.expander(f"📑 Sheet: {sheet_name}"):
                st.write(f"Found {len(tables)} tables")
                for table_name, table_data in tables.items():
                    with st.container():
                        # Get the display filename from session state or use a default
                        display_file_name = "Uploaded File"
                        if 'uploaded_file_name' in st.session_state:
                            display_file_name = st.session_state.uploaded_file_name
                        elif 'upload_success' in st.session_state and st.session_state.upload_success:
                            display_file_name = st.session_state.upload_success.get('file_name', 'Uploaded File')
                            
                        # File name display at the top with clean styling
                        st.markdown(
                            f"""
                            <div style='margin-bottom: 1.5rem;'>
                                <h3 style='color: #2c3e50; margin: 0; display: flex; align-items: center; gap: 8px;'>
                                    <span>📄</span>
                                    <span>{os.path.basename(display_file_name)}</span>
                                </h3>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                        df = pd.DataFrame(table_data)
                        df_cleaned = clean_dataframe(df)
                        # Display the table
                        st.dataframe(df_cleaned)
                        
                        # Download button
                        csv = df_cleaned.to_csv(index=False).encode('utf-8')
                        # Get the original file name from the session state or use a default
                        file_name = "table_data"
                        if 'uploaded_file_name' in st.session_state:
                            file_name = os.path.splitext(st.session_state.uploaded_file_name)[0]
                        elif 'upload_success' in st.session_state and st.session_state.upload_success:
                            file_name = os.path.splitext(st.session_state.upload_success.get('file_name', 'table_data'))[0]
                            
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv,
                            file_name=f"{file_name}_{sheet_name}_{table_name}.csv",
                            mime='text/csv',
                            key=f"dl_upload_{sheet_name}_{table_name}",
                            use_container_width=True
                        )
                        
                        # Removed chat interface section
        
        # Navigation buttons
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("📋 View All Files", use_container_width=True):
                st.session_state.page = 'browse'
                st.session_state.upload_success = None
                st.rerun()
        with col2:
            if st.button("💬 Chat with Sheet", use_container_width=True):
                st.session_state.selected_file_id = file_id
                st.session_state.tables_data = tables_data
                st.session_state.page = 'chat'
                st.session_state.upload_success = None
                st.rerun()
        
        # Add a separator
        st.markdown("---")
        st.subheader("Upload Another File")
    
    # File uploader
    uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx", "xls"])
    
    if uploaded_file and not st.session_state.get('processing_file', False):
        st.session_state.processing_file = True
        # Store the uploaded file name in session state
        st.session_state.uploaded_file_name = uploaded_file.name
        
        try:
            with st.spinner("Processing file..."):
                # Save the uploaded file
                file_path, file_hash = parser.save_uploaded_file(uploaded_file, UPLOAD_FOLDER)
                
                # Check for duplicate file (by hash or filename)
                is_duplicate, message = db.is_duplicate_file(file_hash, uploaded_file.name)
                if is_duplicate:
                    st.warning(message)
                    try:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                    except Exception as e:
                        st.error(f"Error cleaning up temporary file: {str(e)}")
                    st.session_state.processing_file = False
                    return
                
                # Extract tables from the Excel file
                tables_data = parser.extract_all_tables(file_path)
                
                if not tables_data:
                    st.error("No tables found in the Excel file.")
                    os.remove(file_path)
                    st.session_state.processing_file = False
                    return
                
                # Save to database
                try:
                    file_id = db.save_excel_file(
                        file_name=os.path.basename(file_path),
                        file_path=file_path,
                        file_hash=file_hash,
                        tables_data=tables_data
                    )
                    
                    # Store success state in session
                    st.session_state.upload_success = {
                        'file_id': file_id,
                        'tables_data': tables_data
                    }
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Error saving to database: {str(e)}")
                    if os.path.exists(file_path):
                        os.remove(file_path)
                
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            if 'file_path' in locals() and os.path.exists(file_path):
                os.remove(file_path)
        finally:
            st.session_state.processing_file = False

def show_browse_page():
    """Render the file browser page."""
    # Check if we're viewing a specific file's tables
    if 'viewing_file_id' in st.session_state and st.session_state.viewing_file_id:
        file_data = db.get_excel_file(st.session_state.viewing_file_id)
        if file_data:
            st.title(f"📄 {file_data['file_name']}")
            st.caption(f"Uploaded: {file_data['uploaded_at']}")
            
            
            # Display tables using the same style as upload page
            if file_data['tables']:
                sheet_names = list(file_data['tables'].keys())
                tabs = st.tabs([f"📑 {name}" for name in sheet_names])
                
                for idx, (sheet_name, tables) in enumerate(file_data['tables'].items()):
                    with tabs[idx]:
                        st.subheader(f"{sheet_name}")
                        
                        # Create tabs for tables within each sheet
                        if tables:
                            table_tabs = st.tabs([f"📊 {name}" for name in tables.keys()])
                            
                            for tab_idx, (table_name, table_data) in enumerate(tables.items()):
                                with table_tabs[tab_idx]:
                                    df = pd.DataFrame(table_data)
                                    # Create a cleaner card for each table
                                    with st.container():
                                        st.markdown(f"""
                                        <div style='background-color: #f8f9fa; 
                                                    border-radius: 10px; 
                                                    padding: 15px; 
                                                    margin-bottom: 15px;'>
                                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;'>
                                                <h4 style='margin: 0;'>📊 {table_name}</h4>
                                                <span style='color: #666; font-size: 0.9em;'>Sheet: {sheet_name}</span>
                                            </div>
                                        """, unsafe_allow_html=True)
                                        
                                        # Display the table with a fixed height and scroll
                                        df_cleaned = clean_dataframe(df)
                                        st.dataframe(
                                            df_cleaned,
                                            use_container_width=True,
                                            height=min(300, (min(10, len(df_cleaned)) + 1) * 35 + 3),
                                            hide_index=True
                                        )
                                        
                                        st.markdown("</div>", unsafe_allow_html=True)
                        else:
                            st.info("No tables found in this sheet.")
                
                # Back to All Files button in footer
                st.markdown("---")
                if st.button("← Back to All Files", key="back_to_files_footer", use_container_width=True):
                    st.session_state.viewing_file_id = None
                    st.rerun()
            
            return
    
    # If we get here, show the file list
    st.title("📋 Browse Excel Files")
    
    # Get files from database
    files = db.list_excel_files()
    
    if not files:
        st.info("No Excel files found. Upload a file to get started!")
    else:
        st.subheader(f"Found {len(files)} files")
        
        # Display files in a list with actions
        for file in files:
            # Format the datetime for display
            uploaded_at = file['uploaded_at']
            if hasattr(uploaded_at, 'strftime'):
                formatted_date = uploaded_at.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_date = str(uploaded_at)
                
            # Make the entire row clickable for chat
            col1, col2, col3 = st.columns([6, 1, 1])
            with col1:
                # Make the file name clickable to view details
                if st.button(f"📄 {file['file_name']} - {formatted_date}", 
                           key=f"view_{file['id']}", 
                           use_container_width=True,
                           type="primary"):
                    # Set the file to view
                    st.session_state.viewing_file_id = file['id']
                    st.session_state.page = 'file_detail'
                    st.rerun()
            with col2:
                if st.button("🗑️", 
                            key=f"delete_{file['id']}", 
                            help="Delete file"):
                    st.session_state.delete_file_id = file['id']
            with col3:
                if st.button("💬", 
                            key=f"chat_{file['id']}", 
                            help="Chat with this file",
                            type="primary"):
                    # Set the selected file for chat
                    st.session_state.selected_file = file['id']
                    st.session_state.page = 'chat'
                    # Clear any existing chat messages for this file
                    chat_key = f"chat_{file['id']}"
                    if 'chat_messages' in st.session_state and chat_key in st.session_state.chat_messages:
                        del st.session_state.chat_messages[chat_key]
                    st.rerun()
        
        # Handle file deletion with confirmation dialog
        if 'delete_file_id' in st.session_state and st.session_state.delete_file_id:
            file_id = st.session_state.delete_file_id
            file_to_delete = next((f for f in files if f['id'] == file_id), None)
            
            if file_to_delete:
                # Create a modal dialog for confirmation
                with st.sidebar:
                    st.warning("⚠️ Confirm Deletion")
                    st.write(f"You are about to delete: **{file_to_delete['file_name']}**")
                    st.write("This action cannot be undone.")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("✅ Confirm Delete", key=f"confirm_delete_{file_id}", type="primary"):
                            success, message = db.delete_excel_file(file_id)
                            if success:
                                st.success(message)
                                # Clear the selected file if it was deleted
                                if st.session_state.get('selected_file_id') == file_id:
                                    st.session_state.selected_file_id = None
                                # Clear the delete state and rerun to update the UI
                                del st.session_state.delete_file_id
                                st.rerun()
                            else:
                                st.error(message)
                    
                    with col2:
                        if st.button("❌ Cancel", key=f"cancel_delete_{file_id}"):
                            del st.session_state.delete_file_id
                            st.rerun()

def show_file_detail_page():
    """Render the file detail page."""
    # Clear any chat state when entering file detail view
    if 'chat_file_id' in st.session_state:
        del st.session_state['chat_file_id']
    
    if 'viewing_file_id' not in st.session_state:
        st.warning("No file selected")
        st.session_state.page = 'browse'
        st.rerun()
        return
    
    try:
        file_data = db.get_excel_file(st.session_state.viewing_file_id)
        if not file_data:
            st.error("File not found")
            del st.session_state.viewing_file_id
            st.session_state.page = 'browse'
            st.rerun()
            return
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        if 'viewing_file_id' in st.session_state:
            del st.session_state.viewing_file_id
        st.session_state.page = 'browse'
        st.rerun()
        return
    
    # Create three columns for better layout
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        st.title(f"📄 {file_data['file_name']}")
    
    with col2:
        if st.button("💬 Chat with File", 
                   key="chat_with_file_btn",
                   use_container_width=True,
                   type="primary"):
            # Set the selected file and switch to chat page
            st.session_state.selected_file = file_data['id']
            st.session_state.original_filename = file_data['file_name']
            st.session_state.page = 'chat'
            # Clear any existing chat messages for this file
            chat_key = f"chat_{file_data['id']}"
            if 'chat_messages' in st.session_state and chat_key in st.session_state.chat_messages:
                del st.session_state.chat_messages[chat_key]
            st.rerun()
    
    with col3:
        if st.button("← Back to Files", 
                   key="detail_back_btn",
                   use_container_width=True):
            # Clear viewing state when going back
            if 'viewing_file_id' in st.session_state:
                del st.session_state.viewing_file_id
            st.session_state.page = 'browse'
            st.rerun()
    
    st.caption(f"Uploaded: {file_data['uploaded_at']}")
    
    # Add tabs for sheets
    if file_data['tables']:
        sheet_names = list(file_data['tables'].keys())
        tabs = st.tabs([f"📑 {name}" for name in sheet_names])
        
        for idx, (sheet_name, tables) in enumerate(file_data['tables'].items()):
            with tabs[idx]:
                st.subheader(f"{sheet_name}")
                
                # Create tabs for tables within each sheet
                if tables:
                    table_tabs = st.tabs([f"📊 {name}" for name in tables.keys()])
                    
                    for tab_idx, (table_name, table_data) in enumerate(tables.items()):
                        with table_tabs[tab_idx]:
                            # Convert all data to strings to avoid Arrow type conflicts
                            df = pd.DataFrame(table_data)
                            
                            # Ensure all values are strings to prevent Arrow type conflicts
                            df = df.astype(str)
                            
                            # Clean the dataframe
                            df_cleaned = clean_dataframe(df)
                            
                            # Display the dataframe with improved styling
                            st.dataframe(
                                df_cleaned,
                                use_container_width=True,
                                height=min(400, (len(df_cleaned) + 1) * 35 + 3),
                                hide_index=True,
                            )
                            
                            # Back button at the bottom of the table with unique key
                            if st.button("← Back to Files", 
                                       key=f"back_btn_{file_data['id']}_{sheet_name}_{table_name}",
                                       use_container_width=True, 
                                       help="Return to file browser"):
                                if 'viewing_file_id' in st.session_state:
                                    del st.session_state['viewing_file_id']
                                st.session_state.page = 'browse'
                                st.session_state.selected_file_id = file_data['id']
                                st.rerun()
    else:
        st.warning("No sheets with tables found in this file.")

def get_chat_styles():
    """Return the CSS styles for the chat interface."""
    return """
    <style>
        /* Main chat interface styles */
        .chat-interface {
            display: flex;
            flex-direction: column;
            height: calc(100vh - 200px);
            max-height: 100vh;
            padding: 1rem;
            box-sizing: border-box;
        }
        
        /* Chat button styles */
        .chat-button {
            background: white;
            border: 1px solid #e0e0e0;
            border-radius: 50% !important;
            width: 42px;
            height: 42px;
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            font-size: 1.2rem;
            transition: all 0.2s ease;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
            padding: 0;
            margin: 0;
            flex-shrink: 0;
        }
        
        .chat-button:hover {
            transform: translateY(-1px);
        }
        
        .chat-button.clear {
            color: #ff4b4b;
            border-color: #ffebee;
        }
        
        .chat-button.clear:hover {
            background-color: #ffebee;
        }
        
        .chat-button.save {
            color: #4CAF50;
            border-color: #e8f5e9;
        }
        
        .chat-button.save:hover {
            background-color: #e8f5e9;
        }
        
        .chat-button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
            transform: none;
        }

        .messages-container {
            flex: 1;
            overflow-y: auto;
            padding: 1rem 0.5rem;
            margin-bottom: 1rem;
            scroll-behavior: smooth;
        }

        /* Chat input with buttons */
        .custom-input-area {
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background: white;
            padding: 1rem;
            box-shadow: 0 -2px 10px rgba(0, 0, 0, 0.05);
            z-index: 1000;
            display: flex;
            justify-content: center;
        }

        .chat-input-container {
            display: flex;
            align-items: center;
            gap: 8px;
            width: 100%;
            max-width: 1000px;
            margin: 0 auto;
            padding: 0 1rem;
        }

        .chat-input-wrapper {
            flex: 1;
            position: relative;
        }

        /* Style the Streamlit chat input */
        .stChatInputContainer {
            margin: 0 !important;
            width: 100% !important;
            max-width: 100% !important;
        }

        .stTextInput {
            margin: 0 !important;
        }

        .stTextInput > div {
            width: 100% !important;
        }

        .stTextInput input {
            width: 100% !important;
            padding: 12px 16px !important;
            border-radius: 24px !important;
            border: 1px solid #e0e0e0 !important;
            box-shadow: 0 2px 6px rgba(0, 0, 0, 0.05) !important;
            font-size: 0.95rem !important;
            transition: all 0.2s ease !important;
        }

        .stTextInput input:focus {
            border-color: #4CAF50 !important;
            box-shadow: 0 0 0 2px rgba(76, 175, 80, 0.2) !important;
            outline: none !important;
        }

        .chat-button {
            background: white;
            border: 1px solid #e0e0e0;
            cursor: pointer;
            font-size: 1.2rem;
            width: 42px;
            height: 42px;
            border-radius: 50% !important;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s ease;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        }

        .chat-button:hover {
            background-color: #f8f8f8;
            transform: translateY(-1px);
        }

        .chat-button.clear {
            color: #ff4b4b;
            border-color: #ffebee;
        }

        .chat-button.clear:hover {
            background-color: #ffebee;
        }

        .chat-button.save {
            color: #4CAF50;
            border-color: #e8f5e9;
        }

        .chat-button.save:hover {
            background-color: #e8f5e9;
        }

        /* Message styling */
        .stChatMessage {
            margin: 0.75rem 0;
            padding: 0.75rem 1rem;
            border-radius: 1rem;
            max-width: 85%;
            word-wrap: break-word;
            line-height: 1.5;
            font-size: 0.95rem;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05) !important;
            transition: all 0.2s ease;
        }

        [data-testid="stChatMessage"][data-message-author-role="user"] {
            margin-left: auto;
            background-color: #f0f7ff;
            border-radius: 1.25rem 1.25rem 0.25rem 1.25rem;
            margin-right: 0.5rem;
            border: 1px solid #e3f2fd;
        }

        [data-testid="stChatMessage"][data-message-author-role="assistant"] {
            margin-right: auto;
            background-color: #f8f9fa;
            border-radius: 1.25rem 1.25rem 1.25rem 0.25rem;
            margin-left: 0.5rem;
            border: 1px solid #f1f1f1;
        }

        /* Scrollbar styling */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }

        ::-webkit-scrollbar-track {
            background: #f8f9fa;
            border-radius: 4px;
        }

        ::-webkit-scrollbar-thumb {
            background: #c1c1c1;
            border-radius: 4px;
        }

        ::-webkit-scrollbar-thumb:hover {
            background: #a8a8a8;
        }

        /* Make sure the app has proper spacing */
        .stApp {
            padding-bottom: 100px !important;
        }

        /* Toast notification */
        .toast-notification {
            position: fixed;
            bottom: 20px;
            right: 20px;
            padding: 12px 20px;
            border-radius: 4px;
            color: white;
            font-size: 14px;
            z-index: 10000;
            opacity: 0;
            transform: translateY(20px);
            transition: opacity 0.3s, transform 0.3s;
        }
        .toast-notification.show {
            opacity: 1;
            transform: translateY(0);
        }
        .toast-notification.success {
            background-color: #4CAF50;
        }
        .toast-notification.error {
            background-color: #f44336;
        }
    </style>
    """

def get_chat_script():
    """Return the JavaScript for the chat interface."""
    return """
    <script>
    // Global variable to track button states
    let isProcessing = false;
    
    // Function to handle button clicks
    function handleButtonClick(type) {
        if (isProcessing) return false;
        
        // Show loading state on the button
        const button = document.getElementById(`${type}ChatBtn`);
        if (button) {
            isProcessing = true;
            const originalHtml = button.innerHTML;
            const originalTitle = button.title;
            
            // Update button state
            button.innerHTML = type === 'clear' ? '🗑️' : '💾';
            button.title = type === 'clear' ? 'Clearing...' : 'Saving...';
            button.disabled = true;
            
            try {
                // Create a hidden form to submit the action
                const form = document.createElement('form');
                form.method = 'GET';  // Use GET to update query params
                form.style.display = 'none';
                
                // Add the action parameter to the URL
                const actionInput = document.createElement('input');
                actionInput.type = 'hidden';
                actionInput.name = `${type}_chat_clicked`;
                actionInput.value = 'true';
                
                // Add any existing query parameters
                const urlParams = new URLSearchParams(window.location.search);
                urlParams.forEach((value, key) => {
                    if (key !== `${type}_chat_clicked`) {
                        const input = document.createElement('input');
                        input.type = 'hidden';
                        input.name = key;
                        input.value = value;
                        form.appendChild(input);
                    }
                });
                
                form.appendChild(actionInput);
                document.body.appendChild(form);
                
                // Submit the form
                form.submit();
                
                // Show toast for save action
                if (type === 'save') {
                    showToast('Saving chat...', 'info');
                }
                
            } catch (error) {
                console.error('Error submitting form:', error);
                showToast('An error occurred. Please try again.', 'error');
                
                // Reset button state on error
                button.innerHTML = originalHtml;
                button.title = originalTitle;
                button.disabled = false;
                isProcessing = false;
            }
            
            return false;
        }
        return false;
    }
    
    // Function to show toast notifications
    function showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `toast-notification ${type}`;
        toast.textContent = message;
        document.body.appendChild(toast);
        
        // Trigger reflow to ensure styles are applied before showing
        void toast.offsetWidth;
        
        // Show toast
        toast.classList.add('show');
        
        // Auto-hide after 3 seconds
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => {
                toast.remove();
            }, 300);
        }, 3000);
    }
    
    // Function to initialize the chat interface
    function initializeChat() {
        // Set up button event listeners
        const clearBtn = document.getElementById('clearChatBtn');
        const saveBtn = document.getElementById('saveChatBtn');
        
        if (clearBtn) clearBtn.onclick = (e) => {
            e.preventDefault();
            handleButtonClick('clear');
            return false;
        };
        
        if (saveBtn) saveBtn.onclick = (e) => {
            e.preventDefault();
            handleButtonClick('save');
            return false;
        };
        
        // Auto-scroll to bottom of messages
        const messagesContainer = document.querySelector('.stChatMessageContainer') || 
                                document.querySelector('.messages-container');
        if (messagesContainer) {
            messagesContainer.scrollTop = messagesContainer.scrollHeight;
        }
        
        // Check for any pending actions in session storage
        if (sessionStorage.getItem('chat_clear_clicked') === 'true') {
            sessionStorage.removeItem('chat_clear_clicked');
            handleButtonClick('clear');
        }
        
        if (sessionStorage.getItem('chat_save_clicked') === 'true') {
            sessionStorage.removeItem('chat_save_clicked');
            handleButtonClick('save');
        }
    }
    
    // Initialize when the page loads and on Streamlit events
    document.addEventListener('DOMContentLoaded', initializeChat);
    document.addEventListener('st:render', initializeChat);
    
    // Also run after a short delay to ensure everything is loaded
    setTimeout(initializeChat, 500);
    </script>
    """

def get_chat_html():
    """Return the HTML structure for the chat interface."""
    return """
    <div class="chat-interface">
        <div class="messages-container" id="messagesContainer">
            <!-- Messages will be inserted here by Streamlit -->
        </div>
    </div>
    
    <div class="custom-input-area">
        <div class="chat-input-container">
            <button class="chat-button clear" id="clearChatBtn" title="Clear chat">🗑️</button>
            <div class="chat-input-wrapper">
                <!-- Streamlit chat input will be inserted here -->
                <div id="streamlitChatInput"></div>
            </div>
            <button class="chat-button save" id="saveChatBtn" title="Save chat">💾</button>
        </div>
    </div>
    """

def show_chat_page():
    """Render the chat interface for the selected file with all sheets."""
    st.title("💬 Chat with Excel")
    
    # Check if a file is selected
    if 'selected_file' not in st.session_state or not st.session_state.selected_file:
        st.warning("Please select a file from the Browse Files page first.")
        if st.button("Go to Browse Files"):
            st.session_state.page = "📋 Browse Files"
            if 'viewing_file_id' in st.session_state:
                del st.session_state.viewing_file_id
            st.rerun()
        return
    
    # Get file data
    file_data = db.get_excel_file(st.session_state.selected_file)
    if not file_data:
        st.error("File not found. Please select another file.")
        if st.button("Back to Files"):
            st.session_state.page = "📋 Browse Files"
            st.rerun()
        return
    
    # Load all tables from the Excel file
    try:
        all_tables = extract_all_tables(file_data['file_path'])
    except Exception as e:
        st.error(f"Error loading Excel file: {str(e)}")
        return
    
    # Initialize session state for sheet and table selection
    if 'selected_sheet' not in st.session_state:
        st.session_state.selected_sheet = list(all_tables.keys())[0] if all_tables else None
    if 'selected_table' not in st.session_state:
        st.session_state.selected_table = None
    
    # Sidebar: Sheet and table selection
    st.sidebar.subheader("📄 File Info")
    st.sidebar.write(f"**File:** {file_data.get('file_name', 'N/A')}")
    st.sidebar.write(f"**Uploaded:** {file_data.get('uploaded_at', 'N/A')}")
    
    if all_tables:
        st.sidebar.markdown("---")
        st.sidebar.subheader("Select Data for Analysis")
        selected_sheet = st.sidebar.selectbox(
            "Select Sheet",
            options=list(all_tables.keys()),
            index=list(all_tables.keys()).index(st.session_state.selected_sheet) if st.session_state.selected_sheet in all_tables else 0,
            key="sheet_selector"
        )
        st.session_state.selected_sheet = selected_sheet
        
        table_names = list(all_tables[selected_sheet].keys())
        if table_names:
            selected_table = st.sidebar.selectbox(
                "Select Table",
                options=table_names,
                index=table_names.index(st.session_state.selected_table) if st.session_state.selected_table in table_names else 0,
                key="table_selector"
            )
            st.session_state.selected_table = selected_table
        else:
            st.sidebar.warning("No tables found in this sheet.")
            st.session_state.selected_table = None
            return
    else:
        st.error("No tables found in the Excel file.")
        return
    
    # Convert selected table to DataFrame
    try:
        df = pd.DataFrame(all_tables[selected_sheet][selected_table])
        df = clean_dataframe(df)  # Assuming clean_dataframe is defined
    except Exception as e:
        st.error(f"Error converting table to DataFrame: {str(e)}")
        return
    
    # Initialize chat messages
    if 'chat_messages' not in st.session_state:
        st.session_state.chat_messages = {}
    
    chat_key = f"chat_{st.session_state.selected_file}_{selected_sheet}_{selected_table}"
    
    # Initialize chat if not exists
    if chat_key not in st.session_state.chat_messages:
        st.session_state.chat_messages[chat_key] = [
            {"role": "assistant", "content": f"Hello! I can help you analyze the table **{selected_table}** in sheet **{selected_sheet}** from **{file_data.get('file_name', '')}**. Ask me anything about the data or request a visualization.", "fig": None}
        ]
    
    # Display chat messages
    with st.container():
        if chat_key in st.session_state.chat_messages:
            for idx, message in enumerate(st.session_state.chat_messages[chat_key]):
                if not message.get("content", "").strip() or message.get("role") == "system":
                    continue
                
                is_processing = (
                    message == st.session_state.chat_messages[chat_key][-1] and 
                    message["role"] == "assistant" and 
                    message.get("status") == "processing"
                )
                
                with st.chat_message(
                    message["role"], 
                    avatar="🤖" if message["role"] == "assistant" else "👤"
                ):
                    if is_processing:
                        loading_html = """
                        <div style='display: flex; align-items: center; gap: 10px; padding: 10px; background: #f8f9fa; border-radius: 10px; margin: 5px 0;'>
                            <div class='loader' style='font-size: 24px;'>🤖</div>
                            <div>Analyzing your data...</div>
                        </div>
                        <style>
                            @keyframes bounce {
                                0%, 100% { transform: translateY(0); }
                                50% { transform: translateY(-5px); }
                            }
                            .loader {
                                animation: bounce 1s infinite;
                                display: inline-block;
                            }
                        </style>
                        """
                        st.markdown(loading_html, unsafe_allow_html=True)
                        time.sleep(0.1)
                    
                    if message["content"] and not is_processing:
                        st.markdown(message["content"], unsafe_allow_html=True)
                    
                    # Render Plotly chart with unique key
                    if message.get("fig") and not is_processing:
                        st.markdown("### Chart")
                        st.plotly_chart(
                            message["fig"],
                            use_container_width=True,
                            key=f"plotly_chart_{chat_key}_{message['created_at']}_{idx}"
                        )
                    
                    if 'created_at' in message and not is_processing:
                        st.caption(f"{message['created_at']}")
                    if not is_processing:
                        st.markdown("<div style='margin: 0.5rem 0;'></div>", unsafe_allow_html=True)
    
    # Single chat input
    prompt = st.chat_input("Ask me anything about your Excel data...", key=f"{chat_key}_chat_input")
    
    # Process new message
    if prompt and (len(st.session_state.chat_messages[chat_key]) == 0 or 
                  st.session_state.chat_messages[chat_key][-1].get("content") != prompt):
        try:
            print(f"Processing new message: {prompt}")
            user_message = {
                "role": "user", 
                "content": prompt,
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "fig": None
            }
            st.session_state.chat_messages[chat_key].append(user_message)
            
            processing_msg = {
                "role": "assistant",
                "content": "",
                "status": "processing",
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "fig": None
            }
            st.session_state.chat_messages[chat_key].append(processing_msg)
            
            st.session_state.pending_prompt = prompt
            st.session_state.processing_started = True
            st.rerun()
            
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            st.error(f"Error processing your message: {str(e)}")
    
    # Handle AI response
    if ('pending_prompt' in st.session_state and 
        st.session_state.pending_prompt and 
        st.session_state.get('processing_started', False)):
        
        prompt = st.session_state.pending_prompt
        print(f"Processing AI response for prompt: {prompt}")
        st.session_state.processing_started = False
        
        try:
            # Generate and render Plotly chart
            graph_rendered, fig = generate_and_render_graph(df, prompt, get_openai_response)
            
            # Prepare analysis data for text response
            analysis_data = [{
                'sheet': selected_sheet,
                'table': selected_table,
                'columns': list(df.columns),
                'data': df.to_dict('records'),
                'sample_data': df.head(5).to_dict('records'),
                'total_rows': len(df),
                'column_types': {col: str(df[col].dtype) for col in df.columns}
            }]
            
            if not analysis_data:
                raise ValueError("No valid data available for analysis.")
            
            # Get AI text response
            response = analyze_table(analysis_data, prompt)
            print("Received response from OpenAI API")
            
            if not response or not response.strip():
                response = "I'm sorry, but I couldn't generate a response. Please try again with a different question."
            elif not isinstance(response, str):
                response = str(response)
            
            # Remove processing message
            if chat_key in st.session_state.chat_messages:
                st.session_state.chat_messages[chat_key] = [
                    msg for msg in st.session_state.chat_messages[chat_key] 
                    if msg.get('status') != 'processing'
                ]
                
                # Add assistant's response with chart (if rendered)
                if response and response.strip():
                    assistant_message = {
                        "role": "assistant", 
                        "content": response,
                        "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "fig": fig if graph_rendered else None
                    }
                    st.session_state.chat_messages[chat_key].append(assistant_message)
                
                # Save graph metadata if chart was rendered
                if graph_rendered:
                    graph_info = detect_graph_request(prompt, df.columns.tolist(), get_openai_response)
                    if graph_info:
                        try:
                            y_cols = graph_info.get("y_col")
                            y_col = y_cols[0] if isinstance(y_cols, list) else y_cols
                            db.save_graph_metadata(
                                session=db.init_db(),
                                file_id=st.session_state.selected_file,
                                query=prompt,
                                chart_type=graph_info.get("chart_type", ""),
                                x_col=graph_info.get("x_col", ""),
                                y_col=y_col
                            )
                        except Exception as e:
                            print(f"Error saving graph metadata: {str(e)}")
                
                # Save chat history
                try:
                    db.save_chat_history(
                        file_id=st.session_state.selected_file,
                        messages=st.session_state.chat_messages[chat_key],
                        sheet_name=f"{selected_sheet}_{selected_table}"
                    )
                except Exception as e:
                    print(f"Error saving chat history: {str(e)}")
                    st.error(f"Error saving chat history: {str(e)}")
            
            # Clear pending state
            if 'pending_prompt' in st.session_state:
                del st.session_state.pending_prompt
            if 'processing_started' in st.session_state:
                del st.session_state.processing_started
            
            st.rerun()
            
        except Exception as e:
            error_msg = str(e)
            print(f"Error in chat processing: {error_msg}")
            
            if (chat_key in st.session_state.chat_messages and 
                st.session_state.chat_messages[chat_key] and 
                st.session_state.chat_messages[chat_key][-1].get("status") == "processing"):
                st.session_state.chat_messages[chat_key].pop()
            
            error_message = f"I'm sorry, but I encountered an error: {error_msg}"
            st.session_state.chat_messages[chat_key].append({
                "role": "assistant", 
                "content": error_message,
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "fig": None
            })
            
            if 'pending_prompt' in st.session_state:
                del st.session_state.pending_prompt
            if 'processing_started' in st.session_state:
                del st.session_state.processing_started
            
            st.rerun()
    
    # Chat actions in sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("Chat Actions")
    
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("🗑️ Clear Chat", use_container_width=True):
            st.session_state.chat_messages[chat_key] = [
                {"role": "assistant", "content": f"Chat cleared. How can I help you analyze **{selected_table}** in **{selected_sheet}** from **{file_data.get('file_name', 'this file')}**?", "fig": None}
            ]
            st.rerun()
    
    with col2:
        if st.button("💾 Save Chat", use_container_width=True):
            try:
                for msg in st.session_state.chat_messages[chat_key]:
                    if 'created_at' not in msg:
                        msg['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                db.save_chat_history(
                    file_id=st.session_state.selected_file,
                    messages=st.session_state.chat_messages[chat_key],
                    sheet_name=f"{selected_sheet}_{selected_table}"
                )
                st.sidebar.success("Chat saved!")
            except Exception as e:
                st.sidebar.error(f"Failed to save: {str(e)}")
    
    # Add model information
    st.sidebar.markdown("---")
    st.sidebar.markdown("### AI Model")
    st.sidebar.info("Using: GPT 3.5 Turbo")
    st.sidebar.caption("Powered by OpenAI API")
    
    # Add helpful tips
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 💡 Tips")
    st.sidebar.markdown("• Ask questions about your data in natural language")
    st.sidebar.markdown("• Request analysis, summaries, or explanations")
    st.sidebar.markdown("• Ask to find patterns or insights in your data")
    st.sidebar.markdown("• Request visualizations like 'Show a line chart of [column] by [column]'")
    
    # Handle button clicks
    if st.session_state.get('clear_chat_clicked', False):
        st.session_state.clear_chat_clicked = False
        try:
            total_tables = sum(len(tables) for tables in file_data.get('tables', {}).values())
            initial_message = (
                f"I can help you analyze the table **{selected_table}** in sheet **{selected_sheet}** "
                f"from **{file_data.get('file_name', 'your file')}**. "
                f"This file contains {len(file_data.get('tables', {}))} sheet(s) with {total_tables} tables. "
                "Ask me anything about the data or request a visualization."
            )
            st.session_state.chat_messages[chat_key] = [
                {"role": "assistant", "content": initial_message, "fig": None}
            ]
            for msg in st.session_state.chat_messages[chat_key]:
                if 'created_at' not in msg:
                    msg['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            db.save_chat_history(
                file_id=st.session_state.selected_file,
                messages=st.session_state.chat_messages[chat_key],
                sheet_name=f"{selected_sheet}_{selected_table}"
            )
            st.rerun()
        except Exception as e:
            st.error(f"❌ Failed to clear chat: {str(e)}")
    
    if st.session_state.get('save_chat_clicked', False):
        st.session_state.save_chat_clicked = False
        try:
            for msg in st.session_state.chat_messages[chat_key]:
                if 'created_at' not in msg:
                    msg['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            db.save_chat_history(
                file_id=st.session_state.selected_file,
                messages=st.session_state.chat_messages[chat_key],
                sheet_name=f"{selected_sheet}_{selected_table}"
            )
            st.sidebar.success("Chat saved successfully!")
        except Exception as e:
            st.error(f"❌ Failed to save: {str(e)}")
    
    # Add external JavaScript for chat interface enhancements
    try:
        with open(os.path.join('templates', 'chat.js'), 'r', encoding='utf-8') as f:
            chat_js = f.read()
        st.markdown(f'<script>{chat_js}</script>', unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error loading chat script: {str(e)}")
    
    # Back button
    if st.button("← Back to File", key="chat_back_btn", use_container_width=True):
        if 'selected_file' in st.session_state:
            st.session_state.viewing_file_id = st.session_state.selected_file
            del st.session_state.selected_file
        st.session_state.page = 'file_detail'
        st.rerun()
# Navigation
st.sidebar.title("📊 Excel ChatBot")

# Navigation options
nav_options = ["📤 Upload Excel", "📋 Browse Files", "💬 Chat with Sheets"]

# Get current page index
page_index = 0  # Default to Upload Excel
if st.session_state.page == 'browse' or st.session_state.page == 'file_detail':
    page_index = 1
elif st.session_state.page == 'chat':
    page_index = 2

# Navigation radio
page = st.sidebar.radio(
    "Navigation",
    nav_options,
    index=page_index,
    key='nav_radio'
)

# Page routing
if page == "📤 Upload Excel":
    st.session_state.page = 'upload'
    show_upload_page()
elif page == "📋 Browse Files":
    st.session_state.page = 'browse'
    show_browse_page()
elif page == "💬 Chat with Sheets":
    # Only show chat page if a file is selected
    if 'selected_file' in st.session_state and st.session_state.selected_file:
        st.session_state.page = 'chat'
        show_chat_page()
    else:
        st.warning("Please select a file from the Browse Files page first.")
        st.session_state.page = 'browse'
        show_browse_page()

# Clear upload success state when navigating away from upload page
if 'last_page' in st.session_state and st.session_state.last_page == 'upload' and st.session_state.page != 'upload':
    st.session_state.upload_success = False

# Store current page for next render
st.session_state.last_page = st.session_state.page

# Handle file detail page (not in sidebar)
if st.session_state.page == 'file_detail':
    show_file_detail_page()