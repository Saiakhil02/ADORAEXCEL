# AskAdora - Document Analyzer with AI Chat

A powerful Streamlit-based application that enables users to upload and analyze various document types using natural language. The application leverages OpenAI's language models for intelligent document processing and FAISS for efficient semantic search capabilities.

## Features

- **Multi-format Support**: Upload and analyze PDF, DOCX, XLSX, XLS, and TXT files
- **Intelligent Document Processing**: Automatic text extraction and table detection
- **Natural Language Querying**: Ask questions about your documents in plain English
- **Interactive Chat Interface**: Engage in conversations about your documents
- **Vector Database**: FAISS-based vector store for efficient semantic search
- **Persistent Storage**: SQLite/PostgreSQL database for document and chat history
- **Document Management**: View, search, and manage uploaded documents
- **Responsive Design**: Clean, modern UI that works on different screen sizes

## Prerequisites

- Python 3.8+
- OpenAI API key (for text processing and embeddings)
- (Optional) PostgreSQL (for production) or SQLite (default, for development)
- Node.js and npm (for optional frontend development)

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd AskAdora
   ```

2. **Set up a virtual environment** (recommended):
   ```bash
   # Windows
   python -m venv venv
   venv\Scripts\activate
   
   # macOS/Linux
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**:
   Create a `.env` file in the project root with the following variables:
   ```env
   # Required
   OPENAI_API_KEY=your_openai_api_key_here
   
   # Optional - Only needed if using PostgreSQL
   # DATABASE_URL=postgresql://username:password@localhost:5432/askadora
   
   # Optional - For production
   # DEBUG=False
   # SECRET_KEY=your_secret_key_here
   ```

5. **Initialize the database**:
   The application will automatically create and initialize a SQLite database (`app.db`) on first run.
   For PostgreSQL, create the database first and update the `DATABASE_URL` in your `.env` file.

## Running the Application

1. **Start the application**:
   ```bash
   streamlit run app.py
   ```

2. **Access the web interface**:
   Open your browser and navigate to `http://localhost:8501`

3. **Start analyzing documents**:
   - Use the sidebar to navigate between "Upload Documents" and "Chat with Documents"
   - Upload your documents (PDF, DOCX, XLSX, XLS, TXT)
   - Ask questions about your documents using natural language

## Usage Examples

1. **Uploading Documents**:
   - Click "Upload Documents" in the sidebar
   - Drag and drop or select files to upload
   - The system will process and index your documents automatically
## Project Structure

```
.
├── app.py                # Main Streamlit application
├── chatbot.py            # Chatbot implementation
├── vector_store.py       # FAISS vector store management
├── file_processor.py     # Document processing utilities
├── excel_processor.py    # Excel-specific processing
├── database.py           # Database models and session management
├── visualization_utils.py# Data visualization helpers
├── requirements.txt      # Python dependencies
├── .env                 # Environment variables (create from .env.example)
├── uploads/             # Directory for uploaded files
├── faiss_index/         # FAISS vector store indices
└── static/              # Static files (CSS, JS, images)
```

## Configuration

### Environment Variables

Create a `.env` file in the root directory with the following variables:

```env
# Required
OPENAI_API_KEY=your_openai_api_key_here

# Optional - Database configuration
# DATABASE_URL=sqlite:///app.db  # Default SQLite
# DATABASE_URL=postgresql://user:password@localhost:5432/askadora

# Optional - Application settings
# DEBUG=True
# SECRET_KEY=your_secret_key_here
# MAX_FILE_SIZE_MB=200
```

## How It Works

1. **Document Processing**:
   - Documents are processed based on their type (PDF, DOCX, XLSX, XLS, TXT)
   - Text is extracted and split into manageable chunks
   - Chunks are converted to vector embeddings using OpenAI's API
   - Embeddings are stored in a FAISS vector database for efficient similarity search

2. **Query Processing**:
   - User queries are converted to vector embeddings
   - The system performs a similarity search to find relevant document chunks
   - Relevant context is passed to the OpenAI model to generate a response
   - Responses are formatted and displayed in the chat interface

## Customization

### Adding Support for New File Types

1. Create a new processing function in `file_processor.py`
2. Update the `_process_file_content` function in `app.py` to handle the new file type
3. Add the file extension to the supported types in the file uploader

### Modifying the Chat Interface

The chat interface is defined in the `_display_chat_interface` function in `app.py`. You can customize:
- UI components and layout
- Message formatting
- System prompts and instructions
- Response generation parameters

## Troubleshooting

### Common Issues

1. **File Upload Fails**:
   - Check file size (default limit: 200MB)
   - Verify the file type is supported
   - Ensure the uploads directory has write permissions

2. **Database Connection Issues**:
   - Verify the database server is running (if using PostgreSQL)
   - Check the `DATABASE_URL` in your `.env` file
   - Ensure the database user has the necessary permissions

3. **OpenAI API Errors**:
   - Verify your API key is correct and has sufficient credits
   - Check the OpenAI status page for any service disruptions
   - Consider implementing rate limiting if you hit API quotas

## Deployment

### Local Development

```bash
streamlit run app.py
```

### Production Deployment

For production deployment, consider using:
- **Docker** for containerization
- **Gunicorn** or **Uvicorn** as a production server
- **Nginx** as a reverse proxy
- **PostgreSQL** for the database
- **Redis** for caching (optional)

Example Docker configuration:

```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Streamlit](https://streamlit.io/) for the web framework
- [OpenAI](https://openai.com/) for the language models
- [FAISS](https://github.com/facebookresearch/faiss) for efficient similarity search
- [LangChain](https://github.com/langchain-ai/langchain) for LLM orchestration
- [SQLAlchemy](https://www.sqlalchemy.org/) for database ORM
