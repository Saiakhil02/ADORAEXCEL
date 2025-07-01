from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
import os
from dotenv import load_dotenv
from models import Base
from typing import Generator, Optional, Dict, Any, List, Tuple
from models import Base, ExcelFile, ExcelTable, ChatHistory, MessageRole, GraphHistory


# Load environment variables
load_dotenv()

# Use SQLite (default fallback)
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///app.db")

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db_session() -> Generator[scoped_session, None, None]:
    session = scoped_session(SessionLocal)
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def init_db():
    Base.metadata.create_all(bind=engine)


def is_duplicate_file(file_hash: str, file_name: str = None) -> Tuple[bool, str]:
    """
    Check if a file with the given hash or filename already exists in the database.
    
    Args:
        file_hash: The hash of the file to check
        file_name: Optional original filename to check
        
    Returns:
        Tuple of (is_duplicate, message) where message explains the reason
    """
    with get_db_session() as db_session:
        # First check by hash (exact duplicate)
        existing_by_hash = db_session.query(ExcelFile).filter(ExcelFile.file_hash == file_hash).first()
        if existing_by_hash:
            return True, "This exact file has already been uploaded."
            
        # Then check by filename if provided
        if file_name:
            existing_by_name = db_session.query(ExcelFile).filter(
                ExcelFile.file_name.ilike(f"%{os.path.basename(file_name)}")
            ).first()
            
            if existing_by_name:
                return True, f"A file with the name '{os.path.basename(file_name)}' already exists in the database. Please use a different filename or delete the existing one first."
                
        return False, ""

def save_excel_file(file_name: str, file_path: str, file_hash: str, tables_data: Dict[str, Any]) -> Optional[int]:
    """Save Excel file and its tables to the database."""
    with get_db_session() as db_session:
        try:
            # Create ExcelFile record
            excel_file = ExcelFile(
                file_name=file_name,
                file_path=file_path,
                file_hash=file_hash
            )
            db_session.add(excel_file)
            db_session.flush()
            
            # Prepare and add ExcelTable records
            for sheet_name, tables in tables_data.items():
                for table_name, table_data in tables.items():
                    excel_table = ExcelTable(
                        excel_file_id=excel_file.id,
                        sheet_name=sheet_name,
                        table_name=table_name,
                        data=table_data  # Data will be serialized by SQLAlchemy
                    )
                    db_session.add(excel_table)
            
            db_session.commit()
            return excel_file.id
            
        except SQLAlchemyError as e:
            db_session.rollback()
            raise Exception(f"Failed to save Excel file to database: {str(e)}")

def get_excel_file(file_id: int) -> Optional[Dict[str, Any]]:
    """Retrieve an Excel file and its tables by ID."""
    with get_db_session() as db_session:
        file = db_session.query(ExcelFile).filter_by(id=file_id).first()
        if not file:
            return None
            
        tables_by_sheet = {}
        for table in file.tables:
            tables_by_sheet.setdefault(table.sheet_name, {})[table.table_name] = table.data
            
        return {
            'id': file.id,
            'file_name': file.file_name,
            'file_path': file.file_path,
            'uploaded_at': file.uploaded_at,  # Keep as datetime object
            'tables': tables_by_sheet
        }

def list_excel_files() -> List[Dict[str, Any]]:
    """List all Excel files in the database."""
    with get_db_session() as db_session:
        files = db_session.query(ExcelFile).order_by(ExcelFile.uploaded_at.desc()).all()
        return [{
            'id': f.id,
            'file_name': f.file_name,
            'uploaded_at': f.uploaded_at,  # Keep as datetime object
            'tables_count': len(f.tables)
        } for f in files]

def duplicate_excel_file(file_id: int) -> tuple[bool, str, int]:
    """
    Create a duplicate of an existing Excel file with a new sequential number.
    
    Args:
        file_id: ID of the file to duplicate
        
    Returns:
        tuple: (success: bool, message: str, new_file_id: int)
    """
    with get_db_session() as db_session:
        try:
            # Get the file to duplicate
            original_file = db_session.query(ExcelFile).filter(ExcelFile.id == file_id).first()
            if not original_file:
                return False, "Original file not found", -1
            
            # Get the next available file number
            max_file_num = db_session.query(
                db.func.max(
                    db.cast(
                        db.func.regexp_replace(ExcelFile.file_name, r'^file(\d+).*', '\\1'),
                        db.Integer
                    )
                )
            ).scalar() or 0
            new_file_num = max_file_num + 1
            
            # Create new file name with next sequential number
            file_name_parts = os.path.splitext(original_file.file_name)
            new_file_name = f"file{new_file_num}{file_name_parts[1] if len(file_name_parts) > 1 else ''}"
            
            # Create new file path
            original_dir = os.path.dirname(original_file.file_path)
            new_file_path = os.path.join(original_dir, new_file_name)
            
            # Copy the file
            import shutil
            shutil.copy2(original_file.file_path, new_file_path)
            
            # Create new database record
            new_file = ExcelFile(
                file_name=new_file_name,
                file_path=new_file_path,
                file_hash=calculate_file_hash(open(new_file_path, 'rb').read())
            )
            db_session.add(new_file)
            db_session.flush()  # Get the new file ID
            
            # Duplicate all tables and their data
            original_tables = db_session.query(ExcelTable).filter(
                ExcelTable.excel_file_id == file_id
            ).all()
            
            for table in original_tables:
                new_table = ExcelTable(
                    excel_file_id=new_file.id,
                    sheet_name=table.sheet_name,
                    table_name=table.table_name,
                    data=table.data  # This should be a deep copy if it's a mutable object
                )
                db_session.add(new_table)
            
            db_session.commit()
            return True, f"File duplicated successfully as {new_file_name}", new_file.id
            
        except Exception as e:
            db_session.rollback()
            import traceback
            traceback.print_exc()
            return False, f"Error duplicating file: {str(e)}", -1


def get_chat_history(file_id: int, sheet_name: str = None) -> list:
    """
    Retrieve chat history for a specific file and optional sheet.
    
    Args:
        file_id: ID of the file to get chat history for
        sheet_name: Optional name of the sheet to filter by
        
    Returns:
        list: List of chat messages in the format [{"role": "user"|"assistant", "content": str, "created_at": str}]
    """
    with get_db_session() as db_session:
        try:
            query = db_session.query(ChatHistory).filter(
                ChatHistory.excel_file_id == file_id
            )
            
            if sheet_name:
                query = query.filter(ChatHistory.sheet_name == sheet_name)
                
            messages = query.order_by(ChatHistory.created_at.asc()).all()
            
            # Convert SQLAlchemy objects to dictionaries
            return [
                {
                    'role': msg.role,
                    'content': msg.content,
                    'created_at': msg.created_at.isoformat(),
                    'sheet_name': msg.sheet_name
                }
                for msg in messages
            ]
            
        except Exception as e:
            print(f"Error getting chat history: {str(e)}")
            return []

def get_all_chat_history(file_id: int) -> list:
    """
    Retrieve all chat history for a specific file, across all sheets.
    
    Args:
        file_id: ID of the file to get chat history for
        
    Returns:
        list: List of chat messages with sheet information
    """
    with get_db_session() as db_session:
        try:
            messages = db_session.query(ChatHistory).filter(
                ChatHistory.excel_file_id == file_id
            ).order_by(
                ChatHistory.sheet_name,
                ChatHistory.created_at.asc()
            ).all()
            
            return [
                {
                    'id': msg.id,
                    'role': msg.role,
                    'content': msg.content,
                    'created_at': msg.created_at,
                    'sheet_name': msg.sheet_name or 'default'
                }
                for msg in messages
            ]
            
        except Exception as e:
            print(f"Error getting all chat history: {str(e)}")
            return []

def save_chat_history(file_id: int, messages: list, sheet_name: str = None) -> bool:
    """
    Save chat history for a specific file and sheet.
    
    Args:
        file_id: ID of the file to save chat history for
        messages: List of chat messages in the format [{"role": "user"|"assistant", "content": str}]
        sheet_name: Name of the sheet this chat is for (defaults to None for all sheets)
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not messages:
        return False
        
    with get_db_session() as db_session:
        try:
            # Get existing message hashes to avoid duplicates
            existing_hashes = set()
            existing_messages = db_session.query(ChatHistory).filter(
                ChatHistory.excel_file_id == file_id,
                ChatHistory.sheet_name == sheet_name
            ).all()
            
            # Create a set of existing message content hashes
            for msg in existing_messages:
                msg_hash = hash((msg.content, str(msg.role), str(msg.sheet_name)))
                existing_hashes.add(msg_hash)
            
            # Prepare new messages with proper enum values
            chat_messages = []
            for msg in messages:
                try:
                    # Skip if message is already in the database
                    msg_hash = hash((msg["content"], msg["role"], sheet_name))
                    if msg_hash in existing_hashes:
                        continue
                        
                    # Convert string role to MessageRole enum
                    if isinstance(msg["role"], str):
                        role_enum = MessageRole[msg["role"].upper()]
                    else:
                        role_enum = msg["role"]
                    
                    chat_messages.append({
                        "excel_file_id": file_id,
                        "sheet_name": sheet_name,
                        "role": role_enum,
                        "content": msg["content"],
                        "created_at": msg.get("created_at") or datetime.utcnow()
                    })
                except (KeyError, AttributeError) as e:
                    print(f"Warning: Invalid message format, skipping: {e}")
                    continue
            
            # Bulk insert only new messages
            if chat_messages:
                db_session.bulk_insert_mappings(ChatHistory, chat_messages)
                db_session.commit()
                return True
            return False
            
        except SQLAlchemyError as e:
            db_session.rollback()
            print(f"Error saving chat history: {str(e)}")
            return False
def save_graph_metadata(session, file_id, query, chart_type, x_col, y_col):
    graph = GraphHistory(
        file_id=file_id,
        query=query,
        chart_type=chart_type,
        x_col=x_col,
        y_col=y_col
    )
    session.add(graph)
    session.commit()

def delete_excel_file(file_id: int) -> tuple[bool, str]:
    """
    Delete an Excel file and its associated data from both database and file system.
    
    Args:
        file_id: ID of the file to delete
        
    Returns:
        tuple: (success: bool, message: str)
    """
    with get_db_session() as db_session:
        try:
            # Start a transaction
            file_to_delete = db_session.query(ExcelFile).filter(ExcelFile.id == file_id).first()
            if not file_to_delete:
                return False, "File not found"
                
            file_path = file_to_delete.file_path
            
            # Delete associated chat history first
            db_session.query(ChatHistory).filter(ChatHistory.excel_file_id == file_id).delete()
            
            # Delete associated tables next
            db_session.query(ExcelTable).filter(ExcelTable.excel_file_id == file_id).delete()
            
            # Delete the file record from database
            db_session.delete(file_to_delete)
            
            # If database deletion was successful, delete the actual file
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    
                    # Try to remove the directory if it's empty
                    directory = os.path.dirname(file_path)
                    if os.path.exists(directory) and not os.listdir(directory):
                        os.rmdir(directory)
                
                db_session.commit()
                return True, "File deleted successfully"
                
            except Exception as e:
                # If file operations fail, rollback the transaction
                db_session.rollback()
                import logging
                logging.error(f"Error during file operations: {str(e)}")
                return False, f"Error during file operations: {str(e)}"
                
        except SQLAlchemyError as e:
            db_session.rollback()
            return False, f"Database error: {str(e)}"
        except Exception as e:
            db_session.rollback()
            return False, f"Unexpected error: {str(e)}"