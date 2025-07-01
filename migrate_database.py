"""
Database migration script to add the chat_history table.
Run this script to update your database schema.
"""
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, DateTime, ForeignKey, Enum
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'Sweethome%40143')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'Adora_AI')
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def create_chat_history_table():
    """Create the chat_history table if it doesn't exist."""
    try:
        # Connect to the database
        engine = create_engine(DATABASE_URL)
        metadata = MetaData()
        
        # Check if the table already exists
        from sqlalchemy import inspect
        inspector = inspect(engine)
        if not inspector.has_table('chat_history'):
            # Create the table
            Table(
                'chat_history',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('excel_file_id', Integer, ForeignKey('excel_files.id', ondelete='CASCADE'), nullable=False),
                Column('sheet_name', String(255), nullable=False),
                Column('role', Enum('user', 'assistant', 'system', name='message_role'), nullable=False),
                Column('content', Text, nullable=False),
                Column('created_at', DateTime, server_default='now()')
            )
            
            # Create the table
            metadata.create_all(engine)
            print("✅ Created 'chat_history' table")
        else:
            print("ℹ️ 'chat_history' table already exists")
            
        # Check if the foreign key constraint exists
        inspector = engine.dialect.inspector(engine)
        fk_exists = False
        for fk in inspector.get_foreign_keys('chat_history'):
            if fk['referred_table'] == 'excel_files' and 'excel_file_id' in fk['constrained_columns']:
                fk_exists = True
                break
                
        if not fk_exists:
            # Add foreign key constraint
            with engine.connect() as conn:
                conn.execute("""
                    ALTER TABLE chat_history 
                    ADD CONSTRAINT fk_chat_history_excel_files 
                    FOREIGN KEY (excel_file_id) 
                    REFERENCES excel_files(id) 
                    ON DELETE CASCADE
                """)
                print("✅ Added foreign key constraint to 'chat_history' table")
        else:
            print("ℹ️ Foreign key constraint already exists on 'chat_history' table")
            
    except Exception as e:
        print(f"❌ Error creating 'chat_history' table: {str(e)}")
        raise

if __name__ == "__main__":
    print("Starting database migration...")
    create_chat_history_table()
    print("✅ Database migration completed")
