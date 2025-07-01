from sqlalchemy import Column, String, Text, DateTime, Integer, JSON, ForeignKey, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional

Base = declarative_base()

class MessageRole(PyEnum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"

class ExcelFile(Base):
    __tablename__ = 'excel_files'
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String(255), nullable=False)
    file_path = Column(String(512), nullable=False)
    file_hash = Column(String(64), nullable=False, unique=True)
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    tables = relationship("ExcelTable", back_populates="excel_file", cascade="all, delete-orphan")
    chat_messages = relationship("ChatHistory", back_populates="excel_file", cascade="all, delete-orphan")

class ExcelTable(Base):
    __tablename__ = 'excel_tables'
    id = Column(Integer, primary_key=True, autoincrement=True)
    excel_file_id = Column(Integer, ForeignKey('excel_files.id', ondelete='CASCADE'))
    sheet_name = Column(String(255), nullable=False)
    table_name = Column(String(255), nullable=False)
    data = Column(JSON, nullable=False)
    excel_file = relationship("ExcelFile", back_populates="tables")

class ChatHistory(Base):
    __tablename__ = 'chat_history'
    id = Column(Integer, primary_key=True, autoincrement=True)
    excel_file_id = Column(Integer, ForeignKey('excel_files.id', ondelete='CASCADE'), nullable=False)
    sheet_name = Column(String(255), nullable=False)  # Store which sheet this chat is for
    role = Column(Enum(MessageRole), nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    excel_file = relationship("ExcelFile", back_populates="chat_messages")
    
    def to_dict(self) -> dict:
        """Convert the chat message to a dictionary."""
        return {
            "role": self.role.value,
            "content": self.content,
            "created_at": self.created_at.isoformat()
        }

class GraphHistory(Base):
    __tablename__ = 'graph_history'
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('excel_files.id'))
    query = Column(String)
    chart_type = Column(String)
    x_col = Column(String)
    y_col = Column(String)
    created_at = Column(DateTime, default=datetime.now)