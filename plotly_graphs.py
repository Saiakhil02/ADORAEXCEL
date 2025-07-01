# plotly_graphs.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Tuple, Optional, Dict, Any

def detect_graph_request(query: str, columns: list, get_openai_response) -> Optional[Dict[str, Any]]:
    """
    Detect if the query requests a graph and identify chart type and columns.
    Returns a JSON object with chart_type, x_col, and y_col (str or list).
    """
    try:
        system_message = {
            "role": "system",
            "content": """You are an AI that determines if a query requests a graph and identifies the chart type and relevant columns.
            Respond with a JSON object containing:
            - chart_type: The type of chart (e.g., 'bar', 'line', 'pie', 'scatter', 'box', 'area', 'waterfall', 'heatmap')
            - x_col: The column for the x-axis (must be in the provided columns)
            - y_col: The column(s) for the y-axis (single column or list for multi-line charts)
            If no graph is requested or columns are invalid, return null."""
        }
        user_message = {
            "role": "user",
            "content": f"""Does this query request a graph? If so, specify the chart type and relevant columns from {columns}.
            Query: {query}
            Return a JSON object with 'chart_type', 'x_col', and 'y_col' (can be a single column or list of columns)."""
        }
        response = get_openai_response([system_message, user_message])
        
        try:
            result = eval(response) if isinstance(response, str) else response
            if not result or not isinstance(result, dict):
                return None
            # Validate columns
            if result.get("x_col") not in columns:
                return None
            y_cols = result.get("y_col")
            if isinstance(y_cols, str) and y_cols not in columns:
                return None
            if isinstance(y_cols, list) and not all(col in columns for col in y_cols):
                return None
            return result
        except:
            return None
    except Exception as e:
        print(f"Error in detect_graph_request: {str(e)}")
        return None

def generate_plotly_chart(df: pd.DataFrame, chart_type: str, x_col: str, y_cols: str | list) -> Tuple[Optional[Any], Optional[str]]:
    """
    Generate a Plotly chart for the specified chart type and columns.
    Supports scatter, box, area, waterfall, heatmap, line, bar, and pie charts.
    Returns the Plotly figure and an error message (if any).
    """
    try:
        # Ensure y_cols is a list for consistency
        y_cols = [y_cols] if isinstance(y_cols, str) else y_cols
        
        # Validate inputs
        if x_col not in df.columns:
            return None, f"Invalid x-axis column: {x_col}"
        if not all(y_col in df.columns for y_col in y_cols):
            return None, f"Invalid y-axis column(s): {y_cols}"
        
        # Clean and prepare data
        df_clean = df[[x_col] + y_cols].dropna()
        if df_clean.empty:
            return None, "No valid data after cleaning"
        
        # Convert x_col to string for consistency
        df_clean[x_col] = df_clean[x_col].astype(str)
        
        # Generate chart based on chart type
        if chart_type.lower() == "pie":
            if len(y_cols) != 1:
                return None, "Pie chart requires exactly one y-axis column"
            y_col = y_cols[0]
            fig = px.pie(df_clean, names=x_col, values=y_col, title=f"{y_col} by {x_col}")
            fig.update_layout(
                template="plotly_white",
                font=dict(size=12),
                margin=dict(l=50, r=50, t=50, b=50)
            )
            return fig, None
        
        elif chart_type.lower() == "line":
            fig = px.line(df_clean, x=x_col, y=y_cols, title=f"{', '.join(y_cols)} by {x_col}", markers=True)
            fig.update_layout(
                template="plotly_white",
                xaxis_title=x_col,
                yaxis_title="Values",
                legend_title="Metrics",
                font=dict(size=12),
                margin=dict(l=50, r=50, t=50, b=50)
            )
            return fig, None
        
        elif chart_type.lower() == "bar":
            fig = px.bar(df_clean, x=x_col, y=y_cols, title=f"{', '.join(y_cols)} by {x_col}", barmode='group')
            fig.update_layout(
                template="plotly_white",
                xaxis_title=x_col,
                yaxis_title="Values",
                legend_title="Metrics",
                font=dict(size=12),
                margin=dict(l=50, r=50, t=50, b=50)
            )
            return fig, None
        
        elif chart_type.lower() == "scatter":
            fig = px.scatter(df_clean, x=x_col, y=y_cols, title=f"{', '.join(y_cols)} by {x_col}")
            fig.update_layout(
                template="plotly_white",
                xaxis_title=x_col,
                yaxis_title="Values",
                legend_title="Metrics",
                font=dict(size=12),
                margin=dict(l=50, r=50, t=50, b=50)
            )
            return fig, None
        
        elif chart_type.lower() == "box":
            fig = px.box(df_clean, x=x_col, y=y_cols, title=f"{', '.join(y_cols)} by {x_col}")
            fig.update_layout(
                template="plotly_white",
                xaxis_title=x_col,
                yaxis_title="Values",
                legend_title="Metrics",
                font=dict(size=12),
                margin=dict(l=50, r=50, t=50, b=50)
            )
            return fig, None
        
        elif chart_type.lower() == "area":
            fig = px.area(df_clean, x=x_col, y=y_cols, title=f"{', '.join(y_cols)} by {x_col}")
            fig.update_layout(
                template="plotly_white",
                xaxis_title=x_col,
                yaxis_title="Values",
                legend_title="Metrics",
                font=dict(size=12),
                margin=dict(l=50, r=50, t=50, b=50)
            )
            return fig, None
        
        elif chart_type.lower() == "waterfall":
            if len(y_cols) != 1:
                return None, "Waterfall chart requires exactly one y-axis column"
            y_col = y_cols[0]
            # Assume df_clean has a 'measure' column for waterfall (relative/absolute/total)
            # If not present, treat all as relative
            if 'measure' not in df_clean.columns:
                df_clean['measure'] = 'relative'
            fig = go.Figure(go.Waterfall(
                name=y_col,
                orientation="v",
                measure=df_clean['measure'],
                x=df_clean[x_col],
                y=df_clean[y_col],
                text=df_clean[y_col],
                textposition="auto",
                connector={"line": {"color": "rgb(63, 63, 63)"}},
            ))
            fig.update_layout(
                title=f"{y_col} Waterfall by {x_col}",
                template="plotly_white",
                xaxis_title=x_col,
                yaxis_title=y_col,
                font=dict(size=12),
                margin=dict(l=50, r=50, t=50, b=50)
            )
            return fig, None
        
        elif chart_type.lower() == "heatmap":
            # For heatmap, assume y_cols are numeric columns to correlate
            numeric_cols = [col for col in y_cols if pd.api.types.is_numeric_dtype(df_clean[col])]
            if not numeric_cols:
                return None, "Heatmap requires at least one numeric y-axis column"
            # Create correlation matrix
            corr_matrix = df_clean[numeric_cols].corr()
            fig = px.imshow(
                corr_matrix,
                labels=dict(x="Metrics", y="Metrics", color="Correlation"),
                title="Correlation Heatmap",
                color_continuous_scale="RdBu_r",
                zmin=-1,
                zmax=1
            )
            fig.update_layout(
                template="plotly_white",
                font=dict(size=12),
                margin=dict(l=50, r=50, t=50, b=50)
            )
            return fig, None
        
        else:
            return None, f"Unsupported chart type: {chart_type}"
    
    except Exception as e:
        return None, f"Error generating Plotly chart: {str(e)}"

def generate_and_render_graph(df: pd.DataFrame, query: str, get_openai_response) -> Tuple[bool, Optional[Any]]:
    """
    Generate and render a Plotly chart based on the query.
    Returns (True, fig) if a chart was rendered, (False, None) otherwise.
    """
    try:
        graph_info = detect_graph_request(query, df.columns.tolist(), get_openai_response)
        if not graph_info:
            return False, None
        
        chart_type = graph_info.get("chart_type")
        x_col = graph_info.get("x_col")
        y_col = graph_info.get("y_col")
        
        fig, error = generate_plotly_chart(df, chart_type, x_col, y_col)
        if error:
            st.error(error)
            return False, None
        
        # Render the Plotly chart
        st.markdown("### Chart")
        st.plotly_chart(fig, use_container_width=True)
        return True, fig
    
    except Exception as e:
        st.error(f"Error rendering graph: {str(e)}")
        return False, None