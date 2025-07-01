from datetime import datetime, date
import json

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime and date objects."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

def serialize_data(data):
    """Recursively serialize data, handling datetime objects and other non-serializable types."""
    if isinstance(data, (datetime, date)):
        return data.isoformat()
    elif isinstance(data, dict):
        return {str(k) if k is not None else "": serialize_data(v) for k, v in data.items()}
    elif isinstance(data, (list, tuple)):
        return [serialize_data(item) for item in data]
    elif hasattr(data, 'isoformat'):  # Handles date/time objects
        return data.isoformat()
    else:
        return data

def prepare_for_db(data):
    """Prepare data for database insertion by ensuring it's JSON serializable."""
    return json.loads(json.dumps(data, cls=DateTimeEncoder))
