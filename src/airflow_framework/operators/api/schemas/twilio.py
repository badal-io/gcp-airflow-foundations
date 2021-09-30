import logging

def get_twilio_schema(tableId=None):
    if tableId == 'twilio_text_messages':
        schema_fields = [
            {"name": "account_sid", "type": "STRING", "mode": "NULLABLE"},
            {"name": "api_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "body", "type": "STRING", "mode": "NULLABLE"},
            {"name": "date_created", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "date_sent", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "date_updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "direction", "type": "STRING", "mode": "NULLABLE"},
            {"name": "error_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "error_message", "type": "STRING", "mode": "NULLABLE"},
            {"name": "from", "type": "STRING", "mode": "NULLABLE"},
            {"name": "messaging_service_sid", "type": "STRING", "mode": "NULLABLE"},
            {"name": "num_media", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "num_segments", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "price_unit", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sid", "type": "STRING", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "to", "type": "STRING", "mode": "NULLABLE"},
            {"name": "uri", "type": "STRING", "mode": "NULLABLE"},
            {"name": "feedback", "type": "STRING", "mode": "NULLABLE"},
            {"name": "media", "type": "STRING", "mode": "NULLABLE"}
        ]
        return schema_fields
    else:
        logging.info(f"Schema unavailable for {tableId}")
        raise Exception(f"Schema unavailable for {tableId}")

