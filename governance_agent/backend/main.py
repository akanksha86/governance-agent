import os
import json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery

app = FastAPI()

# Enable CORS for React development server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For demo purposes
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
VIEW_ID = f"{PROJECT_ID}.governance_export.metadata_evolution"

client = bigquery.Client(project=PROJECT_ID)

@app.get("/api/evolution/{table_name}")
async def get_evolution(table_name: str):
    query = f"""
        SELECT * 
        FROM `{VIEW_ID}` 
        WHERE entry_fqn LIKE '%{table_name}%'
        ORDER BY event_timestamp ASC
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        
        evolution_data = []
        for row in results:
            # The BigQuery client returns JSON as a dict if using SAFE.PARSE_JSON in view
            # or as a string if using TO_JSON_STRING.
            # My current view uses JSON_EXTRACT which returns a string in some contexts.
            # Let us handle both.
            all_aspects = row.all_aspects
            if isinstance(all_aspects, str):
                all_aspects = json.loads(all_aspects)
            
            schema = row.schema_aspect
            if isinstance(schema, str):
                schema = json.loads(schema)
            schema_data = schema.get("data", {}) if schema else {"fields": []}

            # Map all aspects. Identify column-level ones.
            aspects = {}
            column_aspects = {} # Map column -> list of aspect names
            
            if all_aspects:
                for key, val in all_aspects.items():
                    if "@Schema." in key:
                        col_parts = key.split("@Schema.")
                        col_name = col_parts[1]
                        aspect_type = key.split(".")[-1].split("@")[0] # Simple heuristic
                        if col_name not in column_aspects:
                            column_aspects[col_name] = []
                        column_aspects[col_name].append(aspect_type)
                    elif "data-governance-aspect" in key:
                        gov_data = val.get("data", {})
                        # If wrapped in 'governance_info' record, pull it to root
                        aspects["data-governance-aspect"] = gov_data.get("governance_info", gov_data)

            evolution_data.append({
                "id": str(row.event_timestamp),
                "timestamp": row.event_timestamp.isoformat(),
                "type": row.change_type,
                "user": row.user_email if row.user_email else "system@google.com",
                "summary": f"{row.change_type} for {table_name}",
                "snapshot": {
                    "fields": schema_data.get("fields", []),
                    "aspects": aspects,
                    "column_aspects": column_aspects
                }
            })
            
        return evolution_data
    except Exception as e:
        print(f"Error fetching evolution data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
