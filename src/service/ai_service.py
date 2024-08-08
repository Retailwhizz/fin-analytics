import vertexai
import re
from vertexai.generative_models import GenerativeModel, Part, SafetySetting, FinishReason
from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel, Image
import json

vertexai.init(project="hackeasy-red-flags", location="us-central1")
model = GenerativeModel(
    "gemini-1.5-flash-001",
)

generation_config = {
    "max_output_tokens": 8192,
    "temperature": 1,
    "top_p": 0.95,
}

safety_settings = [
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
    ),
]


def prepromt():
    text = """
        Generate a highly accurate and performant BigQuery SQL query for sales data analysis, leveraging the provided schema:
        retailer(id, name, city)
        distributor(id, name, type)
        retailer_distributor_mapping(retailer_id, distributor_id, credit_limit)
        invoice(id, retailer_id, distributor_id, amount, date, due_date, order_date)
        invoice_item(id, invoice_id, item_code, name, manufacture, company, scheme, discount, orderedQnt, billedQnt, freeQnt, finalQnt, bouncedQnt, schemeType, offerType)
        payments(id, display_id, invoice_id, amount, date, created_by_type)
        
        relation:
        retailer_distributor_mapping.retailer_id = retailer.id
        retailer_distributor_mapping.distributor_id = distributor.id
        invoice.retailer_id = retailer.id
        invoice.distributor_id = retailer.distributor_id
        invoice.id = invoice_item.invoice_id
        invoice.id = payments.invoice_id
        
        Add Below Parameters while generating queries:
        project=hackeasy-red-flags
        dataset=retailer_sales
        
        Ensure the queries consider the relationships between these tables. Additionally, categorize retailers into four categories: New (has not placed orders), Active (placed orders in the last 60 days), Dormant (placed orders between 60 and 120 days), and Churned (not placed orders in 120 days). Incorporating user-defined metrics, time-series analysis, and potential visualizations. Handle multiple datasets, complex query structures, and data quality issues. Consider advanced analytics techniques as needed. Answer only with query and do not provide any explanation.
    """
    return text


def retailers():
    text = """
    Generate a highly accurate and performant BigQuery SQL query for sales data analysis, leveraging the provided schema:
        retailer(id, name, city)
        
        Add Below Parameters while generating queries:
        project=hackeasy-red-flags
        dataset=retailer_sales
        
        Q: Get names of 10 retailers
        A:
    """
    return text


def replace_backticks(query):
    query = re.sub(r'^```', '', query)
    query = re.sub(r'```$', '', query)
    query = re.sub(r'^\s*sql', '', query, flags=re.IGNORECASE)
    return query


def execute_query(sql_query):
    # Create a BigQuery client
    bq_client = bigquery.Client()

    # Execute the SQL query
    query_job = bq_client.query(sql_query)
    results = query_job.result()
    rows = [dict(row) for row in results]
    #json_results = json.dumps(rows[:6], indent=2)
    return rows


def run_model(prompt):
    return model.generate_content(
        [prompt],
        generation_config=generation_config,
        safety_settings=safety_settings,
        stream=True,
    )


def generate():
    responses = run_model(prepromt())

    sql_query = ""
    for response in responses:
        sql_query += response.text

    print("Query :", sql_query)

    rows = execute_query(replace_backticks(sql_query))
    print(rows)


# generate()


def get_dynamic_results(question):
    # Q: Categorise the retailers into good & bad retailers based on if the retailers are making payments before due date
    prompt = f"{prepromt()}\nQ: {question}\nA:"
    responses = run_model(prompt)

    sql_query = ""
    for response in responses:
        sql_query += response.text

    sql_query = replace_backticks(sql_query)
    print("Final Query :", sql_query)

    rows = execute_query(sql_query)
    return rows
