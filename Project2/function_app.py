import azure.functions as func  # Importing Azure Functions library for creating HTTP trigger functions
import logging  # Logging for error and status tracking
import requests  # HTTP requests library for making API calls
from azure.storage.blob import BlobServiceClient  # Blob service client for interacting with Azure Blob Storage
from sklearn.metrics.pairwise import cosine_similarity  # Function for computing cosine similarity
import os  # Library for interacting with the OS (filesystem, environment variables, etc.)
import tempfile  # Temporary file management
import papermill as pm  # Papermill library for running Jupyter notebooks
import subprocess  # Subprocess for executing system commands
from datetime import datetime  # Library for handling date and time
import nbformat  # Notebook format handler for reading and writing Jupyter notebooks
from pymongo import MongoClient  # MongoDB client for database operations
import urllib.parse  # Library for URL encoding
import time  # Library for time tracking

# Creating an Azure Function App with anonymous access
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Constants for API endpoints (LLM and embedding models from Hugging Face)
API_URL = "https://api-inference.huggingface.co/models/meta-llama/Meta-Llama-3-8B-Instruct"
API_URL2 = "https://api-inference.huggingface.co/models/obrizum/all-mpnet-base-v2"

# HTTP headers for API requests
headers = {
    "Authorization": f"Bearer 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",  # Bearer token for authentication
    "Content-Type": "application/json"  # Set the content type to JSON
}

# Function to call the Llama3 model API and return a response
def prompt_llama3(payload):
    start_time = time.time()  # Start timing the operation
    try:
        response = requests.post(API_URL, headers=headers, json=payload)  # Make POST request to Llama3 API
        response.raise_for_status()  # Raise error if the response status is not OK
        return response.json(), time.time() - start_time  # Return JSON response and time taken
    except requests.RequestException as e:
        logging.error(f"Error calling LLM API: {e}")  # Log any request errors
        return {"error": "LLM API request failed"}, time.time() - start_time  # Return error message

# Function to generate embeddings using the embedding model API
def generate_embedding(payload):
    start_time = time.time()  # Start timing the operation
    try:
        response = requests.post(API_URL2, headers=headers, json=payload)  # Make POST request to embedding model API
        response.raise_for_status()  # Raise error if the response status is not OK
        return response.json(), time.time() - start_time  # Return JSON response and time taken
    except requests.RequestException as e:
        logging.error(f"Error calling embedding model API: {e}")  # Log any request errors
        return {"error": "Embedding generation API request failed"}, time.time() - start_time  # Return error message

# Function to perform semantic search using cosine similarity between query and stored embeddings
def semantic_search(query_embedding, embeddings, num_results=6):
    similarities = cosine_similarity([query_embedding], embeddings)[0]  # Calculate cosine similarity
    most_similar_indices = similarities.argsort()[-num_results:][::-1]  # Get indices of most similar embeddings
    return most_similar_indices  # Return the indices of the top matches

# First HTTP trigger function that responds to requests
@app.route(route="test")
def test(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    overall_start_time = time.time()  # Start timing the whole process

    test_query = req.params.get('prompt')  # Get the 'prompt' parameter from the request

    # Generate embedding for the input query
    embeded_query, embedding_duration = generate_embedding({
        "inputs": test_query,
        "options": {
            "wait_for_model": True  # Wait until the model is ready
        }
    })

    # Connect to MongoDB to fetch stored embeddings
    db_start_time = time.time()
    client = MongoClient("mongodb+srv://azizjaz24:BulW5vV8Tbn1XNFY@cluster0.vqxbqai.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client.Knowledge_Base
    colec = db.Incidents_Best_practises  # Select the relevant collection from the database
    db_duration = time.time() - db_start_time  # Time taken to connect to the database

    # Fetch stored embeddings from MongoDB
    vectors_start_time = time.time()
    embedding_documents = colec.find({}, {"_id": 0, "embedded_response_description": 1})  # Retrieve only embedded descriptions
    vectors = [doc["embedded_response_description"] for doc in embedding_documents]  # Extract embeddings
    vectors_duration = time.time() - vectors_start_time + db_duration  # Time taken to load vectors and connect to DB

    # Perform semantic search to find the closest descriptions to the query
    search_start_time = time.time()
    closest_indices = semantic_search(embeded_query, vectors, num_results=4)  # Find top 4 similar entries
    search_duration = time.time() - search_start_time  # Time taken for semantic search

    # Retrieve and format the closest descriptions based on the semantic search result
    descriptions_start_time = time.time()
    closest_descriptions = []
    for idx in closest_indices:
        document = colec.find_one({"embedded_response_description": vectors[idx]})  # Fetch the document corresponding to the embedding
        if document:
            closest_descriptions.append(
                f"For the considered incident {document['incident']} it is recommended to do the following: {document['incident_response_steps']}"
            )
    descriptions_duration = time.time() - descriptions_start_time  # Time taken to generate descriptions
    context = "\n".join(closest_descriptions)  # Combine descriptions into a single string

    # Send the query and context to Llama3 for further processing and recommendations
    llama_response_start_time = time.time()
    output1, llama_response_duration = prompt_llama3({
        "inputs": f"Answer this question:\n{test_query}\nby analyzing and synthesizing the following information:\n{context}\nand justify your choice of recommendation briefly after you give the recommendations.",
        "parameters": {
            "temperature": 0.8,  # Controls randomness in generation
            "top_p": 0.9,  # Limits tokens with cumulative probability
            "top_k": 50,  # Limits tokens to a fixed size of vocabulary
            "max_new_tokens": 250,  # Maximum number of tokens to generate
            "repetition_penalty": 1.0,  # Penalty for repeating phrases
            "return_full_text": False,  # Do not return the full input
            "options": {
                "use_cache": False  # Do not cache model responses
            }
        }
    })
    llama_response_duration = time.time() - llama_response_start_time  # Time taken to get a response from Llama3

    # If there's an error in the Llama3 response, return an error message
    if "error" in output1:
        return func.HttpResponse(f"Error from LLM API: {output1['error']}", status_code=500)

    # Extract the Llama3 generated text
    Llama_response = output1[0]["generated_text"]

    overall_duration = time.time() - overall_start_time  # Total time for the function

    # Return the Llama response and the time taken for each step
    return func.HttpResponse(
        f"Llama Response: {Llama_response}\n\n"
        f"Overall Process Duration: {overall_duration:.2f} seconds\n"
        f"Embedding Generation Duration: {embedding_duration:.2f} seconds\n"
        f"Database Connection + Vector Loading Duration: {vectors_duration:.2f} seconds\n"
        f"Semantic Search Duration: {search_duration:.2f} seconds\n"
        f"Description Generation Duration: {descriptions_duration:.2f} seconds\n"
        f"Llama Response Generation Duration: {llama_response_duration:.2f} seconds",
        status_code=200
    )

# Function to install Jupyter kernel for Papermill to execute notebooks
def InstallJupyterKernel():
    os.system('pip install ipykernel')  # Install IPython kernel
    os.system('python -m ipykernel install --user --name python3')  # Install kernel for Jupyter notebooks

# Second HTTP trigger function that processes Jupyter notebooks
@app.route(route="test2")
def test2(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger has processed a request.")
    
    # Configuration for accessing Azure Blob Storage
    CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=mltest2421780651;AccountKey=N4cLYuzfS7Ya+FmjDhynRVN4FVIBYl2sISkFN/D/rDz5sJ/dawY58M4flmCIIGp5tPH6mLBOfNeI+ASt3lbteA==;EndpointSuffix=core.windows.net"
    CONTAINER_NAME = "azureml-blobstore-d1cf2f28-70c7-4c18-bd1d-c34f59b63ff4"
    BLOB_NAME = "spec_test_notebook.ipynb"

    # Create blob service client for interacting with Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    # Download the notebook from the blob storage
    download_start_time = time.time()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".ipynb") as tmp_file:
        blob_client = container_client.get_blob_client(BLOB_NAME)
        tmp_file.write(blob_client.download_blob().readall())  # Write the downloaded notebook content to a temp file
        tmp_file.close()  # Close the temp file after writing

    # Run the downloaded notebook using Papermill
    run_start_time = time.time()
    executed_notebook_path = tmp_file.name.replace(".ipynb", "_executed.ipynb")
    pm.execute_notebook(tmp_file.name, executed_notebook_path)  # Execute the notebook
    run_duration = time.time() - run_start_time  # Time taken to run the notebook

    # Re-upload the executed notebook to the blob storage
    upload_start_time = time.time()
    with open(executed_notebook_path, "rb") as executed_notebook:
        container_client.upload_blob(name=f"executed_{BLOB_NAME}", data=executed_notebook, overwrite=True)  # Upload the executed notebook
    upload_duration = time.time() - upload_start_time  # Time taken to upload the executed notebook

    overall_duration = time.time() - download_start_time  # Total duration for the entire process

    return func.HttpResponse(
        f"Notebook executed and uploaded successfully.\n"
        f"Download Duration: {download_duration:.2f} seconds\n"
        f"Run Duration: {run_duration:.2f} seconds\n"
        f"Upload Duration: {upload_duration:.2f} seconds\n"
        f"Overall Process Duration: {overall_duration:.2f} seconds",
        status_code=200
    )
