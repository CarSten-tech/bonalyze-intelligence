
import os
from google import genai
from dotenv import load_dotenv

load_dotenv()

api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    print("No API Key found")
    exit(1)

client = genai.Client(api_key=api_key)

try:
    response = client.models.embed_content(
        model="text-embedding-004",
        contents="Hello world"
    )
    # Check response structure
    # response should be EmbedContentResponse
    # it likely has embeddings list
    print("Response type:", type(response))
    print("Response:", response)
    
    if hasattr(response, 'embeddings') and response.embeddings:
        print("Embedding length:", len(response.embeddings[0].values))
        print("Success!")
    else:
        print("No embeddings found in response")

except Exception as e:
    print(f"Error: {e}")
