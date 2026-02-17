import os
from google import genai
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Embedder:
    def __init__(self):
        # Initialize Supabase
        url: str = os.environ.get("SUPABASE_URL")
        key: str = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables.")
        self.supabase: Client = create_client(url, key)

        # Initialize Gemini
        gemini_key = os.environ.get("GEMINI_API_KEY")
        if not gemini_key:
            raise ValueError("GEMINI_API_KEY must be set in environment variables.")
        
        self.client = genai.Client(api_key=gemini_key)
        self.model = "text-embedding-004"

    def get_embedding(self, text: str) -> list[float]:
        """
        Retrieves embedding for the given text.
        Checks Supabase cache first. If missing, generates via Gemini API and caches it.
        """
        try:
            # Check cache
            response = self.supabase.table("product_embeddings_cache").select("embedding").eq("name", text).execute()
            
            if response.data and len(response.data) > 0:
                print(f"Embedder: Cache HIT for '{text}'")
                return response.data[0]["embedding"]

            # Generate new embedding
            print(f"Embedder: Cache MISS for '{text}'. Generating via Gemini...")
            
            result = self.client.models.embed_content(
                model=self.model,
                contents=text,
                config={'title': "Product Embedding"} # task_type not strictly needed or handled differently in new client usually
            )
            # Accessing the first embedding's values
            if not result.embeddings:
                 raise ValueError("No embedding returned")
            
            embedding = result.embeddings[0].values

            # Cache the result
            self.supabase.table("product_embeddings_cache").upsert({
                "name": text,
                "embedding": embedding
            }).execute()

            return embedding

        except Exception as e:
            print(f"Embedder Error: {e}")
            return []
