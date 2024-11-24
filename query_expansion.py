import google.generativeai as genai
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Access the API key
api_key = os.getenv('YOUR_ENV_VARIABLE_NAME')

# Configure the GenAI API
if api_key:
    genai.configure(api_key=api_key)
else:
    raise ValueError("API key is missing. Make sure it's set in the .env file.")

# Function to refine the query
def refine_query(query):
    """
    Refines a user-provided query to improve clarity, context, and accuracy.

    Args:
        query (str): The input search query.
    
    Returns:
        str: The refined query.
    """
    prompt = (
        f"The user has entered the following search query: '{query}'. "
        "Refine the query to address issues such as incompleteness, ambiguity, spelling errors, overly broad topics, "
        "lack of context, or vague language. Correct errors and expand or clarify the query as needed. "
        "Output only the refined query without any extra explanation or comments."
    )
    try:
        # Initialize the generative model
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)

        # Return the refined query
        return response.text.strip()
    except Exception as e:
        raise RuntimeError(f"Error while refining query: {e}")
