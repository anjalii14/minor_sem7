import pickle, json
import sqlite3
import time
from urllib.parse import unquote
from bs4 import BeautifulSoup

def processPage():
    pageLinkDict = {}
    cursor = c.execute('''SELECT term, postings FROM posting;''')  # Use the correct columns
    cnt = 0
    hit = 0
    miss = 0
    for row in cursor:
        cnt += 1
        if cnt % 100000 == 0:
            print(f"Processing row {cnt}...")
        
        try:
            # Assuming 'postings' is a JSON string, load it as a list
            postings = json.loads(row[1])
        except json.JSONDecodeError:
            print(f"Error decoding JSON for term: {row[0]}")
            continue  # Skip the problematic row if the JSON is invalid

        term = row[0]  # Assuming 'term' is a string identifier
        pageLinkDict[term] = []  # Using term as a string key

        # Process the postings, assuming each entry in the list contains relevant data
        for posting in postings:
            if isinstance(posting, list) and len(posting) > 0:
                doc_id = posting[0]  # First element might represent a document or ID
                
                # Check if the doc_id or corresponding titleIdDict value is numeric
                if str(doc_id).isdigit():
                    # If it's numeric, convert to integer
                    doc_id = int(doc_id)
                else:
                    # If it's not numeric, it must be a string, so we keep it as a string
                    doc_id = str(doc_id)

                # Now check for a match in titleIdDict
                if str(doc_id) in titleIdDict:
                    pageLinkDict[term].append(str(titleIdDict[str(doc_id)]))  # Store as string
                    hit += 1
                else:
                    miss += 1
            else:
                print(f"Unexpected posting structure: {posting}")  # Print unexpected posting formats

    # Save the resulting dictionary to a file
    pickle.dump(pageLinkDict, open('pageLinkDict.pkl', 'wb'))
    print(f"Processing complete. Hit count: {hit}")
    print(f"Miss count: {miss}")

if __name__ == '__main__':
    db_path = 'wsm.db'
    
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database: {e}")
        sys.exit(1)

    # Loading the titleIdDict file
    try:
        titleIdDict = pickle.load(open('./text/titleIdDict.pkl', 'rb'))
    except FileNotFoundError:
        print("Error: titleIdDict.pkl file not found.")
        sys.exit(1)
    except pickle.UnpicklingError:
        print("Error: Failed to unpickle titleIdDict.pkl file.")
        sys.exit(1)

    # Run the page processing function
    processPage()

    # Commit and close database connection
    conn.commit()
    conn.close()

