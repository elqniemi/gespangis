import psycopg2
import concurrent.futures

# Database connection parameters
db_params = {
    'dbname': 'ligma',
    'user': 'postgres',
    'password': 'balls',
    'host': 'localhost',
    'port': '8888'
}

# Function to process a chunk of routes and insert results into a new table
def process_chunk(start_id, end_id):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Call the analyze_routes function and insert the results
        query = """
        INSERT INTO results.route_cell_stats_9
        SELECT * FROM analyze_routes_parallel(%s, %s);
        """
        cursor.execute(query, (start_id, end_id))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Processed IDs from {start_id} to {end_id}")
    except Exception as e:
        print(f"Error processing IDs from {start_id} to {end_id}: {e}")
    
def main():
    # Total routes and chunk size
    total_routes = 150000
    chunk_size = 100

    # Creating chunks
    chunks = [(i, min(i + chunk_size - 1, total_routes)) for i in range(1, total_routes + 1, chunk_size)]

    # Using ThreadPoolExecutor to process chunks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(lambda x: process_chunk(*x), chunks)

if __name__ == "__main__":
    main()

