from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

# Define a route for the API endpoint
@app.route('/api/hello', methods=['GET'])
def hello():
    # Return a JSON response
    return jsonify(message="Hello, Flask API!",
                   data="test")

@app.route('/metrics/orders', methods=['GET'])
def get_data():

    try:
        connection = psycopg2.connect(
            host="localhost",
            database="user_data",
            user="omerliler",
            password="admin",
            port="5432"
        )
        
        cursor = connection.cursor()

        query = """
            SELECT 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cumulative_session_time)  AS median_session_duration,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY session_rank) AS median_session_count
            FROM
                (SELECT 
                    *,
                    RANK() OVER (PARTITION BY customer_id ORDER BY session_id) AS session_rank,
                    SUM(session_duration) OVER (PARTITION BY customer_id ORDER BY session_id) AS cumulative_session_time
                FROM session_data
                
                )
            WHERE order_placed = true;
        """

        cursor.execute(query)
        results = cursor.fetchall()
        
        median_visits_before_order = results[0][1]
        median_session_duration_minutes_before_order = results[0][0]

        cursor.close()
        connection.close()

    except Exception as e:
        print('\nError occured while getting data from postgres db')
        print(f'Error message: {e}')
        return jsonify(message = f"Error message: {e}")



    # Return a JSON response
    return jsonify(median_visits_before_order=median_visits_before_order,
                   median_session_duration_minutes_before_order=median_session_duration_minutes_before_order)

if __name__ == '__main__':
    # Run the Flask application
    app.run(debug=True, port=5050)