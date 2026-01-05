import os
import pandas as pd
import sqlite3
from flask import Flask, render_template, request, redirect, url_for, flash
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'your_super_secret_key'  # Change this to a random secret key
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

DATABASE = 'processed_subscriber_ids.db'

def get_db():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DATABASE)
    return conn

def init_db():
    """Initializes the database and creates the 'processed_ids' table if it doesn't exist."""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_ids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscriber_id TEXT UNIQUE NOT NULL
            )
        ''')
        db.commit()

@app.route('/')
def index():
    """Renders the main page of the application."""
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    """Handles the file upload and the main processing logic."""
    if 'data_file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    file = request.files['data_file']
    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)

    if file:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        try:
            if filename.endswith('.csv'):
                df = pd.read_csv(filepath)
            elif filename.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(filepath)
            else:
                flash('Invalid file type. Please upload a CSV or Excel file.')
                return redirect(request.url)
        except Exception as e:
            flash(f'Error reading file: {e}')
            return redirect(request.url)

        # Assuming the subscriber ID is in the first column
        subscriber_id_col = df.columns[0]
        new_subscriber_ids = df[subscriber_id_col].dropna().astype(str).tolist()

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT subscriber_id FROM processed_ids")
        processed_ids = {row[0] for row in cursor.fetchall()}

        unique_new_ids = [sub_id for sub_id in new_subscriber_ids if sub_id not in processed_ids]

        num_files = int(request.form.get('num_files', 0))
        output_files_data = []
        for i in range(1, num_files + 1):
            file_name = request.form.get(f'output_file_{i}')
            num_ids = int(request.form.get(f'num_ids_{i}', 0))
            if file_name and num_ids > 0:
                output_files_data.append({'name': file_name, 'count': num_ids})

        if not unique_new_ids:
            flash('No new unique subscriber IDs found in the uploaded file.')
            return redirect(url_for('index'))

        processed_for_this_run = set()
        start_index = 0
        for file_data in output_files_data:
            end_index = start_index + file_data['count']
            ids_for_file = unique_new_ids[start_index:end_index]

            if not ids_for_file:
                break

            output_df = pd.DataFrame({subscriber_id_col: ids_for_file})
            output_filename = secure_filename(file_data['name'])
            if not output_filename.endswith('.csv'):
                output_filename += '.csv'

            output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
            output_df.to_csv(output_path, index=False)

            processed_for_this_run.update(ids_for_file)
            start_index = end_index

        if processed_for_this_run:
            to_insert = [(sub_id,) for sub_id in processed_for_this_run]
            cursor.executemany("INSERT OR IGNORE INTO processed_ids (subscriber_id) VALUES (?)", to_insert)
            conn.commit()
            flash(f'Successfully created {len(output_files_data)} files with unique subscriber IDs.')
        else:
            flash('No new subscriber IDs were processed.')

        conn.close()
        return redirect(url_for('index'))

    return redirect(url_for('index'))

if __name__ == '__main__':
    init_db()
    app.run(debug=True)
