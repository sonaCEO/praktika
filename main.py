import requests
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError
from pywebio.input import input, input_group, select
from pywebio.output import put_html
from pywebio import start_server

def db_connection(db_host, db_port, db_name, db_user, db_password):
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        print("Connected to the database")
    except OperationalError as e:
        print("Error")
    return conn


conn = db_connection("127.0.0.1", "5434", "vacancies", "postgres", "sonapost")

def table(conn):
    with conn.cursor() as cursor:
        table_query = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            company VARCHAR(255),
            city VARCHAR(100),
            salary_from INTEGER,
            salary_to INTEGER,
            currency VARCHAR(3),
            schedule VARCHAR(50),
            experience VARCHAR(50),
            employment_type VARCHAR(50),
            vacancy_url TEXT
        );
        """
        cursor.execute(table_query)
    conn.commit()
table(conn)

def insert_vacancy_data(conn, vacancy_data):
    with conn.cursor() as cursor:
        query = sql.SQL("""
            INSERT INTO vacancies (name, company, city, salary_from, salary_to, currency, schedule, experience, employment_type, vacancy_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """)
        cursor.execute(query, (
            vacancy_data['name'],
            vacancy_data['company'],
            vacancy_data['city'],
            vacancy_data['salary_from'],
            vacancy_data['salary_to'],
            vacancy_data['currency'],
            vacancy_data['schedule'],
            vacancy_data['experience'],
            vacancy_data['employment_type'],
            vacancy_data['vacancy_url']
        ))
    conn.commit()

EMPLOYMENT_TYPE_MAP = {
    'all': None,
    'full': 'full',
    'part': 'part',
    'probation': 'probation'
}
def found_vacancies_and_fill_db(job_title=None, city_id=None, employment_type=None):
    vacancies_to_insert = []
    page = 0
    has_more_pages = True
    employment_type_api = EMPLOYMENT_TYPE_MAP.get(employment_type, None)

    while has_more_pages and page <= 50:
        params = {
            'text': job_title,
            'area': city_id,
            'employment': employment_type_api,
            'page': page
        }

        if employment_type_api:
            params['employment'] = employment_type_api
        response = requests.get('https://api.hh.ru/vacancies', params=params)
        vacancies = response.json()
        page_items = vacancies.get('items', [])
        has_more_pages = vacancies.get('pages', page) > page

        for item in page_items:
            name = item.get('name', '').lower()
            if job_title.lower() in name.lower():
                salary_data = item.get('salary', {})
                if salary_data:
                    salary_from = salary_data.get('from')
                    salary_to = salary_data.get('to')
                    currency = salary_data.get('currency', 'RUR')
                else:
                    salary_from = None
                    salary_to = None
                    currency = 'RUR'
                salary_str = 'не указана'
                if salary_from and salary_to:
                    salary_str = f"{salary_from} - {salary_to} {currency}"
                elif salary_from:
                    salary_str = f"от {salary_from} {currency}"
                elif salary_to:
                    salary_str = f"до {salary_to} {currency}"

                vacancy_data = {
                    'name': item.get('name'),
                    'company': item.get('employer', {}).get('name'),
                    'city': item.get('area', {}).get('name'),
                    'salary_from': salary_from,
                    'salary_to': salary_to,
                    'currency': currency,
                    'schedule': item.get('schedule', {}).get('name'),
                    'experience': item.get('experience', {}).get('name'),
                    'employment_type': item.get('employment', {}).get('name'),
                    'vacancy_url': f"https://hh.ru/vacancy/{item.get('id')}"
                }

                vacancies_to_insert.append(vacancy_data)
                vacancy_output = f"""
                        <p><b>Job title:</b> {vacancy_data['name']}</p>
                        <p><b>Company:</b> {vacancy_data['company']}</p>
                        <p><b>City:</b> {vacancy_data['city']}</p>
                        <p><b>Salary:</b> {salary_str}</p>
                        <p><b>Schedule:</b> {vacancy_data['schedule']}</p>
                        <p><b>Work experience:</b> {vacancy_data['experience']}</p>
                        <p><b>Employment:</b> {vacancy_data['employment_type']}</p>
                        <p><b>Vacancy link:</b> <a href="{vacancy_data['vacancy_url']}" target="_blank">{vacancy_data['vacancy_url']}</a></p>
                        <br>
                        """
                put_html(vacancy_output)
        page+=1

    if not vacancies_to_insert:
        put_html("<p>No vacancies found</p>")
    else:
        for vacancy_data in vacancies_to_insert:
            insert_vacancy_data(conn, vacancy_data)
def remove_duplicates(conn):
    with conn.cursor() as cursor:
        delete_query = """
        DELETE FROM vacancies
        WHERE ctid IN (
            SELECT ctid
            FROM (
                SELECT ctid, ROW_NUMBER() OVER (PARTITION BY vacancy_url ORDER BY ctid) AS rnum
                        FROM vacancies
            ) t
            WHERE t.rnum > 1
            );
            """
        cursor.execute(delete_query)
    conn.commit()
remove_duplicates(conn)

def main():
    employment_options = list(EMPLOYMENT_TYPE_MAP.keys())
    data = input_group("Entering data for job search", [
        input('Enter the job title', name='job_title'),
        input('Enter the city ID (1 - Moscow, 2 - St. Petersburg, 3 - Yekaterinburg)', name='city_id', type='number'),
        select('Choose the type of employment', options=employment_options, name='employment_type')
    ])
    user_selected_employment = data['employment_type']
    employment_type_api = EMPLOYMENT_TYPE_MAP.get(user_selected_employment, None)
    found_vacancies_and_fill_db(job_title=data['job_title'], city_id=data['city_id'], employment_type=employment_type_api)

if __name__ == '__main__':
    start_server(main, port=8080)
