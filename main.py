import base64
import csv
import shutil
import tempfile
from datetime import datetime
import time
import psycopg2
import os
import requests
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from git import Repo
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import storage

# todo - ensure if need to be changed, workaround change GUID to yours local generated
provider = ''
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "maria-daniela-project-firebase-adminsdk.json"
cred = credentials.Certificate('maria-daniela-project-firebase-adminsdk.json')
firebase_admin.initialize_app(cred)


def configure_chrome_driver():
    chrome_driver_path = "chromedriver_path"
    service = Service(executable_path=chrome_driver_path)
    driver = webdriver.Chrome(service=service)
    return driver


def login_to_mergestat(driver):
    try:
        driver.get("http://localhost:3300/login")
        username = driver.find_element(By.CSS_SELECTOR, 'input[placeholder="username"]')
        password = driver.find_element(By.CSS_SELECTOR, 'input[placeholder="password"]')
        username.send_keys("postgres")
        password.send_keys("password")
        login_button = driver.find_element(By.CSS_SELECTOR, 'button.t-button-primary')
        login_button.click()
        wait = WebDriverWait(driver, 10)
        wait.until(EC.url_contains("http://localhost:3300/repos"))
    except NoSuchElementException:
        print("Username or password input fields not found.")
        driver.quit()
    except TimeoutException:
        print("Login element not found or not clickable.")
        driver.quit()


def add_repository(driver, guid, repo):
    try:
        wait = WebDriverWait(driver, 5)
        driver.get("http://localhost:3300/repos/git-sources/" + guid)
        wait.until(EC.url_contains("http://localhost:3300/repos/git-sources/"))
        time.sleep(2)
        repo_input = driver.find_element(By.CSS_SELECTOR, 'input[placeholder="https://github.com/owner/repo"]')
        repo_input.clear()
        repo_input.send_keys(repo)
        add_button = driver.find_element(By.CSS_SELECTOR, 'button.t-button-primary')
        add_button.click()
        # wait.until(EC.url_contains("http://localhost:3300/repos/git-sources/*"))
    except NoSuchElementException:
        print("Repos input fields not found.")
        driver.quit()
    except TimeoutException:
        print("Keep waiting")
        driver.quit()


def get_matching_repositories(num):
    url = 'https://api.github.com/search/repositories'
    params = {
        'q': 'language:java size:<500',
        'sort': 'commits',
        'order': 'desc',
        'per_page': num
    }
    response = requests.get(url, params=params)
    response_data = response.json()
    repositories = []

    if response.status_code == 200:
        for item in response_data['items']:
            repository = item['clone_url']
            repositories.append(repository)
            print(repository)
    return repositories


def connect_to_db():
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="password",
        database="postgres"
    )
    return conn


def insert_repos_top_postgres(matching_repositories, provider_guid, conn):
    try:
        with conn.cursor() as cursor:
            for repository in matching_repositories:
                sql = "INSERT INTO repos (repo, provider) VALUES (%s, %s) ON CONFLICT DO NOTHING"
                cursor.execute(sql, (repository, provider_guid))
            conn.commit()
    except psycopg2.Error as e:
        print("Error:", e)
        conn.rollback()


def query_repos(cur):
    cur.execute("SELECT * FROM repos")
    repos_result = cur.fetchall()
    print("List of inserted repositories:")
    for row in repos_result:
        print(row)


def get_provider_guid(cur):
    cur.execute("SELECT DISTINCT provider FROM repos")
    result = cur.fetchall()
    return [row[0] for row in result]


def close_connection(conn):
    conn.cursor().close()
    conn.close()


def get_repository_ref_url(cur):
    cur.execute("SELECT id FROM repos")
    repos_result = cur.fetchall()
    urls_list = []
    for row in repos_result:
        urls_list.append(row[0])
    return urls_list


def sync_repo_from_repo_url(driver, repo_url):
    try:
        driver.get("http://localhost:3300/repos/" + repo_url)
        wait = WebDriverWait(driver, 10)
        print("Sync for repository:", repo_url)
        wait.until(EC.url_contains("http://localhost:3300/repos/"))
        time.sleep(2)
        button_paths = [
            "//table[@class='t-table-default t-table-hover']//tr[1]//td[7]//button",
            "//table[@class='t-table-default t-table-hover']//tr[2]//td[7]//button",
            "//table[@class='t-table-default t-table-hover']//tr[3]//td[7]//button",
            "//table[@class='t-table-default t-table-hover']//tr[4]//td[7]//button",
            "//table[@class='t-table-default t-table-hover']//tr[5]//td[7]//button"
        ]
        for i, button_xpath in enumerate(button_paths):
            button_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, button_xpath)))
            button_element = button_elements[0]
            ActionChains(driver).move_to_element(button_element).click().perform()
            print("Button", i + 1, "found and clicked")
    except NoSuchElementException:
        print("Ref element not found for repository:", repo_url)
    except TimeoutException:
        print("Timeout, keep waiting for repository:", repo_url)


def get_first_middle_last_commits_to_csv(cur):
    query = """
     WITH ranked_commits AS (
        SELECT repo_id, commit_hash, author_when,
               ROW_NUMBER() OVER (PARTITION BY repo_id ORDER BY author_when ASC) AS commit_rank,
               COUNT(*) OVER (PARTITION BY repo_id) AS total_commits
        FROM git_blame
    ),
    first_commits AS (
        SELECT repo_id, commit_hash AS first_commit_hash, author_when AS first_commit_date
        FROM ranked_commits
        WHERE commit_rank = 1
    ),
    middle_commits AS (
        SELECT repo_id, commit_hash AS middle_commit_hash, author_when AS middle_commit_date
        FROM ranked_commits
        WHERE commit_rank = CEIL(total_commits / 2.0)
    ),
    last_commits AS (
        SELECT repo_id, commit_hash AS last_commit_hash, author_when AS last_commit_date
        FROM ranked_commits
        WHERE commit_rank = total_commits
    )
    SELECT r.repo,f.repo_id, f.first_commit_hash, f.first_commit_date,
           m.middle_commit_hash, m.middle_commit_date,
           l.last_commit_hash, l.last_commit_date
    FROM first_commits f
    JOIN middle_commits m ON f.repo_id = m.repo_id
    JOIN last_commits l ON f.repo_id = l.repo_id
    join repos r on r.id = f.repo_id
    ORDER BY f.repo_id
    """
    cur.execute(query)
    rows = cur.fetchall()

    # Define the CSV file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = "first_middle_last_commits.csv"

    # Write the data to the CSV file
    with open(csv_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        # Write the header row
        writer.writerow(["repo", "repo_id", "first_commit_hash", "first_commit_date",
                         "middle_commit_hash", "middle_commit_date",
                         "last_commit_hash", "last_commit_date"])
        # Write the data rows
        writer.writerows(rows)


def retrieve_code_snapshot(repo_url, commit_hash, temp_folder):
    # Clone the repository in the given temporary directory
    repo = Repo.clone_from(repo_url, temp_folder)
    repo.git.checkout(commit_hash)

    # Create a temporary folder to hold the .java files for this commit
    commit_folder = os.path.join(temp_folder, "commit")
    os.makedirs(commit_folder, exist_ok=True)

    # Move .java files to the commit folder based on commit hash
    for root, _, files in os.walk(temp_folder):
        for file in files:
            if file.endswith(".java"):
                # print(f"Moving file: {file}")
                os.makedirs(os.path.dirname(os.path.join(commit_folder, file)), exist_ok=True)
                shutil.move(os.path.join(root, file), os.path.join(commit_folder, file))

    return commit_folder


def get_repo_name(repo_url):
    return os.path.basename(repo_url).replace(".git", "")


def run():
    driver = configure_chrome_driver()
    try:
        login_to_mergestat(driver)
        initial_repo = 'https://github.com/Daniel-Himself/KnightsMove-Zebra.git'
        conn = connect_to_db()
        provider = get_provider_guid(conn.cursor())[0]
        print('***********   ', provider)
        add_repository(driver, provider, initial_repo)
        repos = get_matching_repositories('1')

        insert_repos_top_postgres(repos, provider, conn)
        query_repos(conn.cursor())
        urls_list = get_repository_ref_url(conn.cursor())
        print(urls_list)
        for repo_url in urls_list:
            sync_repo_from_repo_url(driver, repo_url)
        get_first_middle_last_commits_to_csv(conn.cursor())

        csv_file = "first_middle_last_commits.csv"  # Replace with the actual CSV file name
        firebase_storage_client = storage.Client()
        storage_bucket = firebase_storage_client.bucket("maria-daniela-project.appspot.com")
        with open(csv_file, mode="r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                repo_url = row["repo"]
                repo_name = get_repo_name(repo_url)
                first_commit_hash = row["first_commit_hash"]
                middle_commit_hash = row["middle_commit_hash"]
                last_commit_hash = row["last_commit_hash"]

                # Create temporary directories for each commit snapshot
                with tempfile.TemporaryDirectory() as first_temp_dir:
                    with tempfile.TemporaryDirectory() as mid_temp_dir:
                        with tempfile.TemporaryDirectory() as last_temp_dir:
                            # Retrieve the code snapshots
                            first_snapshot_folder = retrieve_code_snapshot(repo_url, first_commit_hash, first_temp_dir)
                            middle_snapshot_folder = retrieve_code_snapshot(repo_url, middle_commit_hash, mid_temp_dir)
                            last_snapshot_folder = retrieve_code_snapshot(repo_url, last_commit_hash, last_temp_dir)

                            # Upload the .java files to Firestore Storage for the first commit
                            for root, _, files in os.walk(first_snapshot_folder):
                                for file in files:
                                    if file.endswith(".java"):
                                        local_file_path = os.path.join(root, file)
                                        remote_file_path = f"code_snapshots/first/{repo_name}/{file}"
                                        print(f"Uploading file: {local_file_path} to {remote_file_path}")
                                        storage_bucket.blob(remote_file_path).upload_from_filename(local_file_path,
                                                                                                   content_type="text/plain")

                            # Upload the .java files to Firestore Storage for the middle commit
                            for root, _, files in os.walk(middle_snapshot_folder):
                                for file in files:
                                    if file.endswith(".java"):
                                        local_file_path = os.path.join(root, file)
                                        remote_file_path = f"code_snapshots/mid/{repo_name}/{file}"
                                        print(f"Uploading file: {local_file_path} to {remote_file_path}")
                                        storage_bucket.blob(remote_file_path).upload_from_filename(local_file_path,
                                                                                                   content_type="text/plain")

                            # Upload the .java files to Firestore Storage for the last commit
                            for root, _, files in os.walk(last_snapshot_folder):
                                for file in files:
                                    if file.endswith(".java"):
                                        local_file_path = os.path.join(root, file)
                                        remote_file_path = f"code_snapshots/last/{repo_name}/{file}"
                                        print(f"Uploading file: {local_file_path} to {remote_file_path}")
                                        storage_bucket.blob(remote_file_path).upload_from_filename(local_file_path,
                                                                                                   content_type="text/plain")

        print("Code snapshots retrieved and stored in Firestore Storage.")

    except Exception as e:
        print("Error:", e)

    finally:
        pass
        driver.quit()
        close_connection(conn)


run()