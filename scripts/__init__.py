import requests
from bs4 import BeautifulSoup

# Function to load the modules page directly
def load_modules_page(session, modules_url):
    # Step 1: Load the modules page
    response = session.get(modules_url)
    response.raise_for_status()  # Ensure the request was successful
    
    print("Navigated to modules page.")

    print(response.text)  # Print the entire HTML content of the page
    
    return response.text

# Function to download files from the modules page
def download_files_from_modules(session, modules_url, download_dir, base_url):
    response = session.get(modules_url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all file download links
    file_titles = ["Buy Now, Pay Later Data (Part 1)", 
                   "Buy Now, Pay Later Data (Part 2)", 
                   "Buy Now, Pay Later Data (Part 3)", 
                   "Buy Now, Pay Later Data (Part 4)"]
    
    for file_title in file_titles:
        # Try to find the link using the title attribute and class
        file_link = soup.find('a', title=file_title, class_="ig-title title item_link")
        
        if file_link:
            file_url = file_link['href']
            if not file_url.startswith("http"):
                file_url = base_url + file_url  # Construct the full URL
            
            # Navigate to the file item page
            item_page_response = session.get(file_url)
            item_soup = BeautifulSoup(item_page_response.text, 'html.parser')
            
            # Find the actual download link within the item page
            download_link = item_soup.find('a', {'download': 'true'})
            
            if download_link:
                download_url = download_link['href']
                if not download_url.startswith("http"):
                    download_url = base_url + download_url  # Construct the full download URL
                
                # Download the file
                file_name = download_url.split('/')[-1]
                file_response = session.get(download_url)
                with open(f"{download_dir}/{file_name}", 'wb') as f:
                    f.write(file_response.content)
                
                print(f"Downloaded: {file_name}")
            else:
                print(f"Download link not found for {file_title}.")
        else:
            print(f"File {file_title} not found.")

# Main ETL process function
def etl_process():
    # URLs and session
    base_url = "https://canvas.lms.unimelb.edu.au"
    modules_url = f"{base_url}/courses/188486/modules"  # Full URL to modules page

    session = requests.Session()  # Ensure the session is authenticated
    
    download_dir = '../data/tables'  # Directory to save downloaded files
    
    # Step 1: Load the modules page directly
    modules_page_html = load_modules_page(session, modules_url)
    
    # Step 2: Download files from the Modules page
    download_files_from_modules(session, modules_url, download_dir, base_url)

# Execute the ETL process
if __name__ == "__main__":
    etl_process()
