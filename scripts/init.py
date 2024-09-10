import requests
import os
import zipfile

# Canvas API
API_URL = 'https://canvas.lms.unimelb.edu.au/api/v1'
API_TOKEN = 'YOUR_API_TOKEN'
COURSE_ID = '188486' 

# List of Files to Download
FILE_LIST = ['project-2-bnpl-tables-part1.zip', 
    'project-2-bnpl-tables-part2.zip', 
    'project-2-bnpl-tables-part3.zip', 
    'project-2-bnpl-tables-part4.zip']

# Headers for the API request
headers = {
    'Authorization': f'Bearer {API_TOKEN}'
}

def get_files_from_course(course_id):
    # API endpoint for course files
    url = f'{API_URL}/courses/{course_id}/files'
    all_files = []
    
    while url:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            files = response.json()
            all_files.extend(files)
            
            # Check if there is a "next" page
            if 'next' in response.links:
                url = response.links['next']['url']
            else:
                url = None
        else:
            print(f'Failed to get files: {response.status_code}')
            break
    
    return all_files

def unzip_file(zip_path, extract_to):
    # Unzip the file to the specified directory
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
        print(f'File {zip_path} unzipped successfully.')
    
    # Optional: remove the ZIP file after extraction
    os.remove(zip_path)
    print(f'File {zip_path} removed after extraction.')

def download_file(file_url, file_name, download_folder):
    # Download a file from a URL
    response = requests.get(file_url, headers=headers)
    
    if response.status_code == 200:
        file_path = os.path.join(download_folder, file_name)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f'File {file_name} downloaded successfully.')
        
        # If it's a ZIP file, unzip it
        if file_name.endswith('.zip'):
            unzip_file(file_path, download_folder)
            
    else:
        print(f'Failed to download {file_name}: {response.status_code}')

def main():
    download_folder = 'data'  # Folder to save downloaded files
    
    # Create folder if it doesn't exist
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    
    # Get list of files from the course
    files = get_files_from_course(COURSE_ID)
    
    if files:
        for file in files:
            file_name = file['display_name']
            
            # Only download if the file is in the specific list
            if file_name in FILE_LIST:
                file_url = file['url']
                download_file(file_url, file_name, download_folder)
            
if __name__ == '__main__':
    main()
