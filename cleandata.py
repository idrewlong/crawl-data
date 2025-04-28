import pandas as pd
import os
import re
from urllib.parse import urlparse

def clean_filename(url):
    """Convert URL to a clean filename"""
    parsed = urlparse(url)
    hostname = parsed.netloc
    path = parsed.path.strip('/')
    
    if not path:
        return hostname.replace('.', '_')
    
    # Remove special characters and replace with underscores
    filename = re.sub(r'[^\w\-]', '_', f"{hostname}_{path}")
    # Limit length to avoid very long filenames
    if len(filename) > 100:
        filename = filename[:100]
    return filename

def organize_luxury_data(input_file='luxury_data.csv', 
                         output_directory='organized_data',
                         create_individual_files=True,
                         organize_main_csv=True):
    """
    Organizes the luxury data CSV into a well-structured format
    
    Args:
        input_file: Path to the input CSV file
        output_directory: Directory to save organized files
        create_individual_files: Whether to create individual CSVs for each page
        organize_main_csv: Whether to create a single organized CSV with all data
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        # Clean column names (remove any leading/trailing whitespace)
        df.columns = df.columns.str.strip()
        
        # Fill missing values with empty strings
        df = df.fillna('')
        
        # Create a new column with cleaned domain and path for better organization
        df['domain'] = df['url'].apply(lambda x: urlparse(x).netloc)
        df['path'] = df['url'].apply(lambda x: urlparse(x).path)
        
        # Create a new column with a date in a cleaner format
        if 'date_crawled' in df.columns:
            try:
                df['date_crawled_formatted'] = pd.to_datetime(df['date_crawled']).dt.strftime('%Y-%m-%d')
            except:
                # If date conversion fails, keep the original
                df['date_crawled_formatted'] = df['date_crawled']
        
        # Sort the data by domain and then by path for better organization
        df = df.sort_values(['domain', 'path'])
        
        # Create a single organized CSV with all data
        if organize_main_csv:
            # Reorder columns for better organization
            ordered_columns = [
                'domain', 'path', 'url', 'title', 'meta_description', 
                'h1', 'h2', 'h3_plus', 'body_text', 
                'date_crawled', 'date_crawled_formatted', 'errors'
            ]
            
            # Only include columns that exist in the dataframe
            ordered_columns = [col for col in ordered_columns if col in df.columns]
            
            # Add any remaining columns that weren't in our predefined order
            for col in df.columns:
                if col not in ordered_columns:
                    ordered_columns.append(col)
            
            # Save the organized CSV
            df[ordered_columns].to_csv(os.path.join(output_directory, 'organized_luxury_data.csv'), index=False)
            print(f"Created organized CSV: {os.path.join(output_directory, 'organized_luxury_data.csv')}")
        
        # Create individual CSVs for each page
        if create_individual_files:
            # Create a folder for individual pages
            pages_dir = os.path.join(output_directory, 'pages')
            if not os.path.exists(pages_dir):
                os.makedirs(pages_dir)
            
            # Process each row and create a separate CSV
            for idx, row in df.iterrows():
                # Create a clean filename from the URL
                filename = clean_filename(row['url'])
                
                # Save the individual page data
                row_df = pd.DataFrame([row])
                output_file = os.path.join(pages_dir, f"{filename}.csv")
                row_df.to_csv(output_file, index=False)
            
            print(f"Created {len(df)} individual page CSVs in {pages_dir}")
        
        return True
    
    except Exception as e:
        print(f"Error organizing data: {str(e)}")
        return False

def main():
    """Main function to run the data organizer"""
    print("Luxury Data Organizer")
    print("====================")
    
    # Define default values
    input_file = 'luxury_data.csv'
    output_directory = 'organized_data'
    
    # Check if input file exists
    if not os.path.exists(input_file):
        input_file = input("Enter the path to your luxury_data.csv file: ")
        if not os.path.exists(input_file):
            print(f"Error: Could not find file {input_file}")
            return
    
    # Ask for organization options
    print("\nHow would you like to organize the data?")
    print("1. Create a single organized CSV file")
    print("2. Create individual CSV files for each page")
    print("3. Both options (default)")
    choice = input("Enter your choice (1-3): ").strip() or "3"
    
    create_individual_files = choice in ["2", "3"]
    organize_main_csv = choice in ["1", "3"]
    
    # Run the organization
    success = organize_luxury_data(
        input_file=input_file, 
        output_directory=output_directory,
        create_individual_files=create_individual_files,
        organize_main_csv=organize_main_csv
    )
    
    if success:
        print("\nData organization completed successfully!")
        print(f"Organized files are saved in the '{output_directory}' directory")
    else:
        print("\nData organization failed. Please check the input file and try again.")

if __name__ == "__main__":
    main()