import re
import PyPDF2
import pandas as pd
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

# Path to the PDF file
pdf_path = '/Users/razvanstanciu/Downloads/climatechange_key-terms_bookmarks.pdf'

# List of country codes to process
country_codes = [
    "BG", "CS", "DA", "DE", "EL", "ES", "ET", "FI", "FR", "GA", "HU", "IT",
    "LT", "LV", "MT", "NL", "PL", "PT", "RO", "SK", "SL", "SV"
]

# Create a regex pattern for the country codes
country_code_pattern = "|".join(country_codes)

# List to store the extracted translations
translations = []

# Open the PDF file
with open(pdf_path, 'rb') as pdf_file:
    reader = PyPDF2.PdfReader(pdf_file)
    num_pages = len(reader.pages)

    for page_idx in range(num_pages - 3):  # Skip the last 3 pages
        page = reader.pages[page_idx]
        text = page.extract_text()
        lines = text.splitlines()

        current_country_code = None
        current_translation = None

        for line in lines[:-1]:  # Skip the last line of each page (likely page number)
            line = line.replace("\t", "").strip()
            if not line:
                continue

            # Check if the line starts with a country code
            match = re.match(rf"^({country_code_pattern})\s+(.*)", line)
            if match:
                # Save the current translation if exists
                if current_country_code and current_translation:
                    translations.append((current_country_code, current_translation.strip()))

                # Start a new translation
                current_country_code = match[1]
                current_translation = match[2]
            else:
                # Append to the current translation if it doesn't start with a country code
                if current_translation is not None:
                    current_translation += " " + line

        # Save the last translation on the page
        if current_country_code and current_translation:
            translations.append((current_country_code, current_translation.strip()))

# Convert the translations list to a DataFrame
df = pd.DataFrame(translations, columns=["Country Code", "Translation"])
print(df)

#Save the DataFrame to a CSV file
output_path = '/Users/razvanstanciu/PycharmProjects/MBD-Project/climate_change_terms_translations.csv'
df.to_csv(output_path, index=False)

print(f"Translations extracted and saved to {output_path}")
