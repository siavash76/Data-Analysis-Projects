# 🌍 Web Scraping GDP Data — Practice Project

This project is part of the IBM Data Analysis course practice labs.  
It demonstrates skills in **web scraping, data cleaning, and transformation** using Python.

## 🔹 Overview
Extracted GDP data from a Wikipedia page (archived version), cleaned and transformed it using **Pandas** and **NumPy**, and exported the top 10 largest economies to CSV.

## 🔹 Steps
1. Scraped HTML tables from the Wikipedia page using `pd.read_html`.
2. Selected relevant columns (Country, GDP).
3. Cleaned values: removed commas, converted to integers.
4. Converted GDP from millions → billions (rounded to 2 decimals).
5. Exported final dataset to `Largest_economies.csv`.

## 🔹 Output
Top 10 largest economies in a CSV File

## 🔹 Skills Demonstrated
- Web scraping with Pandas
- Data cleaning & transformation
- NumPy calculations
- CSV export