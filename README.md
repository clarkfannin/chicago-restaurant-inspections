# Chicago Restaurant Health and Google Ratings Dashboard  
**Status:** In Progress  
**Last Updated:** October 2025  

This project examines the relationship between restaurant health inspection outcomes and Google Maps ratings in Chicago. It combines public inspection data from the City of Chicago with ratings data from the Google Places API to explore how food safety and customer perception intersect across neighborhoods and cuisines.  

---

## Overview  
This repository contains the full data pipeline and analysis workflow for the project. The goal is to build an automated process that updates and feeds data into an interactive Tableau dashboard for public use.  

---

## Project Pipeline  

### 1. Data Collection  
- Health inspections from the [Chicago Data Portal](https://data.cityofchicago.org)  
- Google ratings via the Google Places API  

### 2. Data Processing  
- Clean and standardize inspection results  
- Merge inspection and rating data by restaurant  
- Apply geospatial joins for neighborhood-level summaries  

### 3. Automation 
- Scheduled data refreshes using GitHub Actions  
- Automated Tableau extract generation  

### 4. Visualization (in progress)  
- Tableau dashboard with metrics on inspection outcomes, ratings, and time-based trends  

---

## Repository Structure  
/data/ all scripts for loading, cleaning, and exporting data
.github/workflows/ automation and CI/CD setup

---

## Technologies  
- **Languages:** Python, SQL  
- **Libraries:** pandas, requests, geopandas  
- **APIs:** Google Places API  
- **Visualization:** Tableau  
- **Automation:** GitHub Actions  

---

## Contact  
**Author:** Clark Fannin 
**Portfolio:** [www.clarkfannin.com  ](https://clarkfannin.com/)
**Email:** jclarkfii@gmail.com
