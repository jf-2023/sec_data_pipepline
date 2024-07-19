# Securities and Exchange Commission (SEC) Data Processing Pipeline

This project processes and analyzes financial data from various companies. It includes data fetching, cleaning, processing, and visualization.

## Features

- Fetches financial data from the SEC API
- Cleans and processes financial data
- Returns a pandas DataFrame with 10-K data
- Benchmarks data fetching and cleaning methods
- Includes tests for data fetching and cleaning

## File Structure

### `main.py`

Executes the core functionality of the application. It returns a pandas DataFrame with 10-K data fetched from the SEC API. Should work out of the box for anyone, just be sure to put your own email in the headers section for using the API request.

### `scripts/`

Contains different methods for benchmarking data fetching and data cleaning.

### `test/`

Includes tests for the data fetching and data cleaning functions in the `scripts` directory.

**NOTE:** Unlike `main.py`, the `scripts` and `test` directories will only work for those who have downloaded the `companyfacts.zip` file provided by the SEC at the bottom of the page [here](https://www.sec.gov/search-filings/edgar-application-programming-interfaces). Be sure to change the directory to where you stored your zip file. It is recommended to extract the zip file beforehand to speed up loading times.

## Usage

1. Clone the repository:
    ```sh
    git clone https://github.com/jf-2023/sec_data_pipeline.git
    ```
2. Navigate to the project directory:
    ```sh
    cd sec_data_pipeline
    ```
3. Install the required dependencies:
    ```sh
    pip install -r requirements.txt
    ```
4. Run the main script:
    ```sh
    python main.py
    ```

## SEC API Documentation

For more information on SEC API usage, visit the [SEC EDGAR API documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).

## Electronic Data Gathering, Analysis, and Retrieval system (EDGAR)

For more information on EDGAR, visit the [EDGAR about page](https://www.sec.gov/submit-filings/about-edgar).