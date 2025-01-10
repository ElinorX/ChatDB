# Natural Language Database Query System

A database system that supports natural language queries, compatible with both SQL and NoSQL databases.

## Requirements

1. Python 3.11+
2. MySQL 5.7+
3. MongoDB 4.0+
4. Required Python packages：
   ```bash
   pip install mysql-connector-python
   pip install pymongo
   pip install pandas
   pip install colorama
   ```

## Database Configuration

1.MySQL Configuration
   ```python
   SQL_CONFIG = {
       "host": "127.0.0.1",
       "port": 3306,        # Adjust according to your MySQL port
       "user": "root",      # Replace with your MySQL username
       "password": "12345678", # Replace with your MySQL password
       "database": "selected_data"
   }
   ```

2. MongoDB Configuration
   ```python
   MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
   MONGO_DATABASE = "selected_data"
   ```

## Startup Steps

1. Ensure data files are ready:
   - Place all CSV files in the archive directory.
   - Supported data files:
     * Air Conditioners.csv
     * All Appliances.csv
     * All Car and Motorbike Products.csv

2. Start the program:
   ```bash
   python main.py
   ```

3. Select the database type:
   - Input `SQL` or `NoSQL`
   - Input `exit` to quit the program.

4. Initialize the database:
   - Enter `yes` to initialize on the first use.
   - Enter `no` to skip initialization for subsequent uses.

## User Guide

1. Basic Commands:
   - `help`: Display help information.
   - `exit`: Return to the database selection interface.

2. Query Syntax:
   ```
   show me [category] + [conditions] + [sorting] + [limit]
   ```

3. Data Categories:
   - air conditioners
   - appliances
   - car and motorbike products

4. SQL-Specific Features:
   - Price analysis
   - Discount calculations
   - Price sorting
   - Complex statistics

5. NoSQL-Specific Features:
   - Rating analysis
   - Comment statistics
   - Category statistics

## Query Examples

### SQL Query Examples

1. Basic Queries:
```sql
# View basic data
show me appliances limit 10 records

# Sort by price
show me air conditioners in ascending price limit 15 records

# Query within a price range
show me car and motorbike products with price between 10000 and 50000 limit 20 records
```

2. Price Analysis:
```sql
# High-end products
show me appliances with price greater than 20000 in descending price limit 10 records

# Budget-friendly products
show me air conditioners with price between 5000 and 15000 in ascending price limit 15 records

# Price statistics
show price range for appliances
show maximum price for air conditioners
show minimum price for car and motorbike products
```

3. Discount Analysis:
```sql
# Products with large discounts:
show me appliances with discount greater than 30 in descending price limit 10 records

# Cost-effective products:
show me air conditioners with price between 10000 and 30000 and discount greater than 25 limit 15 records

# Discount statistics:
show average discount for appliances
```

4. Ratings and Comment Analysis:
```sql
# High-rated products:
show me appliances with rating greater than 4.5 limit 10 records

# Popular products:
show me air conditioners with comments greater than 1000 limit 15 records

# Rating statistics:
show rating statistics for appliances
show average rating for air conditioners group by category
```

5. Complex Combined Queries:
```sql
# High-end, high-rated products:
show me appliances with price greater than 30000 and rating greater than 4.5 in descending price limit 10 records

# Popular budget-friendly products:
show me air conditioners with price less than 20000 and comments greater than 1000 in ascending price limit 15 records

# Highly discounted popular products:
show me appliances with discount greater than 30 and comments greater than 1000 in descending price limit 20 records
```

6. Category Statistics:
```sql
# Price range statistics:
show total number of appliances with price between 5000 and 20000 group by category

# High-rated product statistics:
show total number of air conditioners with rating greater than 4 group by category

# Discounted product statistics:
show total number of appliances with discount greater than 25 group by category
```

### NoSQL Query Examples

1. Basic Queries:
```NoSql
# View basic data
show me appliances limit 10 records
show me air conditioners limit 15 records
show me car and motorbike products limit 20 records
```

2. Rating Analysis:
```NoSql
# High-rated products
show me appliances with rating greater than 4.5 limit 10 records

# Rating statistics
show total number of appliances with rating greater than 4.5 group by category
show average rating for air conditioners group by category
```

3. Comment Analysis:
```NoSql
# Popular products
show me appliances with comments greater than 5000 limit 10 records

# Comment statistics
show total number of air conditioners with comments greater than 2000 group by category
```

4. Combined Conditions:
```NoSql
# High-rated and popular
show me appliances with rating greater than 4.3 and comments greater than 3000 limit 15 records

# Popular items by category
show total number of appliances with comments greater than 5000 group by category
```

5. Category Statistics:
```NoSql
# Basic category statistics
show total number of appliances group by category
show total number of air conditioners group by category

# Conditional category statistics
show total number of appliances with rating greater than 4 group by category
show total number of air conditioners with comments greater than 1000 group by category
```

## Advanced Features

### SQL Advanced Features:
1. Price Analysis:
   - Support for price range queries.
   - Maximum/minimum price statistics.
   - Price range distribution analysis.

2. Discount Analysis:
   - Discount rate calculation.
   - Filter products by discount.
   - Average discount statistics.

3. Complex Statistics:
   - Support for multi-condition queries.
   - Price sorting.
   - Category summarization.

### NoSQL Advanced Features:
1. Rating Analysis:
   - Rating-based filtering.
   - Rating statistics.
   - Rating analysis by category.

2. Comment Analysis:
   - Comment-based filtering.
   - Popular product analysis.
   - Category-based statistics.

3. Category Statistics:
   - Basic category statistics.
   - Conditional category statistics.
   - Multi-dimensional analysis.

