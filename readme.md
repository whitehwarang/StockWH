Stock Data Warehouse with Python + PostgreSQL.

# initialize.
1. Table Creation
2. Connect to Creon Plus server.
3. update the stock & future & option data with the code below.
    import DB; DB.update();

# how to use.
4. daychart: pd.DataFrame = StockWH.Stock.S11_DAYCHART.read()

Caution!
Some python packages like "DTC" may not be contained within this python package.
The "DTC" is a private package that controls DateTime.
But Anyone can guess it's implementation as you read name of the function inside.
