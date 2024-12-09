from neo4j import GraphDatabase
import datetime as dt

def create_database():
    return 0

def is_valid_date(date_str):
    """
    Checks if the provided date string is in the valid YYYY-MM-DD format.
    Returns True if valid, otherwise False.
    """
    try:
        year, month, day = map(int, date_str.split('-'))
        dt.datetime(year, month, day)
        return True
    except ValueError:
        return False

def start_program():
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "bolt://localhost:7687"
    AUTH = ("neo4j", "neo4j")

    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
    driver = GraphDatabase.driver(URI, auth=AUTH)
    session = driver.session
    create_database(session)
    
    print("Please choose an operation:\n",
          "1. Run the first query\n",
          "2. Run the second query\n",
          "3. Run the third query\n",
          "4. Run the fourth query\n",
          "0. Exit the program")

    user_choice = input("Your selection: ")
    user_choice = int(user_choice)

    while user_choice != 0:

        if user_choice == 1:
            ship_date = input("Enter the shipment date in YYYY-MM-DD format: ")
            while not is_valid_date(ship_date):
                ship_date = input("Invalid format! Please enter a valid shipment date (YYYY-MM-DD): ")

            query1_results = query1(database["orders"],
                                    dt.datetime.strptime(ship_date, "%Y-%m-%d"))

            print("\nResults for Query 1:")
            for result in query1_results:
                print(result)

        elif user_choice == 2:
            part_size = input("Enter the size of the part: ")
            while not part_size.isdigit():
                part_size = input("Invalid input. Please enter a valid part size: ")

            part_type = input("Enter the type of the part: ")
            region_name = input("Enter the name of the region: ")

            query2_results = query2(database["partsupp"],
                                    int(part_size),
                                    str(part_type),
                                    str(region_name))

            print("\nResults for Query 2:")
            for result in query2_results:
                print(result)

        elif user_choice == 3:
            market_segment = input("Enter the customer market segment: ")

            order_date1 = input("Enter the order date (YYYY-MM-DD): ")
            while not is_valid_date(order_date1):
                order_date1 = input("Invalid format! Please enter a valid order date (YYYY-MM-DD): ")

            ship_date2 = input("Enter the shipment date (YYYY-MM-DD): ")
            while not is_valid_date(ship_date2):
                ship_date2 = input("Invalid format! Please enter a valid shipment date (YYYY-MM-DD): ")

            query3_results = query3(database["orders"],
                                    str(market_segment),
                                    dt.datetime.strptime(order_date1, "%Y-%m-%d"),
                                    dt.datetime.strptime(ship_date2, "%Y-%m-%d"))

            print("\nResults for Query 3:")
            for result in query3_results:
                print(result)

        elif user_choice == 4:
            order_date = input("Enter the order date (YYYY-MM-DD): ")
            while not is_valid_date(order_date):
                order_date = input("Invalid format! Please enter a valid order date (YYYY-MM-DD): ")

            region = input("Enter the region name: ")

            query4_results = query4(database["orders"],
                                    dt.datetime.strptime(order_date, "%Y-%m-%d"),
                                    region)

            print("\nResults for Query 4:")
            for result in query4_results:
                print(result)

        print("\nPlease choose an operation:\n",
              "1. Run the first query\n",
              "2. Run the second query\n",
              "3. Run the third query\n",
              "4. Run the fourth query\n",
              "0. Exit the program")

        user_choice = input("Your selection: ")
        user_choice = int(user_choice)



if __name__ == "__main__":
    start_program()
