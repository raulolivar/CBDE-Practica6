from neo4j import GraphDatabase
import datetime as dt
import random

name = ["Ignasi", "Victor", "Lamine Yamal", "Gerard", "Marc", "Fermin Lopez", "Pau Cubarsi", "Raphinha", "Robert Lewandowski", "Gavi"]
address = ["Barcelona", "Tremp", "Tarrega", "Badalona", "Girona", "Tarragona", "Santa Coloma", "Sort", "Wisconsin", "Lleida"]
nation = ["Catalunya", "Paisos Catalans", "Republica Catalana", "Unio de Republiques Socialistes Catalanes", "Corona Aragonesa", "EEUU", "UAE", "Euskadi", "Wisconsin", "Andalusia"]
flag = ['A', 'C', 'A', 'B', 'C', 'B', 'A', 'B', 'C', 'C']
price = [4505, 32243, 12512, 34800, 21657, 54347, 97562, 31252, 26347, 11463601]
region = ["Barcelona", "Girona", "Tarragona", "Lleida", "Catalunya Nord", "Valencia", "Madrid", "Saragossa", "Wisconsin", "Utah"]
brand = ["Mango", "CaixaBank", "Naturgy", "Glovo", "EstrellaDamm", "Moritz", "HM", "Deliveroo", "BBVA", "Iberdrola"]

def create_database(session):
    session.run("MATCH (n) DETACH DELETE n") # Borrar nodes BD

    create_part_nodes(session)
    create_supp_nodes(session)
    create_partsupp_nodes(session)
    create_nation_nodes(session)
    create_order_nodes(session)
    create_customer_nodes(session)
    create_lineitem_nodes(session)
    create_relations(session)

def create_part_nodes(session):
    for i in 10: 
        session.run("CREATE (part" + i + ": Part{p_partkey: " + i + ", p_name: 'Partkey" + i + "'"
        ", p_mfgr: 'AEIOU', p_brand '" + brand[i] + "', p_type: 'Cotxe'" + 
        ", p_size: " + str(random.randint(1,5)) + ", p_container: 'Box" + i + "'"
        ", p_retailprice: " + str(float(random.randint(1200, 4500) / 100)) + 
        ", p_comment: 'Todo mal'})")

def create_supp_nodes(session):
    for i in 10:
        session.run("CREATE (supp" + str(i) + ": Supplier {s_suppkey: " + str(i) + 
            ", s_name: 'Suppkey" + str(i) + "', " +
            "s_address: '" + address[i] + "', " +
            "s_phone: '" + str(random.randint(600000000, 699999999)) + "', " +
            "s_acctbal: " + str(random.randint(1000, 5000)) + ", " +
            "s_comment: 'Todo bien'})")


def create_partsupp_nodes(session):
    for i in 10:
        session.run("CREATE (partsupp" + i + ": PartSupp{ps_partkey: " + i + ", ps_suppkey: " + i +
                    ", ps_availqty: " + str(random.randint(150, 460)) +
                    ", ps_supplycost: " + str(float(random.randint(250, 750) / 100)) + ", ps_comment: 'Seguimos mal'})")

    session.run("CREATE INDEX ON: PartSupp(ps_supplycost)")

def create_nation_nodes(session):
    for i in 10:
          session.run("CREATE (nation" + i + ": Nation{n_nationkey: " + i + ", n_name: '" + nation[i] + "'"
                    ", n_comment: 'Seguimos bien'})") 

def create_region_nodes(session):
    for i in 10:
        session.run("CREATE (region" + i + ": Region{r_regionkey: " + i + ", r_name: '" + region[i] + "'"
                    ", r_comment: 'De mal en peor'})")

def create_order_nodes(session):
    for i in 10:
        session.run("CREATE (order" + i + ": Order{o_orderkey: " + i + ", o_orderstatus: 'Z" + "'"
                    ", o_totalprice: " + price[i] + ", o_orderdate: '" + dt.datetime(2024, 5, i) +
                    "', o_orderpriority: 'H" + 
                    "', o_clerk: '" + "Oscar" +
                    "', o_shippriority: '" + str(random.randint(1, 20)) +
                    "', o_comment: 'Lookin good'})")

def create_customer_nodes(session):
    for i in 10:
        session.run("CREATE (customer" + i + ": Customer{c_custkey: " + i + ", c_name: '" + name[i] +
                    "', c_address: '" + address[i] + "', c_phone: " + str(random.randint(600000000, 699999999)) +
                    ", c_acctbal: " + str(random.random()) +
                    ", c_mktsegment: 'Automobile', s_comment: 'Todo bien'})")

def create_lineitem_nodes(session):
    for i in 10:
        session.run("CREATE (lineitem" + i + ": Lineitem{l_linenumber: " + i + ", l_quantity: " + str(random.randint(15, 50)) +
                    ", l_extendedprice: " + price[i] + ", l_discount: " + str(random.randint(0,100) / 100) +
                    ", l_tax: " + str(0.21) +
                    ", l_returnflag: '" + flag[i] +
                    "', l_linestatus: '" + flag[i] +
                    "', l_shipdate: '" + str(dt.datetime(2024, 5, i+1)) +
                    "', l_commitdate: '" + str(dt.datetime(2024, 5, i+2)) +
                    "', l_receiptdate: '" + str(dt.datetime(2024, 5, i+3)) +
                    "', l_shipinstruct: 'Fast af boi" + 
                    "', l_shipmode: '" + flag[i] + 
                    "', l_comment: 'Las cosas no van tan mal por aqui'})")

def create_relations(session):
    # PART TO PARTSUPP
    session.run("MATCH (part1: Part{p_partkey: 1}), (partsupp1: PartSupp{ps_suppkey: 1, ps_partkey: 1}) "
                "CREATE (part1) -[:BELONGS_TO]-> (partsupp1)")
    session.run("MATCH (part2: Part{p_partkey: 2}), (partsupp2: PartSupp{ps_suppkey: 2, ps_partkey: 2}) "
                "CREATE (part2) -[:BELONGS_TO]-> (partsupp2)")
    session.run("MATCH (part3: Part{p_partkey: 3}), (partsupp3: PartSupp{ps_suppkey: 3, ps_partkey: 3}) "
                "CREATE (part3) -[:BELONGS_TO]-> (partsupp3)")
    session.run("MATCH (part4: Part{p_partkey: 4}), (partsupp4: PartSupp{ps_suppkey: 4, ps_partkey: 4}) "
                "CREATE (part4) -[:BELONGS_TO]-> (partsupp4)")
    session.run("MATCH (part5: Part{p_partkey: 5}), (partsupp5: PartSupp{ps_suppkey: 5, ps_partkey: 5}) "
                "CREATE (part5) -[:BELONGS_TO]-> (partsupp5)")
    session.run("MATCH (part6: Part{p_partkey: 6}), (partsupp6: PartSupp{ps_suppkey: 6, ps_partkey: 6}) "
                "CREATE (part6) -[:BELONGS_TO]-> (partsupp6)")
    session.run("MATCH (part7: Part{p_partkey: 7}), (partsupp7: PartSupp{ps_suppkey: 7, ps_partkey: 7}) "
                "CREATE (part7) -[:BELONGS_TO]-> (partsupp7)")
    session.run("MATCH (part8: Part{p_partkey: 8}), (partsupp8: PartSupp{ps_suppkey: 8, ps_partkey: 8}) "
                "CREATE (part8) -[:BELONGS_TO]-> (partsupp8)")
    session.run("MATCH (part9: Part{p_partkey: 9}), (partsupp9: PartSupp{ps_suppkey: 9, ps_partkey: 9}) "
                "CREATE (part9) -[:BELONGS_TO]-> (partsupp9)")
    session.run("MATCH (part10: Part{p_partkey: 10}), (partsupp5: PartSupp{ps_suppkey: 10, ps_partkey: 10}) "
                "CREATE (part10) -[:BELONGS_TO]-> (partsupp10)")

def query1(session, date):
    return 0

def query2(session, size, type, region):
    return 0

def query3(session, segment, date1, date2):
    return 0

def query4(session, date, region):
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
    driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "Practica6"))
    session = driver.session()
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

            query1_results = query1(session,
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

            query2_results = query2(session,
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

            query3_results = query3(session,
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

            query4_results = query4(session,
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
