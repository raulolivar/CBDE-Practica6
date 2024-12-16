from neo4j import GraphDatabase
import datetime as dt
import random

name = ["Ignasi", "Victor", "Lamine Yamal", "Gerard", "Marc", "Fermin Lopez", "Pau Cubarsi", "Raphinha", "Robert Lewandowski", "Gavi"]
address = ["Barcelona", "Tremp", "Tarrega", "Badalona", "Girona", "Tarragona", "Santa Coloma", "Sort", "Wisconsin", "Lleida"]
flag = ['A', 'B', 'C']
price = [4505, 32243, 12512, 34800, 21657, 54347, 97562, 31252, 26347, 11463601]
nation = ["Catalunya", "Paisos Catalans", "Republica Catalana"]
region = ["Barcelona", "Girona", "Tarragona"]
brand = ["Mango", "CaixaBank", "Naturgy", "Glovo", "EstrellaDamm", "Moritz", "HM", "Deliveroo", "BBVA", "Iberdrola"]

def create_database(session):
    session.run("MATCH (n) DETACH DELETE n") # Borrar nodes BD

    create_part_nodes(session)
    create_supp_nodes(session)
    create_partsupp_nodes(session)
    create_nation_nodes(session)
    create_region_nodes(session)
    create_order_nodes(session)
    create_customer_nodes(session)
    create_lineitem_nodes(session)
    create_relations(session)

def create_part_nodes(session):
    for i in range(0, 10):
        session.run("CREATE (part" + str(i) + ": Part{p_partkey: " + str(i) + ", p_name: 'Partkey" + str(i) + "'"
        ", p_mfgr: 'AEIOU', p_brand: '" + brand[i] + "', p_type: '" + flag[random.randint(0,2)] + "'" 
        ", p_size: " + str(random.randint(1,2)) + ", p_container: 'Box" + str(i) + "'"
        ", p_retailprice: " + str(float(random.randint(1200, 4500) / 100)) + 
        ", p_comment: 'Todo mal'})")

def create_supp_nodes(session):
    for i in range(0, 10):
        session.run("CREATE (supp" + str(i) + ": Supplier {s_suppkey: " + str(i) + 
            ", s_name: 'Suppkey" + str(i) + "', " +
            "s_address: '" + address[i] + "', " +
            "s_phone: '" + str(random.randint(600000000, 699999999)) + "', " +
            "s_acctbal: " + str(random.randint(1000, 5000)) + ", " +
            "s_comment: 'Todo bien'})")


def create_partsupp_nodes(session):
    for i in range(0, 10):
        session.run("CREATE (partsupp" + str(i) + ": PartSupp{ps_partkey: 'partSupp" + str(i) + "', ps_suppkey: " + str(i) +
                    ", ps_availqty: " + str(random.randint(150, 460)) +
                    ", ps_supplycost: " + str(float(random.randint(250, 750) / 100)) + ", ps_comment: 'Seguimos mal'})")

    existing_indexes = session.run("SHOW INDEXES;")
    index_exists = any("ps_supplycost" in record["name"] for record in existing_indexes)

    if not index_exists:
        session.run("CREATE INDEX ps_supplycost_index FOR (n:PartSupp) ON (n.ps_supplycost)")
    else:
        print("Index for ps_supplycost already exists. Skipping creation.")

def create_nation_nodes(session):
    for i in range(0, 3):
          session.run("CREATE (nation" + str(i) + ": Nation{n_nationkey: " + str(i) + ", n_name: '" + nation[i] + "'"
                    ", n_comment: 'Seguimos bien'})") 

def create_region_nodes(session):
    for i in range(0, 3):
        session.run("CREATE (region" + str(i) + ": Region{r_regionkey: " + str(i) + ", r_name: '" + region[i] + "'"
                    ", r_comment: 'De mal en peor'})")

def create_order_nodes(session):
    for i in range(0, 10):
        session.run("CREATE (order" + str(i) + ": Order{o_orderkey: 'order" + str(i) + "', o_orderstatus: 'Z" + "'"
                    f", o_totalprice: {price[i]}" + f", o_orderdate: '{dt.datetime(2015, 5, i+1).date()}', o_orderpriority: 'H" + 
                    "', o_clerk: '" + "Oscar" +
                    "', o_shippriority: '" + str(random.randint(1, 20)) +
                    "', o_comment: 'Lookin good'})")

def create_customer_nodes(session):
    for i in range(0, 10):
        session.run("CREATE (customer" + str(i) + ": Customer{c_custkey: " + str(i) + ", c_name: '" + name[i] +
                    "', c_address: '" + address[i] + "', c_phone: " + str(random.randint(600000000, 699999999)) +
                    ", c_acctbal: " + str(random.random()) +
                    ", c_mktsegment: 'seg_x', s_comment: 'Todo bien'})")

def create_lineitem_nodes(session):
    for i in range(0, 10):
        session.run("CREATE (lineitem" + str(i) + ": Lineitem{l_linenumber: " + str(i) + ", l_quantity: " + str(random.randint(15, 50)) +
                    f", l_extendedprice: {price[i]}, l_discount: " + str(random.randint(0,100) / 100) +
                    ", l_tax: " + str(0.21) +
                    ", l_returnflag: '" + flag[random.randint(0,2)] +
                    "', l_linestatus: '" + flag[random.randint(0,2)] +
                    "', l_shipdate: '" + str(dt.datetime(random.randint(2018,2021), random.randint(1,12), i+1).date()) +
                    "', l_commitdate: '" + str(dt.datetime(2024, 5, i+2).date()) +
                    "', l_receiptdate: '" + str(dt.datetime(2024, 7, i+3).date()) +
                    "', l_shipinstruct: 'Fast af boi" + 
                    "', l_shipmode: '" + flag[random.randint(0,2)] + 
                    "', l_comment: 'Las cosas no van tan mal por aqui'})")

def create_relations(session):
    # PART TO PARTSUPP
    session.run("MATCH (part0: Part{p_partkey: 0}), (partsupp0: PartSupp{ps_suppkey: 0, ps_partkey: 'partSupp0'}) "
                "CREATE (part0) -[:BELONGS_TO]-> (partsupp0)")
    session.run("MATCH (part1: Part{p_partkey: 1}), (partsupp1: PartSupp{ps_suppkey: 1, ps_partkey: 'partSupp1'}) "
                "CREATE (part1) -[:BELONGS_TO]-> (partsupp1)")
    session.run("MATCH (part2: Part{p_partkey: 2}), (partsupp2: PartSupp{ps_suppkey: 2, ps_partkey: 'partSupp2'}) "
                "CREATE (part2) -[:BELONGS_TO]-> (partsupp2)")
    session.run("MATCH (part3: Part{p_partkey: 3}), (partsupp3: PartSupp{ps_suppkey: 3, ps_partkey: 'partSupp3'}) "
                "CREATE (part3) -[:BELONGS_TO]-> (partsupp3)")
    session.run("MATCH (part4: Part{p_partkey: 4}), (partsupp4: PartSupp{ps_suppkey: 4, ps_partkey: 'partSupp4'}) "
                "CREATE (part4) -[:BELONGS_TO]-> (partsupp4)")
    session.run("MATCH (part5: Part{p_partkey: 5}), (partsupp5: PartSupp{ps_suppkey: 5, ps_partkey: 'partSupp5'}) "
                "CREATE (part5) -[:BELONGS_TO]-> (partsupp5)")
    session.run("MATCH (part6: Part{p_partkey: 6}), (partsupp6: PartSupp{ps_suppkey: 6, ps_partkey: 'partSupp6'}) "
                "CREATE (part6) -[:BELONGS_TO]-> (partsupp6)")
    session.run("MATCH (part7: Part{p_partkey: 7}), (partsupp7: PartSupp{ps_suppkey: 7, ps_partkey: 'partSupp7'}) "
                "CREATE (part7) -[:BELONGS_TO]-> (partsupp7)")
    session.run("MATCH (part8: Part{p_partkey: 8}), (partsupp8: PartSupp{ps_suppkey: 8, ps_partkey: 'partSupp8'}) "
                "CREATE (part8) -[:BELONGS_TO]-> (partsupp8)")
    session.run("MATCH (part9: Part{p_partkey: 9}), (partsupp9: PartSupp{ps_suppkey: 9, ps_partkey: 'partSupp9'}) "
                "CREATE (part9) -[:BELONGS_TO]-> (partsupp9)")

    # PARTSUPP --> SUPPLIER
    session.run("MATCH (partsupp0: PartSupp{ps_suppkey: 0, ps_partkey: 'partSupp0'}), (supp0: Supplier{s_suppkey: 0}) "
                "CREATE (partsupp0) -[:BELONGS_TO]-> (supp0)")
    session.run("MATCH (partsupp1: PartSupp{ps_suppkey: 1, ps_partkey: 'partSupp1'}), (supp1: Supplier{s_suppkey: 1}) "
                "CREATE (partsupp1) -[:BELONGS_TO]-> (supp1)")
    session.run("MATCH (partsupp2: PartSupp{ps_suppkey: 2, ps_partkey: 'partSupp2'}), (supp2: Supplier{s_suppkey: 2}) "
                "CREATE (partsupp2) -[:BELONGS_TO]-> (supp2)")
    session.run("MATCH (partsupp3: PartSupp{ps_suppkey: 3, ps_partkey: 'partSupp3'}), (supp3: Supplier{s_suppkey: 3}) "
                "CREATE (partsupp3) -[:BELONGS_TO]-> (supp3)")
    session.run("MATCH (partsupp4: PartSupp{ps_suppkey: 4, ps_partkey: 'partSupp4'}), (supp4: Supplier{s_suppkey: 4}) "
                "CREATE (partsupp4) -[:BELONGS_TO]-> (supp4)")
    session.run("MATCH (partsupp5: PartSupp{ps_suppkey: 5, ps_partkey: 'partSupp5'}), (supp5: Supplier{s_suppkey: 5}) "
                "CREATE (partsupp5) -[:BELONGS_TO]-> (supp5)")
    session.run("MATCH (partsupp6: PartSupp{ps_suppkey: 6, ps_partkey: 'partSupp6'}), (supp6: Supplier{s_suppkey: 6}) "
                "CREATE (partsupp6) -[:BELONGS_TO]-> (supp6)")
    session.run("MATCH (partsupp7: PartSupp{ps_suppkey: 7, ps_partkey: 'partSupp7'}), (supp5: Supplier{s_suppkey: 5}) "
                "CREATE (partsupp7) -[:BELONGS_TO]-> (supp7)")
    session.run("MATCH (partsupp8: PartSupp{ps_suppkey: 8, ps_partkey: 'partSupp8'}), (supp5: Supplier{s_suppkey: 5}) "
                "CREATE (partsupp8) -[:BELONGS_TO]-> (supp8)")
    session.run("MATCH (partsupp9: PartSupp{ps_suppkey: 9, ps_partkey: 'partSupp9'}), (supp5: Supplier{s_suppkey: 5}) "
                "CREATE (partsupp9) -[:BELONGS_TO]-> (supp9)")

    # SUPPLIER --> NATION
    session.run("MATCH (supp0: Supplier{s_suppkey: 0}), (nation0: Nation{n_nationkey: 0}) "
                "CREATE (supp0) -[:BELONGS_TO]-> (nation0)")
    session.run("MATCH (supp1: Supplier{s_suppkey: 1}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (supp1) -[:BELONGS_TO]-> (nation1)")
    session.run("MATCH (supp2: Supplier{s_suppkey: 2}), (nation2: Nation{n_nationkey: 2}) "
                "CREATE (supp2) -[:BELONGS_TO]-> (nation2)")
    session.run("MATCH (supp3: Supplier{s_suppkey: 3}), (nation0: Nation{n_nationkey: 0}) "
                "CREATE (supp3) -[:BELONGS_TO]-> (nation0)")
    session.run("MATCH (supp4: Supplier{s_suppkey: 4}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (supp4) -[:BELONGS_TO]-> (nation1)")
    session.run("MATCH (supp5: Supplier{s_suppkey: 5}), (nation2: Nation{n_nationkey: 2}) "
                "CREATE (supp5) -[:BELONGS_TO]-> (nation2)")
    session.run("MATCH (supp6: Supplier{s_suppkey: 6}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (supp6) -[:BELONGS_TO]-> (nation1)")
    session.run("MATCH (supp7: Supplier{s_suppkey: 7}), (nation0: Nation{n_nationkey: 0}) "
                "CREATE (supp7) -[:BELONGS_TO]-> (nation0)")
    session.run("MATCH (supp8: Supplier{s_suppkey: 8}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (supp8) -[:BELONGS_TO]-> (nation1)")
    session.run("MATCH (supp9: Supplier{s_suppkey: 9}), (nation2: Nation{n_nationkey: 2}) "
                "CREATE (supp9) -[:BELONGS_TO]-> (nation2)")

    # NATION --> REGION
    session.run("MATCH (nation0: Nation{n_nationkey: 0}), (region0: Region{r_regionkey: 0}) "
                "CREATE (nation0) -[:BELONGS_TO]-> (region0)")
    session.run("MATCH (nation1: Nation{n_nationkey: 1}), (region1: Region{r_regionkey: 1}) "
                "CREATE (nation1) -[:BELONGS_TO]-> (region1)")
    session.run("MATCH (nation2: Nation{n_nationkey: 2}), (region2: Region{r_regionkey: 2}) "
                "CREATE (nation2) -[:BELONGS_TO]-> (region2)")

    # CUSTOMER --> NATION
    session.run("MATCH (customer0: Customer{c_custkey: 0}), (nation0: Nation{n_nationkey: 0}) "
                "CREATE (customer0) -[:BELONGS_TO]-> (nation0)")
    session.run("MATCH (customer1: Customer{c_custkey: 1}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (customer1) -[:BELONGS_TO]-> (nation1)")
    session.run("MATCH (customer2: Customer{c_custkey: 2}), (nation2: Nation{n_nationkey: 2}) "
                "CREATE (customer2) -[:BELONGS_TO]-> (nation2)")
    session.run("MATCH (customer3: Customer{c_custkey: 3}), (nation0: Nation{n_nationkey: 0}) "
                "CREATE (customer3) -[:BELONGS_TO]-> (nation0)")
    session.run("MATCH (customer4: Customer{c_custkey: 4}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (customer4) -[:BELONGS_TO]-> (nation1)")
    session.run("MATCH (customer5: Customer{c_custkey: 5}), (nation2: Nation{n_nationkey: 2}) "
                "CREATE (customer5) -[:BELONGS_TO]-> (nation2)")
    session.run("MATCH (customer6: Customer{c_custkey: 6}), (nation0: Nation{n_nationkey: 0}) "
                "CREATE (customer6) -[:BELONGS_TO]-> (nation0)")
    session.run("MATCH (customer7: Customer{c_custkey: 7}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (customer7) -[:BELONGS_TO]-> (nation1)")
    session.run("MATCH (customer8: Customer{c_custkey: 8}), (nation2: Nation{n_nationkey: 2}) "
                "CREATE (customer8) -[:BELONGS_TO]-> (nation2)")
    session.run("MATCH (customer9: Customer{c_custkey: 9}), (nation1: Nation{n_nationkey: 1}) "
                "CREATE (customer9) -[:BELONGS_TO]-> (nation1)")

    # CUSTOMER --> ORDER
    session.run("MATCH (customer0: Customer{c_custkey: 0}), (order0: Order{o_orderkey: 'order0'}) "
                "CREATE (customer0) -[:BELONGS_TO]-> (order0)")
    session.run("MATCH (customer1: Customer{c_custkey: 1}), (order1: Order{o_orderkey: 'order1'}) "
                "CREATE (customer1) -[:BELONGS_TO]-> (order1)")
    session.run("MATCH (customer2: Customer{c_custkey: 2}), (order2: Order{o_orderkey: 'order2'}) "
                "CREATE (customer2) -[:BELONGS_TO]-> (order2)")
    session.run("MATCH (customer3: Customer{c_custkey: 3}), (order3: Order{o_orderkey: 'order3'}) "
                "CREATE (customer3) -[:BELONGS_TO]-> (order3)")
    session.run("MATCH (customer4: Customer{c_custkey: 4}), (order4: Order{o_orderkey: 'order4'}) "
                "CREATE (customer4) -[:BELONGS_TO]-> (order4)")
    session.run("MATCH (customer5: Customer{c_custkey: 5}), (order5: Order{o_orderkey: 'order5'}) "
                "CREATE (customer5) -[:BELONGS_TO]-> (order5)")
    session.run("MATCH (customer6: Customer{c_custkey: 6}), (order6: Order{o_orderkey: 'order6'}) "
                "CREATE (customer6) -[:BELONGS_TO]-> (order6)")
    session.run("MATCH (customer7: Customer{c_custkey: 7}), (order7: Order{o_orderkey: 'order7'}) "
                "CREATE (customer7) -[:BELONGS_TO]-> (order7)")
    session.run("MATCH (customer8: Customer{c_custkey: 8}), (order8: Order{o_orderkey: 'order8'}) "
                "CREATE (customer8) -[:BELONGS_TO]-> (order8)")
    session.run("MATCH (customer9: Customer{c_custkey: 9}), (order9: Order{o_orderkey: 'order9'}) "
                "CREATE (customer9) -[:BELONGS_TO]-> (order9)")

    # ORDER --> LINEITEM
    session.run("MATCH (order0: Order{o_orderkey: 'order0'}), (lineitem0: Lineitem{l_linenumber: 0}) "
                "CREATE (order0) -[:BELONGS_TO]-> (lineitem0)")
    session.run("MATCH (order1: Order{o_orderkey: 'order1'}), (lineitem1: Lineitem{l_linenumber: 1}) "
                "CREATE (order1) -[:BELONGS_TO]-> (lineitem1)")
    session.run("MATCH (order2: Order{o_orderkey: 'order2'}), (lineitem2: Lineitem{l_linenumber: 2}) "
                "CREATE (order2) -[:BELONGS_TO]-> (lineitem2)")
    session.run("MATCH (order3: Order{o_orderkey: 'order3'}), (lineitem3: Lineitem{l_linenumber: 3}) "
                "CREATE (order3) -[:BELONGS_TO]-> (lineitem3)")
    session.run("MATCH (order4: Order{o_orderkey: 'order4'}), (lineitem4: Lineitem{l_linenumber: 4}) "
                "CREATE (order4) -[:BELONGS_TO]-> (lineitem4)")
    session.run("MATCH (order5: Order{o_orderkey: 'order5'}), (lineitem5: Lineitem{l_linenumber: 5}) "
                "CREATE (order5) -[:BELONGS_TO]-> (lineitem5)")
    session.run("MATCH (order6: Order{o_orderkey: 'order6'}), (lineitem6: Lineitem{l_linenumber: 6}) "
                "CREATE (order6) -[:BELONGS_TO]-> (lineitem6)")
    session.run("MATCH (order7: Order{o_orderkey: 'order7'}), (lineitem7: Lineitem{l_linenumber: 7}) "
                "CREATE (order7) -[:BELONGS_TO]-> (lineitem7)")
    session.run("MATCH (order8: Order{o_orderkey: 'order8'}), (lineitem8: Lineitem{l_linenumber: 8}) "
                "CREATE (order8) -[:BELONGS_TO]-> (lineitem8)")
    session.run("MATCH (order9: Order{o_orderkey: 'order9'}), (lineitem9: Lineitem{l_linenumber: 9}) "
                "CREATE (order9) -[:BELONGS_TO]-> (lineitem9)")

    # LINEITEM --> PARTSUPP
    session.run("MATCH (lineitem0: Lineitem{l_linenumber: 0}), (partsupp0: PartSupp{ps_suppkey: 0, ps_partkey: 'partSupp0'}) "
                "CREATE (lineitem0) -[:BELONGS_TO]-> (partsupp0)")
    session.run("MATCH (lineitem1: Lineitem{l_linenumber: 1}), (partsupp1: PartSupp{ps_suppkey: 1, ps_partkey: 'partSupp1'}) "
                "CREATE (lineitem1) -[:BELONGS_TO]-> (partsupp1)")
    session.run("MATCH (lineitem2: Lineitem{l_linenumber: 2}), (partsupp2: PartSupp{ps_suppkey: 2, ps_partkey: 'partSupp2'}) "
                "CREATE (lineitem2) -[:BELONGS_TO]-> (partsupp2)")
    session.run("MATCH (lineitem3: Lineitem{l_linenumber: 3}), (partsupp3: PartSupp{ps_suppkey: 3, ps_partkey: 'partSupp3'}) "
                "CREATE (lineitem3) -[:BELONGS_TO]-> (partsupp3)")
    session.run("MATCH (lineitem4: Lineitem{l_linenumber: 4}), (partsupp4: PartSupp{ps_suppkey: 4, ps_partkey: 'partSupp4'}) "
                "CREATE (lineitem4) -[:BELONGS_TO]-> (partsupp4)")
    session.run("MATCH (lineitem5: Lineitem{l_linenumber: 5}), (partsupp5: PartSupp{ps_suppkey: 5, ps_partkey: 'partSupp5'}) "
                "CREATE (lineitem5) -[:BELONGS_TO]-> (partsupp5)")
    session.run("MATCH (lineitem6: Lineitem{l_linenumber: 6}), (partsupp6: PartSupp{ps_suppkey: 6, ps_partkey: 'partSupp6'}) "
                "CREATE (lineitem6) -[:BELONGS_TO]-> (partsupp6)")
    session.run("MATCH (lineitem7: Lineitem{l_linenumber: 7}), (partsupp7: PartSupp{ps_suppkey: 7, ps_partkey: 'partSupp7'}) "
                "CREATE (lineitem7) -[:BELONGS_TO]-> (partsupp7)")
    session.run("MATCH (lineitem8: Lineitem{l_linenumber: 8}), (partsupp8: PartSupp{ps_suppkey: 8, ps_partkey: 'partSupp8'}) "
                "CREATE (lineitem8) -[:BELONGS_TO]-> (partsupp8)")
    session.run("MATCH (lineitem9: Lineitem{l_linenumber: 9}), (partsupp9: PartSupp{ps_suppkey: 9, ps_partkey: 'partSupp9'}) "
                "CREATE (lineitem9) -[:BELONGS_TO]-> (partsupp9)")

def print_all_nodes_and_relationships(session):
    # print all nodes
    print("Nodes in the database:")
    nodes_result = session.run("MATCH (n) RETURN n")
    for record in nodes_result:
        print(record["n"])
    
    # print all relationships
    # print("\nRelationships in the database:")
    # relationships_result = session.run("MATCH ()-[r]->() RETURN r")
    # for record in relationships_result:
    #     print(record["r"])

def query1(session, date):
    cypher_query = """
    MATCH (l:Lineitem)
    WHERE date(l.l_shipdate) <= date($ship_date)
    WITH l.l_returnflag AS l_returnflag, l.l_linestatus AS l_linestatus,
         sum(l.l_quantity) AS sum_qty,
         sum(l.l_extendedprice) AS sum_base_price,
         sum(l.l_extendedprice * (1 - l.l_discount)) AS sum_disc_price,
         sum(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) AS sum_charge,
         avg(l.l_quantity) AS avg_qty,
         avg(l.l_extendedprice) AS avg_price,
         avg(l.l_discount) AS avg_disc,
         count(l) AS count_order
    RETURN l_returnflag, l_linestatus, sum_qty, sum_base_price, sum_disc_price,
           sum_charge, avg_qty, avg_price, avg_disc, count_order
    ORDER BY l_returnflag, l_linestatus
    """
    return session.run(cypher_query, ship_date=date)

def query2(session, size, type, i_region):
    cypher_query = """
    MATCH (p:Part)-[:BELONGS_TO]->(ps:PartSupp)-[:BELONGS_TO]->(s:Supplier)-[:BELONGS_TO]->(n:Nation)-[:BELONGS_TO]->(r:Region)
    WHERE p.p_size = $size AND p.p_type CONTAINS $part_type AND r.r_name = $region
    WITH p, s, n, ps, r, min(ps.ps_supplycost) AS min_cost
    WHERE ps.ps_supplycost = min_cost
    RETURN s.s_acctbal AS s_acctbal, s.s_name AS s_name, n.n_name AS n_name, p.p_partkey AS p_partkey,
           p.p_mfgr AS p_mfgr, s.s_address AS s_address, s.s_phone AS s_phone, s.s_comment AS s_comment
    ORDER BY s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey
    """
    
    return session.run(cypher_query, size=size, part_type=type, region=i_region)

def query3(session, segment, date1, date2):
    cypher_query = """
    MATCH (c: Customer)-[:BELONGS_TO]->(o: Order)-[:BELONGS_TO]->(li: Lineitem) 
    WHERE li.l_shipdate > $date2 and o.o_orderdate < $date1 and c.c_mktsegment = $segment 
    RETURN o.o_orderkey AS l_orderkey, o.o_orderdate AS o_orderdate, o.o_shippriority AS o_shippriority, 
            sum(li.l_extendedprice * (1 - li.l_discount)) AS revenue 
    ORDER BY revenue DESC, o.o_orderdate
    """
    return session.run(cypher_query, segment=segment, date1=date1, date2=date2)

def query4(session, date1, date2, region):
    cypher_query = """
    MATCH (c: Customer)-[:BELONGS_TO]->(o: Order)-[:BELONGS_TO]->(li: Lineitem)-[:BELONGS_TO]->(ps: PartSupp)-[:BELONGS_TO]->(s: Supplier)-[:BELONGS_TO]->(n: Nation)-[:BELONGS_TO]->(r: Region) 
    WHERE o.o_orderdate >= $date1 and o.o_orderdate < $date2 and r.r_name = $region 
    RETURN n.n_name AS n_name, sum(li.l_extendedprice * (1 - li.l_discount)) AS revenue 
    ORDER BY revenue DESC
    """
    return session.run(cypher_query, date1=date1, date2=date2, region=region)

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
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", ""))
    session = driver.session()
    create_database(session)
    
    user_choice = -1
    while user_choice != 0:
        print("Please choose an operation:\n",
            "1. Run the first query [manual input]\n",
            "2. Run the second query [manual input]\n",
            "3. Run the third query [manual input]\n",
            "4. Run the fourth query [manual input]\n",
            "5. Run ALL querys with random DATA\n",
            "6. Print all NODES in db\n",
            "0. Exit the program")

        user_choice = input("Your selection: ")
        user_choice = int(user_choice)

        if user_choice == 1:
            print("Enter the shipment date in YYYY-MM-DD format: ")
            ship_date = input()
            while not is_valid_date(ship_date):
                ship_date = input("Invalid format! Please enter a valid shipment date (YYYY-MM-DD): ")

            query1_results = query1(session,
                                    dt.datetime.strptime(ship_date, "%Y-%m-%d").date())

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

            region_name = input("Enter the region name: ")

            date1 = dt.datetime.strptime(order_date, "%Y-%m-%d")
            date2 = date1.replace(date1.year + 1)

            query4_results = query4(session,
                                    date1,
                                    date2,
                                    region_name)

            print("\nResults for Query 4:")
            for result in query4_results:
                print(result)

        elif user_choice == 5:
            print("query_01:")
            date_i = dt.datetime(random.randint(2020,2024), random.randint(1,12), random.randint(1,15)).date()
            query1_results = query1(session, date=date_i)
            print(f"results for Query 1: [using date: {date_i}]")
            for result in query1_results:
                print(result)
            print()
            
            print("query_02:")
            size_i = random.randint(1,2)
            type_i = flag[random.randint(0,2)]
            region_i = region[random.randint(0,2)]
            query2_results = query2(session, size=size_i, type=type_i, i_region=region_i)
            print(f"results for Query 2: [using: size: {size_i} || type: {type_i} || region: {region_i}]")
            for result in query2_results:
                print(result)
            print()

            print("query_03:")
            date_i01 = dt.datetime(random.randint(2015,2024), random.randint(1,12), random.randint(1,15)).date()
            date_i02 = dt.datetime(random.randint(2000,2004), random.randint(1,12), random.randint(1,15)).date()
            seg_i = 'seg_x'
            query3_results = query3(session, date1=date_i01, date2=date_i02, segment=seg_i)
            print(f"results for Query 3: [using: date_01: {date_i01} || date_02: {date_i02} || segmenet: {seg_i}]")
            for result in query3_results:
                print(result)
            print()

            print("query_04:")
            date_i01 = dt.datetime(random.randint(2000,2014), random.randint(1,12), random.randint(1,15)).date()
            date_i02 = dt.datetime(random.randint(2015,2024), random.randint(1,12), random.randint(1,15)).date()
            region_i = region[random.randint(0,2)]
            query4_results = query4(session, date1=date_i01, date2=date_i02, region=region_i)
            print(f"results for Query 4: [using: date_01: {date_i01} || date_02: {date_i02} || region: {region_i}]")
            for result in query4_results:
                print(result)
            print()

        elif user_choice == 6:
            print_all_nodes_and_relationships(session)
        
        print()

    print("bye bye")


if __name__ == "__main__":
    start_program()
