from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from neo4j import GraphDatabase

namenode = "hadoop-namenode:8020"

uri = "bolt://neo4j:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

spark = SparkSession.builder \
    .appName("XML to Neo4j") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0") \
    .getOrCreate()

xml_df = spark.read \
    .format("xml") \
    .options("rowTag", "entry") \
    .load(f"hdfs://{namenode}/data/landing/xmlfile/")

# Extract data for nodes
protein_data = xml_df.select("protein", "_accession").rdd.map(lambda x: (x[0], x[1])).collect()
gene_data = xml_df.select("gene").rdd.map(lambda x: x[0]).collect()
feature_data = xml_df.select("feature").rdd.map(lambda x: x[0]).collect()
reference_data = xml_df.select("reference").rdd.map(lambda x: x[0]).collect()
organism_data = xml_df.select("organism").rdd.map(lambda x: x[0]).collect()

# Extract relationships data
entry_organism = xml_df.select("_accession", "organism.name.type").rdd.map(lambda x: (x[0], x[1])).collect()
entry_gene = xml_df.select("_accession", "gene.name.type").rdd.map(lambda x: (x[0], x[1])).collect()
entry_feature = xml_df.select("_accession", "feature._id").rdd.map(lambda x: (x[0], x[1])).collect()
entry_reference = xml_df.select("_accession", "reference.key").rdd.map(lambda x: (x[0], x[1])).collect()
entry_full_name = xml_df.select("_accession", "protein.recommendedName.fullName").rdd.map(lambda x: (x[0], x[1])).collect()

def create_graph(tx, protein_data, gene_data, feature_data, reference_data, organism_data,
                 entry_organism, entry_gene, entry_feature, entry_reference, entry_full_name):

    # Create nodes
    for protein, accession in protein_data:
        tx.run("MERGE (:Protein {accession: $accession, fullName: $protein})", accession=accession, protein=protein)
    for gene in gene_data:
        tx.run("MERGE (:Gene {primary: $primary})", primary=gene.name[0].value)
    for feature in feature_data:
        tx.run("MERGE (:Feature {id: $id})", id=feature._id)
    for reference in reference_data:
        tx.run("MERGE (:Reference {key: $key})", key=reference.key)
        for author in reference.authorList.person:
            tx.run("MERGE (:Author {name: $name})", name=author.name)
            tx.run("""
                MATCH (r:Reference {key: $key})
                MATCH (a:Author {name: $name})
                MERGE (r)-[:HAS_AUTHOR]->(a)
            """, key=reference.key, name=author.name)
    for organism in organism_data:
        tx.run("MERGE (:Organism {scientific: $scientific})", scientific=organism.name[0].value)

    # Create relationships
    for accession, organism in entry_organism:
        tx.run("""
            MATCH (p:Protein {accession: $accession})
            MATCH (o:Organism {scientific: $organism})
            MERGE (p)-[:IN_ORGANISM]->(o)
        """, accession=accession, organism=organism)

    for accession, gene in entry_gene:
        tx.run("""
            MATCH (p:Protein {accession: $accession})
            MATCH (g:Gene {primary: $gene})
            MERGE (p)-[:FROM_GENE]->(g)
        """, accession=accession, gene=gene)

    for accession, feature in entry_feature:
        tx.run("""
            MATCH (p:Protein {accession: $accession})
            MATCH (f:Feature {id: $feature})
            MERGE (p)-[:HAS_FEATURE]->(f)
        """, accession=accession, feature=feature)

    for accession, reference in entry_reference:
        tx.run("""
            MATCH (p:Protein {accession: $accession})
            MATCH (r:Reference {key: $reference})
            MERGE (p)-[:HAS_REFERENCE]->(r)
        """, accession=accession, reference=reference)

    for accession, full_name in entry_full_name:
        tx.run("""
            MATCH (p:Protein {accession: $accession})
            MERGE (p)-[:HAS_FULL_NAME]->(:FullName {name: $full_name})
        """, accession=accession, full_name=full_name)


# Execute the create_graph function
with driver.session() as session:
    session.write_transaction(create_graph, protein_data, gene_data, feature_data, reference_data, organism_data, entry_organism, entry_gene, entry_feature, entry_reference, entry_full_name)

# Close the driver connection
driver.close()