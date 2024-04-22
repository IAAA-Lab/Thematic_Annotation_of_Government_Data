"""
harvester/SPARQL_harvester.py
Author: Javier Nogueras (jnog@unizar.es)
Last update: 2022-07-13

Program providing a specialized harvester to download metadata records with properties in English
"""


from SPARQLWrapper import SPARQLWrapper, JSON

import os
import ssl

import rdflib

from rdflib import Graph, URIRef
from rdflib.namespace import FOAF, RDF, DCTERMS, SKOS

OUTPUT = "output/"

EDP_SPARQL = 'https://data.europa.eu/sparql'
EDP_RDF = 'https://data.europa.eu/data/api/datasets/'
EDP_FORMAT = '.ttl?useNormalizedId=true&locale=en'

DCAT_DATASET = URIRef("http://www.w3.org/ns/dcat#Dataset")
DCAT_THEME = URIRef("http://www.w3.org/ns/dcat#theme")
DCAT_KEYWORD = URIRef("http://www.w3.org/ns/dcat#keyword")
DCAT_DISTRIBUTION = URIRef("http://www.w3.org/ns/dcat#distribution")
DCAT_ACCESSURL = URIRef("http://www.w3.org/ns/dcat#accessURL")
DCAT_NS = rdflib.Namespace("http://www.w3.org/ns/dcat#")
LOCN_NS = rdflib.Namespace("http://www.w3.org/ns/locn#")
SCHEMA_NS = rdflib.Namespace("http://schema.org/")
SCHEMAS_NS = rdflib.Namespace("https://schema.org/")

def get_file_name(url):
    """
    https://europeandataportal.eu/set/data/https-opendata-aragon-es-datos-catalogo-dataset-oai-zaguan-unizar-es-89319
    return https-opendata-aragon-es-datos-catalogo-dataset-oai-zaguan-unizar-es-89319
    """
    words = url.split('/')
    file_name = words[len(words)-1]
    return file_name

def transform_to_file_name(url):
    x = ":/\\."
    y = "____"
    table = url.maketrans(x, y)
    return url.translate(table)

def create_folder(url):
    if (not os.path.exists(OUTPUT)):
        os.mkdir(OUTPUT)
    folder_name = transform_to_file_name(url)
    output_path = os.path.join(OUTPUT,folder_name)
    if (not os.path.exists(output_path)):
        os.mkdir(output_path)
    return output_path



class SpecializedHarvester:

    def __init__(self, url, user = None, passwd = None, rdf_url = None, limit = 10000, max_number_of_records = None, output_folder = OUTPUT, format = '.ttl'):
        self.url = url
        self.sparql = SPARQLWrapper(url)
        if user is not None:
            self.sparql.setCredentials(user, passwd)
        self.rdf_url = rdf_url
        self.limit = limit
        self.output_folder = output_folder
        self.format = format
        self.max_number_of_records = max_number_of_records


    def count_datasets(self):
        self.sparql.setQuery("""
               PREFIX dct:<http://purl.org/dc/terms/>
               PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
               PREFIX dcat: <http://www.w3.org/ns/dcat#>
               SELECT (count(distinct ?s) as ?values) WHERE 
               { 
                  ?s a dcat:Dataset  
                  ; dct:language <http://publications.europa.eu/resource/authority/language/ENG>
                  ; dct:title ?title
                  ; dct:description ?description .
                  FILTER(lang(?title)='en') .
                  FILTER(lang(?description)='en')
              }
                """)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        print(results)
        for row in results["results"]["bindings"]:
            count = int(row["values"]["value"])
        return count

    def annotate_identifiers(self, fw, offset):
        query = """
               PREFIX dct:<http://purl.org/dc/terms/>
               PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
               PREFIX dcat: <http://www.w3.org/ns/dcat#>
               SELECT distinct ?s WHERE 
               { 
                  ?s a dcat:Dataset  
                  ; dct:language <http://publications.europa.eu/resource/authority/language/ENG>
                  ; dct:title ?title
                  ; dct:description ?description .
                  FILTER(lang(?title)='en') .
                  FILTER(lang(?description)='en')
              }
              OFFSET  """ + str(offset) + """
              LIMIT """ + str(self.limit) + """
            """
        print(query)
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        for row in results["results"]["bindings"]:
            dataset = row["s"]["value"]
            fw.write(dataset+'\n')

    def parse_dataset(self, url, output_graph):
        """
        Parses the dataset with URL in the graph
        """
        id = get_file_name(url)
        rdf_url = self.rdf_url + id + self.format
        print(rdf_url)
        try:
            input_graph = Graph()
            input_graph.parse(rdf_url, format="turtle")
            dataset = URIRef(url)
            output_graph.add((dataset, RDF.type, DCAT_DATASET))

            for s, p, o in input_graph.triples((None, rdflib.namespace.RDF.type, DCAT_DATASET)):
                self.process_concept(s, input_graph, lang='en', output_graph=output_graph)
        except Exception as err:
            print(f'Other error occurred: {err}')

    def write_lang_property(self,resource, input_graph, property, lang, output_graph):
        for value in input_graph.objects(resource, property):
             if value.language == lang:
                 output_graph.add((resource, property, value))

    def write_property(self,resource, input_graph, property, output_graph):
        for value in input_graph.objects(resource, property):
            print(value)
            output_graph.add((resource, property, value))

    def write_theme(self,resource, input_graph, output_graph):
        for value in input_graph.objects(resource, DCAT_THEME):
            if value.startswith("http://publications.europa.eu/resource/authority/data-theme/"):
             output_graph.add((resource, DCAT_THEME, value))

    def write_complex_node(self,resource, input_graph, property, output_graph):
        for value in input_graph.objects(resource, property):
            output_graph.add((resource, property, value))
            output_graph += input_graph.triples((value,None,None))

    def write_organization_name(self,resource, input_graph, output_graph):
        for publisher in input_graph.objects(resource, DCTERMS.publisher):
            for organization_name in input_graph.objects(publisher, FOAF.name):
                output_graph.add((resource, FOAF.name, organization_name))

    def write_access_URL(self,resource, input_graph, output_graph):
        for distribution in input_graph.objects(resource, DCAT_DISTRIBUTION):
            for access_url in input_graph.objects(distribution, DCAT_ACCESSURL):
                output_graph.add((resource, DCAT_ACCESSURL, access_url))

    def process_concept(self,resource, input_graph, lang, output_graph):
        self.write_lang_property(resource, input_graph, DCTERMS.title, lang, output_graph)
        self.write_lang_property(resource, input_graph, DCTERMS.description, lang, output_graph)
        self.write_theme(resource, input_graph, output_graph)
        self.write_lang_property(resource, input_graph, DCAT_KEYWORD, lang, output_graph)
        self.write_complex_node(resource, input_graph, DCTERMS.spatial, output_graph)
        self.write_complex_node(resource, input_graph, DCTERMS.temporal, output_graph)
        self.write_organization_name(resource, input_graph, output_graph)
        self.write_access_URL(resource, input_graph, output_graph)

    def initialize_graph(self):
        graph = Graph()
        graph.bind('dcterms', DCTERMS)
        graph.bind('dcat', DCAT_NS)
        graph.bind('locn', LOCN_NS)
        graph.bind('foaf', FOAF)
        graph.bind('skos', SKOS)
        graph.bind('schema', SCHEMA_NS)
        graph.bind('schemas', SCHEMAS_NS)
        return graph

    def create_record_files(self, identifiers_file_name, output_folder):
        output_graph = self.initialize_graph()
        fr = open(identifiers_file_name)
        i=0
        for line in fr:
            url = line.replace("\n","")
            self.parse_dataset(url,output_graph)
            i=i+1
            if (i % self.limit) == 0:
                catalog_file_name = os.path.join(output_folder, 'catalog'+str(i)+'.ttl')
                output_graph.serialize(destination=catalog_file_name, format='turtle')
                output_graph = self.initialize_graph()
        if (i % self.limit) > 0:
            catalog_file_name = os.path.join(output_folder, 'catalog' + str(i) + '.ttl')
            output_graph.serialize(destination=catalog_file_name, format='turtle')

    def harvest(self):
        folder = create_folder(self.url)
        identifiers_file_name = os.path.join(folder, 'identifiers.txt')

        if (not os.path.exists((identifiers_file_name))):
            fw = open(identifiers_file_name,'w')
            count = self.count_datasets()
            print (count, ' datasets')
            if self.max_number_of_records is not None:
                count = self.max_number_of_records
            offset = 0
            while (offset < count):
                self.annotate_identifiers( fw, offset)
                offset = offset + self.limit
            fw.close()

        self.create_record_files(identifiers_file_name, folder)

if __name__ == '__main__':

    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
            getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context

    harvester = SpecializedHarvester(url = EDP_SPARQL, rdf_url= EDP_RDF, limit=1000, output_folder = OUTPUT, format = EDP_FORMAT)
    harvester.harvest()
