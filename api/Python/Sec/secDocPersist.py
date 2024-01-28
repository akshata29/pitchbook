import logging, json, os
import azure.functions as func
import os
import numpy as np
from azure.search.documents.indexes.models import (  
    SearchIndex,  
    SearchField,  
    SearchFieldDataType,  
    SimpleField,  
    SearchableField,  
    SearchIndex,  
    SemanticConfiguration,  
    PrioritizedFields,  
    SemanticField,  
    SearchField,  
    SemanticSettings,  
    VectorSearch,  
    HnswVectorSearchAlgorithmConfiguration,  
)
from azure.search.documents.models import Vector 
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import *
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential 
from Utilities.envVars import *
import tiktoken
from itertools import islice
from Utilities.azureBlob import upsertMetadata, getBlob, getAllBlobs

OpenAiDocStorName = os.environ["OpenAiDocStorName"]
OpenAiDocStorKey = os.environ["OpenAiDocStorKey"]
OpenAiDocConnStr = f"DefaultEndpointsProtocol=https;AccountName={OpenAiDocStorName};AccountKey={OpenAiDocStorKey};EndpointSuffix=core.windows.net"
SecDocContainer = os.environ["SecDocContainer"]
OpenAiEndPoint = os.environ['OpenAiEndPoint']
OpenAiChat = os.environ['OpenAiChat']
OpenAiChat16k = os.environ['OpenAiChat16k']
OpenAiKey = os.environ['OpenAiKey']
OpenAiApiKey = os.environ['OpenAiApiKey']
OpenAiEmbedding = os.environ['OpenAiEmbedding']
FmpKey = os.environ['FmpKey']
BingUrl = os.environ['BingUrl']
BingKey = os.environ['BingKey']
SearchService = os.environ['SearchService']
SearchKey = os.environ['SearchKey']

def GetAllFiles():
    # Get all files in the container from Azure Blob Storage
    # Create the BlobServiceClient object
    blobList = getAllBlobs(OpenAiDocConnStr, SecDocContainer)
    files = []
    for file in blobList:
        if (file.metadata == None):
            files.append({
            "filename" : file.name,
            "embedded": "false",
            })
        else:
            files.append({
                "filename" : file.name,
                "embedded": file.metadata["embedded"] if "embedded" in file.metadata else "false",
                })
    logging.info(f"Found {len(files)} files in the container")
    return files

def createSearchIndex(indexType, indexName):
    indexClient = SearchIndexClient(endpoint=f"https://{SearchService}.search.windows.net/",
            credential=AzureKeyCredential(SearchKey))
    if indexName not in indexClient.list_index_names():
        if indexType == "cogsearchvs":
            index = SearchIndex(
                name=indexName,
                fields=[
                            SimpleField(name="id", type=SearchFieldDataType.String, key=True), 
                            SimpleField(name="cik", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                            SimpleField(name="company", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                            SimpleField(name="filingType", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                            SimpleField(name="filingDate", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                            SimpleField(name="periodOfReport", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                            SimpleField(name="sic", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                            SimpleField(name="stateOfInc", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                            SimpleField(name="stateLocation", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                            SimpleField(name="fiscalYearEnd", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                            SimpleField(name="filingHtmlIndex", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                            SimpleField(name="htmFilingLink", type=SearchFieldDataType.String, retrievable=True),
                            SimpleField(name="completeTextFilingLink", type=SearchFieldDataType.String, retrievable=True),
                            SimpleField(name="filename", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item1", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item1A", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item1B", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item2", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item3", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item4", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item5", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item6", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item7", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item7A", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item8", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item9", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item9A", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item9B", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item10", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item11", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item12", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item13", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item14", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="item15", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SimpleField(name="metadata", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                            SearchableField(name="content", type=SearchFieldDataType.String, retrievable=True),
                            # SearchField(name="contentVector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                            #             searchable=True, vector_search_dimensions=1536, vector_search_configuration="vectorConfig"),
                            SimpleField(name="sourcefile", type="Edm.String", filterable=True, facetable=True),
                ],
                semantic_settings=SemanticSettings(
                    configurations=[SemanticConfiguration(
                        name='semanticConfig',
                        prioritized_fields=PrioritizedFields(
                            title_field=SemanticField(field_name="content"), prioritized_content_fields=[SemanticField(field_name='content')]))],
                        prioritized_keywords_fields=[SemanticField(field_name='sourcefile')])
                )
        elif indexType == "cogsearch":
            index = SearchIndex(
            name=indexName,
            fields=[
                        SimpleField(name="id", type=SearchFieldDataType.String, key=True), 
                        SimpleField(name="cik", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                        SimpleField(name="company", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                        SimpleField(name="filingType", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                        SimpleField(name="filingDate", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                        SimpleField(name="periodOfReport", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                        SimpleField(name="sic", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                        SimpleField(name="stateOfInc", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                        SimpleField(name="stateLocation", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                        SimpleField(name="fiscalYearEnd", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                        SimpleField(name="filingHtmlIndex", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=True, analyzer_name="en.microsoft"),
                        SimpleField(name="htmFilingLink", type=SearchFieldDataType.String, retrievable=True),
                        SimpleField(name="completeTextFilingLink", type=SearchFieldDataType.String, retrievable=True),
                        SimpleField(name="filename", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item1", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item1A", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item1B", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item2", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item3", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item4", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item5", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item6", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item7", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item7A", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item8", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item9", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item9A", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item9B", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item10", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item11", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item12", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item13", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item14", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="item15", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SimpleField(name="metadata", type=SearchFieldDataType.String, searchable=True, retrievable=True),
                        SearchableField(name="content", type=SearchFieldDataType.String, retrievable=True),
                        # SearchField(name="contentVector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                        #             searchable=True, vector_search_dimensions=1536, vector_search_configuration="vectorConfig"),
                        SimpleField(name="sourcefile", type="Edm.String", filterable=True, facetable=True),
            ],
            semantic_settings=SemanticSettings(
                configurations=[SemanticConfiguration(
                    name='semanticConfig',
                    prioritized_fields=PrioritizedFields(
                        title_field=SemanticField(field_name="content"), prioritized_content_fields=[SemanticField(field_name='content')]))],
                        prioritized_keywords_fields=[SemanticField(field_name='sourcefile')])
            )

        try:
            print(f"Creating {indexName} search index")
            indexClient.create_index(index)
        except Exception as e:
            print(e)
    else:
        logging.info(f"Search index {indexName} already exists")

def batched(iterable, n):
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := tuple(islice(it, n))):
        yield batch

def chunkedTokens(text, encoding_name, chunk_length):
    encoding = tiktoken.get_encoding(encoding_name)
    tokens = encoding.encode(text)
    chunks_iterator = batched(tokens, chunk_length)
    yield from chunks_iterator

def getChunkedText(text, encoding_name="cl100k_base", max_tokens=1500):
    chunked_text = []
    encoding = tiktoken.get_encoding(encoding_name)
    for chunk in chunkedTokens(text, encoding_name=encoding_name, chunk_length=max_tokens):
        chunked_text.append(encoding.decode(chunk))
    return chunked_text

def chunkAndEmbed(embeddingModelType, indexType, indexName, secDoc, fullPath):
    encoding = tiktoken.get_encoding("cl100k_base")
    fullData = []
    text = secDoc['item_1'] + secDoc['item_1A'] + secDoc['item_7'] + secDoc['item_7A']
    text = text.replace("\n", " ")
    # Since we are not embedding, let's not worry about the length of the text
    # length = len(encoding.encode(text))

    if indexType == "cogsearchvs":
        # if length > 1500:
        #     k=0
        #     chunkedText = getChunkedText(text, encoding_name="cl100k_base", max_tokens=1500)
        #     logging.info(f"Total chunks: {len(chunkedText)}")
        #     for chunk in chunkedText:
        #         secCommonData = {
        #             "id": f"{fullPath}-{k}".replace(".", "_").replace(" ", "_").replace(":", "_").replace("/", "_").replace(",", "_").replace("&", "_"),
        #             "cik": secDoc['cik'],
        #             "company": secDoc['company'],
        #             "filing_type": secDoc['filing_type'],
        #             "filing_date": secDoc['filing_date'],
        #             "period_of_report": secDoc['period_of_report'],
        #             "sic": secDoc['sic'],
        #             "state_of_inc": secDoc['state_of_inc'],
        #             "state_location": secDoc['state_location'],
        #             "fiscal_year_end": secDoc['fiscal_year_end'],
        #             "filing_html_index": secDoc['filing_html_index'],
        #             "htm_filing_link": secDoc['htm_filing_link'],
        #             "complete_text_filing_link": secDoc['complete_text_filing_link'],
        #             "filename": secDoc['filename'],
        #             "item_1": secDoc['item_1'],
        #             "item_1A": secDoc['item_1A'],
        #             "item_1B": secDoc['item_1B'],
        #             "item_2": secDoc['item_2'],
        #             "item_3": secDoc['item_3'],
        #             "item_4": secDoc['item_4'],
        #             "item_5": secDoc['item_5'],
        #             "item_6": secDoc['item_6'],
        #             "item_7": secDoc['item_7'],
        #             "item_7A": secDoc['item_7A'],
        #             "item_8": secDoc['item_8'],
        #             "item_9": secDoc['item_9'],
        #             "item_9A": secDoc['item_9A'],
        #             "item_9B": secDoc['item_9B'],
        #             "item_10": secDoc['item_10'],
        #             "item_11": secDoc['item_11'],
        #             "item_12": secDoc['item_12'],
        #             "item_13": secDoc['item_13'],
        #             "item_14": secDoc['item_14'],
        #             "item_15": secDoc['item_15'],
        #             "content": chunk,
        #             #"contentVector": [],
        #             "metadata" : json.dumps({"cik": secDoc['cik'], "source": secDoc['filename'], "filingType": secDoc['filing_type'], "reportDate": secDoc['period_of_report']}),
        #             "sourcefile": fullPath
        #         }
        #         # Comment for now on not generating embeddings
        #         #secCommonData['contentVector'] = generateEmbeddings(embeddingModelType, chunk)
        #         fullData.append(secCommonData)
        #         k=k+1
        # else:
        #logging.info(f"Process full text with text {text}")
        secCommonData = {
                "id": f"{fullPath}".replace(".", "_").replace(" ", "_").replace(":", "_").replace("/", "_").replace(",", "_").replace("&", "_"),
                "cik": secDoc['cik'],
                "company": secDoc['company'],
                "filingType": secDoc['filing_type'],
                "filingDate": secDoc['filing_date'],
                "periodOfReport": secDoc['period_of_report'],
                "sic": secDoc['sic'],
                "stateOfInc": secDoc['state_of_inc'],
                "stateLocation": secDoc['state_location'],
                "fiscalYearEnd": secDoc['fiscal_year_end'],
                "filingHtmlIndex": secDoc['filing_html_index'],
                "htmFilingLink": secDoc['htm_filing_link'],
                "completeTextFilingLink": secDoc['complete_text_filing_link'],
                "filename": secDoc['filename'],
                "item1": secDoc['item_1'],
                "item1A": secDoc['item_1A'],
                "item1B": secDoc['item_1B'],
                "item2": secDoc['item_2'],
                "item3": secDoc['item_3'],
                "item4": secDoc['item_4'],
                "item5": secDoc['item_5'],
                "item6": secDoc['item_6'],
                "item7": secDoc['item_7'],
                "item7A": secDoc['item_7A'],
                "item8": secDoc['item_8'],
                "item9": secDoc['item_9'],
                "item9A": secDoc['item_9A'],
                "item9B": secDoc['item_9B'],
                "item10": secDoc['item_10'],
                "item11": secDoc['item_11'],
                "item12": secDoc['item_12'],
                "item13": secDoc['item_13'],
                "item14": secDoc['item_14'],
                "item15": secDoc['item_15'],
                "content": text,
                #"contentVector": [],
                "metadata" : json.dumps({"cik": secDoc['cik'], "source": secDoc['filename'], "filingType": secDoc['filing_type'], "reportDate": secDoc['period_of_report']}),
                "sourcefile": fullPath
            }
        # Comment for now on not generating embeddings
        #secCommonData['contentVector'] = generateEmbeddings(embeddingModelType, text)
        fullData.append(secCommonData)
        try:

            searchClient = SearchClient(endpoint=f"https://{SearchService}.search.windows.net/",
                                        index_name=indexName,
                                        credential=AzureKeyCredential(SearchKey))
            results = searchClient.upload_documents(fullData)
            #succeeded = sum([1 for r in results if r.succeeded])
            logging.info("Completed Indexing of documents")
        except Exception as e:
            logging.error(f"Error indexing documents {e}")
            raise e

    return None

def PersistSecDocs(embeddingModelType, indexType, indexName,  req):
    body = json.dumps(req)
    values = json.loads(body)['values']
    value = values[0]
    data = value['data']
    text = data['text']
    logging.info("Embedding text")

    try:
        filesData = GetAllFiles()
        filesData = list(filter(lambda x : x['embedded'] == "false", filesData))
        logging.info(filesData)
        filesData = list(map(lambda x: {'filename': x['filename']}, filesData))

        logging.info(f"Found {len(filesData)} files to embed")
        for file in filesData:
            fileName = file['filename']
            readBytes = getBlob(OpenAiDocConnStr, SecDocContainer, fileName)
            secDoc = json.loads(readBytes.decode("utf-8"))           
            # For now we will combine Item 1, 1A, 7, 7A into a single field "content"
            # item_1 = TextField(name="item_1")
            # item_1A = TextField(name="item_1A")
            # item_1B = TextField(name="item_1B")
            # item_2 = TextField(name="item_2")
            # item_3 = TextField(name="item_3")
            # item_4 = TextField(name="item_4")
            # item_5 = TextField(name="item_5")
            # item_6 = TextField(name="item_6")
            # item_7 = TextField(name="item_7")
            # item_7A = TextField(name="item_7A")
            # item_8 = TextField(name="item_8")
            # item_9 = TextField(name="item_9")
            # item_9A = TextField(name="item_9A")
            # item_9B = TextField(name="item_9B")
            # item_10 = TextField(name="item_10")
            # item_11 = TextField(name="item_11")
            # item_12 = TextField(name="item_12")
            # item_13 = TextField(name="item_13")
            # item_14 = TextField(name="item_14")
            # item_15 = TextField(name="item_15")
            # item1Embedding = VectorField("item1_vector", "HNSW", { "TYPE": "FLOAT32", "DIM": 1536, "DISTANCE_METRIC": distanceMetrics, "INITIAL_CAP": 3155})
            # item1AEmbedding = VectorField("item1A_vector", "HNSW", { "TYPE": "FLOAT32", "DIM": 1536, "DISTANCE_METRIC": distanceMetrics, "INITIAL_CAP": 3155})
            # item7Embedding = VectorField("item7_vector", "HNSW", { "TYPE": "FLOAT32", "DIM": 1536, "DISTANCE_METRIC": distanceMetrics, "INITIAL_CAP": 3155})
            # item7AEmbedding = VectorField("item7A_vector", "HNSW", { "TYPE": "FLOAT32", "DIM": 1536, "DISTANCE_METRIC": distanceMetrics, "INITIAL_CAP": 3155})
            # item8Embedding = VectorField("item8_vector", "HNSW", { "TYPE": "FLOAT32", "DIM": 1536, "DISTANCE_METRIC": distanceMetrics, "INITIAL_CAP": 3155})
            # fields = [cik, company, filing_type, filing_date, period_of_report, sic, state_of_inc, state_location, 
            #          fiscal_year_end, filing_html_index, htm_filing_link, complete_text_filing_link, filename,
            #          item_1, item_1A, item_1B, item_2, item_3, item_4, item_5, item_6, item_7, item_7A, item_8, item_9, 
            #          item_9A, item_9B, item_10, item_11, item_12, item_13, item_14, item_15, item1Embedding,
            #          item1AEmbedding, item7Embedding, item7AEmbedding, item8Embedding]
            
            if indexType == "cogsearchvs":
                logging.info("Create index")
                createSearchIndex(indexType, indexName)
                logging.info("Index created")
                logging.info("Chunk and Embed")
                try:
                    chunkAndEmbed(embeddingModelType, indexType, indexName, secDoc, os.path.basename(fileName))
                except Exception as e:
                    logging.error(e)
                    logging.error("Error chunking and embedding")
                logging.info("Embedding complete")
                metadata = {'embedded': 'true', 'indexType': indexType, "indexName": indexName}
                upsertMetadata(OpenAiDocConnStr, SecDocContainer, fileName, metadata)
        return "Success"
    except Exception as e:
      logging.error(e)
      