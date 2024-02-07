from flask import Flask, request, jsonify, make_response, Response
import requests
import json
from dotenv import load_dotenv
import os
import logging
from Utilities.fmp import *
from azure.cosmos import CosmosClient, PartitionKey

load_dotenv()
app = Flask(__name__)

@app.route("/", defaults={"path": "index.html"})
@app.route("/<path:path>")
def static_file(path):
    return app.send_static_file(path)

@app.route("/getPib", methods=["POST"])
def getPib():
    step=request.json["step"]
    symbol=request.json["symbol"]
    embeddingModelType=request.json["embeddingModelType"]
    reProcess=request.json["reProcess"]
    postBody=request.json["postBody"]
 
    try:
        headers = {'content-type': 'application/json'}
        url = os.environ.get("Pib_Url")

        data = postBody
        params = {'step': step, 'symbol': symbol, 'embeddingModelType': embeddingModelType, 'reProcess': reProcess }
        resp = requests.post(url, params=params, data=json.dumps(data), headers=headers)
        jsonDict = json.loads(resp.text)
        #return json.dumps(jsonDict)
        return jsonify(jsonDict)
    except Exception as e:
        logging.exception("Exception in /getPib")
        return jsonify({"error": str(e)}), 500

@app.route("/getSuggestedQuestions", methods=["POST"])
def getSuggestedQuestions():
    symbol=request.json["symbol"]
    postBody=request.json["postBody"]
 
    try:
        headers = {'content-type': 'application/json'}
        url = os.environ.get("PibChatQuestions_Url")

        data = postBody
        params = {'symbol': symbol }
        resp = requests.post(url, params=params, data=json.dumps(data), headers=headers)
        jsonDict = json.loads(resp.text)
        return jsonify(jsonDict)
    except Exception as e:
        logging.exception("Exception in /getSuggestedQuestions")
        return jsonify({"error": str(e)}), 500

@app.route("/pibChat", methods=["POST"])
def pibChat():
    symbol=request.json["symbol"]
    indexName=request.json["indexName"]
    postBody=request.json["postBody"]
 
    logging.info(f"symbol: {symbol}")
    
    try:
        headers = {'content-type': 'application/json'}
        url = os.environ.get("PibChat_Url")

        data = postBody
        params = {'symbol': symbol, 'indexName': indexName }
        resp = requests.post(url, params=params, data=json.dumps(data), headers=headers)
        jsonDict = json.loads(resp.text)
        #return json.dumps(jsonDict)
        return jsonify(jsonDict)
    except Exception as e:
        logging.exception("Exception in /pibChat")
        return jsonify({"error": str(e)}), 500

@app.route("/getNews", methods=["POST"])
def getNews():
    symbol=request.json["symbol"]
    logging.info(f"symbol: {symbol}")
    try:
        FmpKey = os.environ.get("FmpKey")

        newsResp = stockNews(apikey=FmpKey, tickers=[symbol], limit=10)
        return jsonify(newsResp)
    except Exception as e:
        logging.exception("Exception in /getNews")
        return jsonify({"error": str(e)}), 500

@app.route("/getSocialSentiment", methods=["POST"])
def getSocialSentiment():
    symbol=request.json["symbol"]
    logging.info(f"symbol: {symbol}")
    try:
        FmpKey = os.environ.get("FmpKey")

        sSentiment = socialSentiments(apikey=FmpKey, symbol=symbol)
        return jsonify(sSentiment)
    except Exception as e:
        logging.exception("Exception in /getSocialSentiment")
        return jsonify({"error": str(e)}), 500

@app.route("/getIncomeStatement", methods=["POST"])
def getIncomeStatement():
    symbol=request.json["symbol"]
    logging.info(f"symbol: {symbol}")
    try:
        FmpKey = os.environ.get("FmpKey")

        sSentiment = incomeStatement(apikey=FmpKey, symbol=symbol, limit=5)
        return jsonify(sSentiment)
    except Exception as e:
        logging.exception("Exception in /getIncomeStatement")
        return jsonify({"error": str(e)}), 500
    
@app.route("/getCashFlow", methods=["POST"])
def getCashFlow():
    symbol=request.json["symbol"]
    logging.info(f"symbol: {symbol}")
    try:
        FmpKey = os.environ.get("FmpKey")

        sSentiment = cashFlowStatement(apikey=FmpKey, symbol=symbol, limit=5)
        return jsonify(sSentiment)
    except Exception as e:
        logging.exception("Exception in /getCashFlow")
        return jsonify({"error": str(e)}), 500

@app.route("/getAllSessions", methods=["POST"])
def getAllSessions():
    indexType=request.json["indexType"]
    feature=request.json["feature"]
    type=request.json["type"]
    
    try:
        CosmosEndPoint = os.environ.get("CosmosEndPoint")
        CosmosKey = os.environ.get("CosmosKey")
        CosmosDb = os.environ.get("CosmosDatabase")
        CosmosContainer = os.environ.get("CosmosContainer")

        cosmosClient = CosmosClient(url=CosmosEndPoint, credential=CosmosKey)
        cosmosDb = cosmosClient.create_database_if_not_exists(id=CosmosDb)
        CosmosKey = PartitionKey(path="/sessionId")
        CosmosContainer = cosmosDb.create_container_if_not_exists(id=CosmosContainer, partition_key=CosmosKey, offer_throughput=400)

        cosmosQuery = "SELECT c.sessionId, c.name, c.indexId FROM c WHERE c.type = @type and c.feature = @feature and c.indexType = @indexType"
        params = [dict(name="@type", value=type), 
                  dict(name="@feature", value=feature), 
                  dict(name="@indexType", value=indexType)]
        results = CosmosContainer.query_items(query=cosmosQuery, parameters=params, enable_cross_partition_query=True)
        items = [item for item in results]
        #output = json.dumps(items, indent=True)
        return jsonify(items)
    except Exception as e:
        logging.exception("Exception in /getAllSessions")
        return jsonify({"error": str(e)}), 500
        
@app.route("/getAllIndexSessions", methods=["POST"])
def getAllIndexSessions():
    indexType=request.json["indexType"]
    indexNs=request.json["indexNs"]
    feature=request.json["feature"]
    type=request.json["type"]
    
    try:
        CosmosEndPoint = os.environ.get("CosmosEndPoint")
        CosmosKey = os.environ.get("CosmosKey")
        CosmosDb = os.environ.get("CosmosDatabase")
        CosmosContainer = os.environ.get("CosmosContainer")

        cosmosClient = CosmosClient(url=CosmosEndPoint, credential=CosmosKey)
        cosmosDb = cosmosClient.create_database_if_not_exists(id=CosmosDb)
        CosmosKey = PartitionKey(path="/sessionId")
        CosmosContainer = cosmosDb.create_container_if_not_exists(id=CosmosContainer, partition_key=CosmosKey, offer_throughput=400)

        cosmosQuery = "SELECT c.sessionId, c.name FROM c WHERE c.type = @type and c.feature = @feature and c.indexType = @indexType and c.indexId = @indexNs"
        params = [dict(name="@type", value=type), 
                  dict(name="@feature", value=feature), 
                  dict(name="@indexType", value=indexType), 
                  dict(name="@indexNs", value=indexNs)]
        results = CosmosContainer.query_items(query=cosmosQuery, parameters=params, enable_cross_partition_query=True)
        items = [item for item in results]
        #output = json.dumps(items, indent=True)
        return jsonify(items)
    except Exception as e:
        logging.exception("Exception in /getAllIndexSessions")
        return jsonify({"error": str(e)}), 500
    
@app.route("/getIndexSession", methods=["POST"])
def getIndexSession():
    indexType=request.json["indexType"]
    indexNs=request.json["indexNs"]
    sessionName=request.json["sessionName"]
    
    try:
        CosmosEndPoint = os.environ.get("CosmosEndPoint")
        CosmosKey = os.environ.get("CosmosKey")
        CosmosDb = os.environ.get("CosmosDatabase")
        CosmosContainer = os.environ.get("CosmosContainer")

        cosmosClient = CosmosClient(url=CosmosEndPoint, credential=CosmosKey)
        cosmosDb = cosmosClient.create_database_if_not_exists(id=CosmosDb)
        CosmosKey = PartitionKey(path="/sessionId")
        CosmosContainer = cosmosDb.create_container_if_not_exists(id=CosmosContainer, partition_key=CosmosKey, offer_throughput=400)

        cosmosQuery = "SELECT c.id, c.type, c.sessionId, c.name, c.chainType, \
         c.feature, c.indexId, c.IndexType, c.IndexName, c.llmModel, \
          c.timestamp, c.tokenUsed, c.embeddingModelType FROM c WHERE c.name = @sessionName and c.indexType = @indexType and c.indexId = @indexNs"
        params = [dict(name="@sessionName", value=sessionName), 
                  dict(name="@indexType", value=indexType), 
                  dict(name="@indexNs", value=indexNs)]
        results = CosmosContainer.query_items(query=cosmosQuery, parameters=params, enable_cross_partition_query=True,
                                              max_item_count=1)
        sessions = [item for item in results]
        return jsonify(sessions)
    except Exception as e:
        logging.exception("Exception in /getIndexSession")
        return jsonify({"error": str(e)}), 500
    
@app.route("/deleteIndexSession", methods=["POST"])
def deleteIndexSession():
    indexType=request.json["indexType"]
    indexNs=request.json["indexNs"]
    sessionName=request.json["sessionName"]
    
    try:
        CosmosEndPoint = os.environ.get("CosmosEndPoint")
        CosmosKey = os.environ.get("CosmosKey")
        CosmosDb = os.environ.get("CosmosDatabase")
        CosmosContainer = os.environ.get("CosmosContainer")

        cosmosClient = CosmosClient(url=CosmosEndPoint, credential=CosmosKey)
        cosmosDb = cosmosClient.create_database_if_not_exists(id=CosmosDb)
        CosmosKey = PartitionKey(path="/sessionId")
        CosmosContainer = cosmosDb.create_container_if_not_exists(id=CosmosContainer, partition_key=CosmosKey, offer_throughput=400)

        cosmosQuery = "SELECT c.sessionId FROM c WHERE c.name = @sessionName and c.indexType = @indexType and c.indexId = @indexNs"
        params = [dict(name="@sessionName", value=sessionName), 
                  dict(name="@indexType", value=indexType), 
                  dict(name="@indexNs", value=indexNs)]
        results = CosmosContainer.query_items(query=cosmosQuery, parameters=params, enable_cross_partition_query=True,
                                              max_item_count=1)
        sessions = [item for item in results]
        sessionData = json.loads(json.dumps(sessions))[0]
        cosmosAllDocQuery = "SELECT * FROM c WHERE c.sessionId = @sessionId"
        params = [dict(name="@sessionId", value=sessionData['sessionId'])]
        allDocs = CosmosContainer.query_items(query=cosmosAllDocQuery, parameters=params, enable_cross_partition_query=True)
        for i in allDocs:
            CosmosContainer.delete_item(i, partition_key=i["sessionId"])
        
        #deleteQuery = "SELECT c._self FROM c WHERE c.sessionId = '" + sessionData['sessionId'] + "'"
        #result = CosmosContainer.scripts.execute_stored_procedure(sproc="bulkDeleteSproc",params=[deleteQuery], partition_key=CosmosKey)
        #print(result)
        
        #CosmosContainer.delete_all_items_by_partition_key(sessionData['sessionId'])
        return jsonify(sessions)
    except Exception as e:
        logging.exception("Exception in /deleteIndexSession")
        return jsonify({"error": str(e)}), 500
    
@app.route("/renameIndexSession", methods=["POST"])
def renameIndexSession():
    oldSessionName=request.json["oldSessionName"]
    newSessionName=request.json["newSessionName"]
    
    try:
        CosmosEndPoint = os.environ.get("CosmosEndPoint")
        CosmosKey = os.environ.get("CosmosKey")
        CosmosDb = os.environ.get("CosmosDatabase")
        CosmosContainer = os.environ.get("CosmosContainer")

        cosmosClient = CosmosClient(url=CosmosEndPoint, credential=CosmosKey)
        cosmosDb = cosmosClient.create_database_if_not_exists(id=CosmosDb)
        CosmosKey = PartitionKey(path="/sessionId")
        CosmosContainer = cosmosDb.create_container_if_not_exists(id=CosmosContainer, partition_key=CosmosKey, offer_throughput=400)

        cosmosQuery = "SELECT * FROM c WHERE c.name = @sessionName and c.type = 'Session'"
        params = [dict(name="@sessionName", value=oldSessionName)]
        results = CosmosContainer.query_items(query=cosmosQuery, parameters=params, enable_cross_partition_query=True,
                                              max_item_count=1)
        sessions = [item for item in results]
        sessionData = json.loads(json.dumps(sessions))[0]
        #selfId = sessionData['_self']
        sessionData['name'] = newSessionName
        CosmosContainer.replace_item(item=sessionData, body=sessionData)
        return jsonify(sessions)
    except Exception as e:
        logging.exception("Exception in /renameIndexSession")
        return jsonify({"error": str(e)}), 500

@app.route("/getIndexSessionDetail", methods=["POST"])
def getIndexSessionDetail():
    sessionId=request.json["sessionId"]
    
    try:
        CosmosEndPoint = os.environ.get("CosmosEndPoint")
        CosmosKey = os.environ.get("CosmosKey")
        CosmosDb = os.environ.get("CosmosDatabase")
        CosmosContainer = os.environ.get("CosmosContainer")

        cosmosClient = CosmosClient(url=CosmosEndPoint, credential=CosmosKey)
        cosmosDb = cosmosClient.create_database_if_not_exists(id=CosmosDb)
        CosmosKey = PartitionKey(path="/sessionId")
        CosmosContainer = cosmosDb.create_container_if_not_exists(id=CosmosContainer, partition_key=CosmosKey, offer_throughput=400)

        cosmosQuery = "SELECT c.role, c.content FROM c WHERE c.sessionId = @sessionId and c.type = 'Message' ORDER by c._ts ASC"
        params = [dict(name="@sessionId", value=sessionId)]
        results = CosmosContainer.query_items(query=cosmosQuery, parameters=params, enable_cross_partition_query=True)
        items = [item for item in results]
        #output = json.dumps(items, indent=True)
        return jsonify(items)
    except Exception as e:
        logging.exception("Exception in /getIndexSessionDetail")
        return jsonify({"error": str(e)}), 500
        
if __name__ == "__main__":
    app.run(port=5003)