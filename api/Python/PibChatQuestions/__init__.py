from Utilities.envVars import *
from datetime import datetime
from pytz import timezone
from dateutil.relativedelta import relativedelta
from Utilities.pibCopilot import createPibQuestionsIndex, findPibQuestionsData, findLatestEarningCallBySymbol, mergeDocs, findLatestSecFilingsBySymbol
from Utilities.fmp import *
import logging, json, os
import azure.functions as func
from Utilities.modelHelper import numTokenFromMessages, getTokenLimit
from openai import AzureOpenAI
import uuid
import tiktoken

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
PibQuestionsIndex = os.environ['PibQuestionsIndex']
PibEarningsCallIndex = os.environ['PibEarningsCallIndex']
PibSecDataIndex = os.environ['PibSecDataIndex']

def generateQuestions(symbol, client, questionType):
    # Find if we already have generated the questions
    pibQuestionsIndex = os.environ['PibQuestionsIndex']
    r = findPibQuestionsData(SearchService, SearchKey, pibQuestionsIndex, symbol, questionType[0], returnFields=['id', 'symbol', "questionType",
                                                                   'pibQuestions'])
    generatedQuestion = []
    filingType = "10-K"
    if r.get_count() == 0:
        logging.info("No questions found for symbol " + symbol + " and questionType " + questionType[0])
        earningIndexName = PibEarningsCallIndex
        secIndexName = PibSecDataIndex

        # Find the latest earning call transcript
        if questionType[0] == "earningCalls":
            r = findLatestEarningCallBySymbol(SearchService, SearchKey, earningIndexName, symbol, returnFields=["id", "symbol", "quarter", "year", "callDate", "content"])
        else:
            logging.info("Find out the CIK for the Symbol")
            cik = str(int(searchCik(apikey=FmpKey, ticker=symbol)[0]["companyCik"]))
            logging.info(f"CIK for {symbol} is {cik}")
            r = findLatestSecFilingsBySymbol(SearchService, SearchKey, secIndexName, cik, filingType, returnFields=["id", "cik", "filingType", "filingDate", "content"])
        if r.get_count() > 0:
            logging.info("Earning call found for symbol " + symbol)
            for s in r:
                content = s['content']
                break;
            
            if questionType[0] == "earningCalls":
                systemTemplate = """You are an AI assistant tasked with generating relevant questions from earning call transcripts.   Given the following extracted earning call transcript, 
                        generate 15 questions an Financial Analyst will ask during the call.   
                        Do not include any explanations, only provide a  RFC8259 compliant JSON response following this format without deviation.
                        [{
                        "question": "generated question here",
                        }]
                        """
            elif questionType[0] == "secFiling":
                systemTemplate = """You are an AI assistant tasked with generating relevant questions from SEC 10-K annual report.   Given the following extracted SEC filings, 
                generate 15 questions an Investment Bank will ask. Question should cover the financial statements, market risk, future growth and other topics.
                Do not include any explanations, only provide a  RFC8259 compliant JSON response following this format without deviation.
                [{
                "question": "generated question here",
                }]
                        """

            messages = getMessagesFromHistory(
                    systemTemplate,
                    content,
                    )
            completion = client.chat.completions.create(
                model=OpenAiChat16k, 
                messages=messages,
                temperature=0.0,
                max_tokens=1000,
                n=1)
            answer = completion.choices[0].message.content
            generatedQuestion.append(
                    {
                        'id' : str(uuid.uuid4()),
                        'symbol': symbol,
                        'questionType': questionType[0],
                        'pibQuestions': answer
                    })

        else:
            logging.info("No earning call found for symbol " + symbol)
            for s in r:
                generatedQuestion.append(
                    {
                        'id' : str(uuid.uuid4()),
                        'symbol': symbol,
                        'questionType': questionType[0],
                        'pibQuestions': "[{'question':'No Earning Call Found'}]"
                    })
    else:
        logging.info("Questions found for symbol " + symbol + " and questionType " + questionType[0])
        for s in r:
            generatedQuestion.append(
                {
                    'id' : s['id'],
                    'symbol': s['symbol'],
                    'questionType': s['questionType'],
                    'pibQuestions': s['pibQuestions']
                })

    mergeDocs(SearchService, SearchKey, pibQuestionsIndex, generatedQuestion)

    return generatedQuestion
def truncateToken(string: str, encoding_name: str, max_length: int = 15000) -> str:
    """Truncates a text string based on max number of tokens."""
    encoding = tiktoken.encoding_for_model(encoding_name)
    encoded_string = encoding.encode(string)
    num_tokens = len(encoded_string)

    if num_tokens > max_length:
        string = encoding.decode(encoded_string[:max_length])

    return string
def getMessagesFromHistory(systemPrompt: str, userConv: str):
        #messageBuilder = MessageBuilder(systemPrompt, modelId)
        messages = []
        messages.append({'role': 'system', 'content': systemPrompt})
        userContent = truncateToken(string=userConv, encoding_name="gpt-3.5-turbo", max_length=8192)
        messages.append({'role': "user", 'content': userContent})
        
        return messages
def PibSuggestQuestions(symbol, overrides):
    logging.info("Calling PibSuggestQuestions Open AI for symbol " + symbol)

    try:
        questionType = overrides.get("topics") or []
        central = timezone('US/Central')
        today = datetime.now(central)
        historicalDate = today - relativedelta(years=3)
        historicalDate = historicalDate.strftime("%Y-%m-%d")
        pibQuestionsIndex = PibQuestionsIndex
        filingType = "10-K"
        # Find out the CIK for the Symbol 
        createPibQuestionsIndex(SearchService, SearchKey, pibQuestionsIndex)
    except Exception as e:
        logging.info("Error in PibData Open AI : " + str(e))
        return {"data_points": "", "answer": "Exception during finding answers - Error : " + str(e), "thoughts": "", "sources": "", "nextQuestions": "", "error":  str(e)}

    try:
        client = AzureOpenAI(
                    api_key = OpenAiKey,  
                    api_version = OpenAiVersion,
                    azure_endpoint = OpenAiEndPoint
                    )
        logging.info("LLM Setup done")        

        pibQuestions = generateQuestions(symbol, client, questionType)
        outputFinalAnswer = {"data_points": "", "answer": pibQuestions, 
                        "thoughts": '',
                            "sources": '', "nextQuestions": '', "error": ""}
        return outputFinalAnswer    
    except Exception as e:
      logging.info("Error in PibData Open AI : " + str(e))
      return {"data_points": "", "answer": "Exception during finding answers - Error : " + str(e), "thoughts": "", "sources": "", "nextQuestions": "", "error":  str(e)}

    #return answer
def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    logging.info(f'{context.function_name} HTTP trigger function processed a request.')
    if hasattr(context, 'retry_context'):
        logging.info(f'Current retry count: {context.retry_context.retry_count}')

        if context.retry_context.retry_count == context.retry_context.max_retry_count:
            logging.info(
                f"Max retries of {context.retry_context.max_retry_count} for "
                f"function {context.function_name} has been reached")

    try:
        symbol = req.params.get('symbol')
        body = json.dumps(req.get_json())
    except ValueError:
        return func.HttpResponse(
             "Invalid body",
             status_code=400
        )

    if body:
        result = ComposeResponse(symbol, body)
        return func.HttpResponse(result, mimetype="application/json")
    else:
        return func.HttpResponse(
             "Invalid body",
             status_code=400
        )
def ComposeResponse(symbol, jsonData):
    values = json.loads(jsonData)['values']

    logging.info("Calling Compose Response")
    # Prepare the Output before the loop
    results = {}
    results["values"] = []

    for value in values:
        outputRecord = TransformValue(symbol, value)
        if outputRecord != None:
            results["values"].append(outputRecord)
    return json.dumps(results, ensure_ascii=False)
def TransformValue(symbol, record):
    logging.info("Calling Transform Value")
    try:
        recordId = record['recordId']
    except AssertionError  as error:
        return None

    # Validate the inputs
    try:
        assert ('data' in record), "'data' field is required."
        data = record['data']
        assert ('text' in data), "'text' field is required in 'data' object."

    except KeyError as error:
        return (
            {
            "recordId": recordId,
            "errors": [ { "message": "KeyError:" + error.args[0] }   ]
            })
    except AssertionError as error:
        return (
            {
            "recordId": recordId,
            "errors": [ { "message": "AssertionError:" + error.args[0] }   ]
            })
    except SystemError as error:
        return (
            {
            "recordId": recordId,
            "errors": [ { "message": "SystemError:" + error.args[0] }   ]
            })

    try:
        # Getting the items from the values/data/text
        value = data['text']
        overrides = data['overrides']
        answer = PibSuggestQuestions(symbol, overrides)
        return ({
            "recordId": recordId,
            "data": answer
            })

    except:
        return (
            {
            "recordId": recordId,
            "errors": [ { "message": "Could not complete operation for record." }   ]
            })