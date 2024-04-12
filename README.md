# Pitch Book using LLM

This sample demonstrates building a pitch book from public, private and paid data sources.

## Updates

* 4/12/2024 - Upgrade the packages (langchain, azure-search, pinecone, redis, etc) to the latest versions
* 2/7/2024 - Add capability to suggest questions for Earning Calls & SEC Filings
* 1/28/2024 - Additional Details on all Cognitive search Index used
  * pibec - Index to store the earning calls raw content
  * pibpr - Index to store the Press Releases raw content (PR Date, Title, Content)
  * pibecvector - Index to store the earning calls vector content (Only latest earning call transcript data)
  * pibsummaries - Index to store the summaries of Pre-defined or Custom Topics for earning calls and SEC Filings
  * pibsec - Index to store the SEC Filings raw content (Itemized by sections, content and additional metadata)
  * pibsecvector - Index to store the sec data vector content (Only latest sec filing data - Not the itemized vector content, but the entire document vector.  For now missing additional metadata too)
  * pibdata - Index to store the "Cached" data from the above indexes.  This is the index that is used for the search results
* 1/27/2024 - Initial Version

## Architecture

![PIB Architecture](/assets/PIB.png)

## Resources

* [Revolutionize your Enterprise Data with ChatGPT: Next-gen Apps w/ Azure OpenAI and Cognitive Search](https://aka.ms/entgptsearchblog)
* [Azure Cognitive Search](https://learn.microsoft.com/azure/search/search-what-is-azure-search)
* [Azure OpenAI Service](https://learn.microsoft.com/azure/cognitive-services/openai/overview)
* [Redis Search](https://learn.microsoft.com/en-us/azure/azure-cache-for-redis/cache-redis-modules#redisearch)
* [Pinecone](https://www.pinecone.io/learn/pinecone-v2/)
* [Cognitive Search Vector Store](https://aka.ms/VectorSearchSignUp)

## Contributions

We are open to contributions, whether it is in the form of new feature, update existing functionality or better documentation.  Please create a pull request and we will review and merge it.

### Note

>Adapted from the repo at [OpenAI-CogSearch](https://github.com/Azure-Samples/azure-search-openai-demo/),  [Call Center Analytics](https://github.com/amulchapla/AI-Powered-Call-Center-Intelligence), [Auto Evaluator](https://github.com/langchain-ai/auto-evaluator) and [Edgar Crawler](https://github.com/nlpaueb/edgar-crawler)
