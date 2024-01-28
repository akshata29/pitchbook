import { AskRequest, AskResponse, ChatRequest, ChatResponse, SpeechTokenResponse, UserInfo} from "./models";
import { Any } from "@react-spring/web";

export async function getUserInfo(): Promise<UserInfo[]> {
  const response = await fetch('/.auth/me');
  if (!response.ok) {
      console.log("No identity provider found. Access to chat will be blocked.")
      return [];
  }

  const payload = await response.json();
  return payload;
}
export async function getNews(symbol: string): Promise<Any> {
  const response = await fetch('/getNews', {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        symbol: symbol,
          postBody: {
            values: [
              {
                recordId: 0,
                data: {
                  text: '',
                  }
              }
            ]
          }
      })
  });

  const parsedResponse: Any = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse
}
export async function getSocialSentiment(symbol: string): Promise<Any> {
  const response = await fetch('/getSocialSentiment', {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        symbol: symbol,
          postBody: {
            values: [
              {
                recordId: 0,
                data: {
                  text: '',
                  }
              }
            ]
          }
      })
  });

  const parsedResponse: Any = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse
}
export async function getIncomeStatement(symbol: string): Promise<Any> {
  const response = await fetch('/getIncomeStatement', {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        symbol: symbol,
          postBody: {
            values: [
              {
                recordId: 0,
                data: {
                  text: '',
                  }
              }
            ]
          }
      })
  });

  const parsedResponse: Any = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse
}
export async function getCashFlow(symbol: string): Promise<Any> {
  const response = await fetch('/getCashFlow', {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        symbol: symbol,
          postBody: {
            values: [
              {
                recordId: 0,
                data: {
                  text: '',
                  }
              }
            ]
          }
      })
  });

  const parsedResponse: Any = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse
}

export async function getPib(step: string, symbol: string, embeddingModelType: string, reProcess: string, options: AskRequest): Promise<AskResponse> {
  const response = await fetch('/getPib', {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        step: step,
        symbol: symbol,
        embeddingModelType:embeddingModelType,
        reProcess:reProcess,
        postBody: {
          values: [
            {
              recordId: 0,
              data: {
                text: '',
                overrides: {
                    topics: options.overrides?.topics,
                  }
  
              }
            }
          ]
        }
      })
  });

  const parsedResponse: ChatResponse = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse.values[0].data
}

export async function pibChatGptApi(options: ChatRequest, symbol: string, indexName: string): Promise<AskResponse> {
  const response = await fetch('/pibChat' , {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        symbol:symbol,
        indexName: indexName,
        postBody: {
          values: [
            {
              recordId: 0,
              data: {
                text: '',
                history: options.history,
                approach: 'rrr',
                overrides: {
                  top: options.overrides?.top,
                  temperature: options.overrides?.temperature,
                  promptTemplate: options.overrides?.promptTemplate,
                  suggest_followup_questions: options.overrides?.suggestFollowupQuestions,
                  embeddingModelType: options.overrides?.embeddingModelType,
                  firstSession:options.overrides?.firstSession,
                  session:options.overrides?.session,
                  sessionId:options.overrides?.sessionId,
                  deploymentType: options.overrides?.deploymentType,
                  chainType: options.overrides?.chainType,
                }
              }
            }
          ]
        }
      })
  });

  const parsedResponse: ChatResponse = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error(parsedResponse.values[0].data.error || "Unknown error");
  }
  return parsedResponse.values[0].data;
}

export async function getAllSessions(indexType:string, feature:string, type:string): Promise<Any> {
  const response = await fetch('/getAllSessions' , {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        indexType:indexType,
        feature:feature,
        type:type,
      })
  });

  const parsedResponse: Any = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse;
}

export async function getAllIndexSessions(indexNs: string, indexType:string, feature:string, type:string): Promise<Any> {
  const response = await fetch('/getAllIndexSessions' , {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        indexType:indexType,
        indexNs: indexNs,
        feature:feature,
        type:type,
      })
  });

  const parsedResponse: Any = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse;
}
export async function getIndexSession(indexNs: string, indexType:string, sessionName:string): Promise<Any> {
  const response = await fetch('/getIndexSession' , {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        indexType:indexType,
        indexNs: indexNs,
        sessionName:sessionName
      })
  });

  const parsedResponse: any = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse;
}
export async function deleteIndexSession(indexNs: string, indexType:string, sessionName:string): Promise<String> {
  const response = await fetch('/deleteIndexSession' , {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        indexType:indexType,
        indexNs: indexNs,
        sessionName:sessionName
      })
  });

  const parsedResponse: any = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse;
}

export async function renameIndexSession(oldSessionName: string, newSessionName:string): Promise<String> {
  const response = await fetch('/renameIndexSession' , {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        oldSessionName:oldSessionName,
        newSessionName: newSessionName
      })
  });

  const parsedResponse: any = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse;
}
export async function getIndexSessionDetail(sessionId: string): Promise<Any> {
  const response = await fetch('/getIndexSessionDetail' , {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
        sessionId:sessionId,
      })
  });

  const parsedResponse: Any = await response.json();
  if (response.status > 299 || !response.ok) {
      throw Error("Unknown error");
  }
  return parsedResponse;
}

export async function getSpeechToken(): Promise<SpeechTokenResponse> {
  const response = await fetch('/speechToken' , {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
  });

  const parsedResponse: SpeechTokenResponse = await response.json();
  if (response.status > 299 || !response.ok) {
    throw Error("Unknown error");
  }
  return parsedResponse
}

export async function getSpeechApi(text: string): Promise<string|null> {
  return await fetch("/speech", {
      method: "POST",
      headers: {
          "Content-Type": "application/json"
      },
      body: JSON.stringify({
          text: text
      })
  }).then((response) => { 
      if(response.status == 200){
          return response.blob();
      } else {
          console.error("Unable to get speech synthesis.");
          return null;
      }
  }).then((blob) => blob ? URL.createObjectURL(blob) : null);
}
export function getCitationFilePath(citation: string): string {
    return `/content/${citation}`;
}
