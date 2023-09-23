import jwt
import json
from decouple import config
import time
import os
import uvicorn
from fastapi import FastAPI, Request, BackgroundTasks, Header
import shutil
import warnings
import wget
import requests
import re
import uuid
import datetime
from pathlib import Path
import gc
import gdown
import torch
import subprocess
import pymongo
from decouple import config
import base64
import sentry_sdk
from sentry_sdk import capture_exception, set_tag
warnings.filterwarnings("ignore")

sentry_sdk.init(
    dsn="https://5fa2374f99934aef8d92fcf07a3fc233@o1213286.ingest.sentry.io/4505198976172032",
    traces_sample_rate=1.0)

def read_encoded():
  retrieved = config('PUBLIC_KEY')  # Retrieve the value of 'PUBLIC_KEY' from the environment using config function
  base64_bytes = retrieved.encode("ascii")  # Encode the retrieved value as ASCII bytes
  bytes_like = base64.b64decode(base64_bytes)  # Decode the base64-encoded bytes
  public_key = bytes_like.decode("ascii")  # Decode the bytes as ASCII and store the result as public_key
  return public_key

public_key = read_encoded()

#DB Connect
try :   
    connection_url = config("MONGO")
    client = pymongo.MongoClient(connection_url)

    database_name = "aimodels"

    stt_db = client[database_name]
    
    collection_name= "post_stt_logs"
    collection = stt_db[collection_name]
    print("Mongo DB connection established")

except Exception as e:
    capture_exception(e)
    print(f'MongoDB connection error: {e}')


#MAIN
def main(uid, model_name, device, output, logs, cdr, files, audio_url, ln, webhook_URL):

    #START
    t1=time.time()
    logs[uid].append(f"Process start with uid: {uid}")

    try:
        #Making Dir
        main_output_dir=os.path.join(str(Path.cwd()), 'data/'+str(datetime.date.today()))
        os.makedirs(main_output_dir, exist_ok=True)

        #Reading Audio
        reg_site = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
        audio_path=[]
        if re.findall(reg_site, audio_url):
            try:
                if "drive.google.com" in audio_url:
                    print("Going with G-drive audio")
                    path=str(Path.cwd())+f"/data/{uid}.wav"
                    gdown.download(audio_url, path, quiet=False, fuzzy=True)
                    audio_path.append(path)
                    logs[uid].append(f"G-drive Audio Saved, Path: {path}")
                else:
                    print("Going with S3 audio")
                    path=str(Path.cwd())+f"/data/{uid}.wav"
                    audio_response = wget.download(audio_url, path)
                    audio_path.append(audio_response)
                    logs[uid].append(f"S3 Audio Saved, Path: {path}")
            except Exception as e:
                error={"Error":f"Download fail, UID:{uid}, error: {e}"}
                print(f"Download fail:{error}")
                requests.post(webhook_URL, json=error)
                logs[uid].append(error)
                ex_id = collection.insert_one(logs).inserted_id
                print(f"logs sent to DB with exit id:{ex_id}")
                return None
        else:
            print("Going with Local Audio")
            path=str(Path.cwd())+"/data/"+audio_url.split("/")[-1]
            logs[uid].append(f"Local Audio Saved, Path: {path}")
            shutil.copy(audio_url, path)
            audio_path.append(path)

        #Sentry Context-Tag
        sentry_sdk.set_context("STT-staging", output)
        set_tag("requestid", uid)

        #SHELL Exec
        print("\nTranscribing")
        logs[uid].append("Transcribing")
        command=f"whisper {audio_path[0]}  --model {model_name} --language {ln} --device {device} --word_timestamps True"
        data= subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).stdout.readlines()
        print("Transcription done")
        logs[uid].append("Transcription done")
        
        #JSON Check
        output_json=audio_path[0].split('/')[-1].split('.')[0]+".json"
        cdr1=subprocess.Popen("pwd", stdout=subprocess.PIPE, shell=True).stdout.readlines()
        files1=subprocess.Popen("ls", stdout=subprocess.PIPE, shell=True).stdout.readlines()
        for i1 in cdr1:
            cdr.append(i1.decode("utf-8").splitlines()[0])
        for i2 in files1:
            files.append(i2.decode("utf-8").splitlines()[0])

        if output_json in files:
            with open(output_json, "r") as f:
                result=json.load(f)
            pass
        else:
            error={"Error": "NO JSON formed, process terminated"}
            print(error)
            requests.post(webhook_URL, json=error)
            logs[uid].append(error)
            ex_id = collection.insert_one(logs).inserted_id
            print(f"logs sent to DB with exit id:{ex_id}")
            return None

        #Data Filling
        print("filling data...")
        logs[uid].append("filling data...")

        output["metadata"]["request_id"]=output["metadata"]["request_id"]+uid
        output["metadata"]["created"]=output["metadata"]["created"]+str(datetime.datetime.now())
        output["metadata"]["language"]=output["metadata"]["language"]+ln
        output["metadata"]["audio_url"]=output["metadata"]["audio_url"]+audio_url
        output["metadata"]["models"]["transcription"]=output["metadata"]["models"]["transcription"]+model_name
        output["metadata"]["transcription time taken"]=output["metadata"]["transcription time taken"]+str(round((time.time()-t1), 2))+" sec"

        output["results"]['channels'][0]['alternatives'][0]['transcript']=result["text"]
        words=[]
        for seg in result["segments"]:
            for item in seg["words"]:
                item["confidence"]=item["probability"]
                del item["probability"]
                words.append(item)
        output["results"]['channels'][0]['alternatives'][0]['words']=words

        #JSON Save
        shutil.move(output_json, main_output_dir)
        print("output saved")
        logs[uid].append("output saved")
        requests.post(webhook_URL, json={"output": output})
        print(output)


        #Sending Logs
        del output["results"]
        logs[uid].append({"main_output":output})
        ex_id = collection.insert_one(logs).inserted_id
        print(f"logs sent to DB with exit id:{ex_id}")

        #Cleaning
        os.remove(audio_path[0])
        os.remove(audio_path[0].split('/')[-1].split('.')[0]+".tsv")
        os.remove(audio_path[0].split('/')[-1].split('.')[0]+".srt")
        os.remove(audio_path[0].split('/')[-1].split('.')[0]+".txt")
        os.remove(audio_path[0].split('/')[-1].split('.')[0]+".vtt")
        gc.collect()

    except Exception as e:
        error={f"Error in process with uid: {uid}" : e}
        print(error)
        logs[uid].append(error)
        requests.post(webhook_URL, json=error)
        capture_exception(e)
        ex_id = collection.insert_one(logs).inserted_id
        print(f"logs sent to DB with exit id:{ex_id}")
        return None
        

#API requests
import logging
class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage().find("/speech-text/health") == -1
# Filter out /endpoint
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

app = FastAPI()
print("app started...")
@app.get('/speech-text/')
async def home():
    return {'Welcome message':'welcome to stt model GKE big audio test'}

@app.get('/speech-text/health')
async def health():
    return{"message":'healthy with new 31.0'}

@app.post('/speech-text/post_call')
async def transcription(request:Request, background_task : BackgroundTasks, encoded:str=Header(None)):

    torch.cuda.empty_cache()
    gc.collect()

    #Request Parameters: 4
    body = await request.json()
    audio_url = body['audio_URL']
    ln=body['language']
    webhook_URL=body['webhook_URL']
    tag=body['metadata']

    #Main Parameters: 4
    uid=str(uuid.uuid1())
    model_name="base"
    device="cuda"
    logs={uid:[]}
    cdr=[]
    files=[]
    output={
        "metadata": {
            "tag": tag,
        "request_id": "",
        "created": "",
        "audio duration":"",
        "channels": "",
        "language":"",
        "transcription time taken":"",
        "audio_url":"",
        "models": {"transcription":""},
        "model_info": {
            "openai-whisper": {
                "name": "openai-whisper",
                "version": "20230314",
                "tier": "tiny"
                }
            }
        },
        "results": {
            "channels": [
                {
                    "alternatives": [{}]
                }
            ]
        }
    }
    try:
        dic = jwt.decode(encoded, public_key, algorithms=['RS256'])
        background_task.add_task(main, uid, model_name, device, output, logs, cdr, files, audio_url, ln, webhook_URL)
        return{"status":f"success with uid: {uid}"}
    except Exception as e:
        return {"message":"Authentication Failed: " + str(e)}


if __name__ == "__main__":
   uvicorn.run("transcript:app",host = '0.0.0.0',port = 5000)

   
#transcript_shell


