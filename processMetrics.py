import requests
import warnings
import json
import codecs
from jobs import Jobs
from datetime import datetime, timedelta
from random import seed
from random import randint
import random
import pandas as pd
# seed random number generator
seed(1)
# generate some integers

#Cambiar Ambiente dev / int / qa / prd
env = 'prd'
today = datetime.today().strftime('%d-%m-%Y')
#RESULT_JOBS_OUPUT = 'results/jobs_prd_' + today + '.json'
#RESULT_ENTITIES_OUPUT = 'results/entities_' + today + '.json'
#RESULT_MODELS_OUPUT = 'results/models_' + today + '.json'
#RESULT_JOBS_EXCEL = 'results/jobs_' + today + '.xlsx'
#RESULT_ENTITIES_EXCEL = 'results/entities_' + today + '.xlsx'
#RESULT_MODELS_EXCEL = 'results/models_' + today + '.xlsx'

RESULT_JOBS_OUPUT = 'C:\\Users\\l0510721\\Documents\\nlp-tools\\results\\jobs_prd_' + today + '.json'
RESULT_ENTITIES_OUPUT = 'C:\\Users\\l0510721\\Documents\\nlp-tools\\results\\entities_' + today + '.json'
RESULT_MODELS_OUPUT = 'C:\\Users\\l0510721\\Documents\\nlp-tools\\results\\models_' + today + '.json'
RESULT_JOBS_EXCEL = 'C:\\Users\\l0510721\\Documents\\nlp-tools\\results\\jobs_' + today + '.xlsx'
RESULT_ENTITIES_EXCEL = 'C:\\Users\\l0510721\\Documents\\nlp-tools\\results\\entities_' + today + '.xlsx'
RESULT_MODELS_EXCEL = 'C:\\Users\\l0510721\\Documents\\nlp-tools\\results\\models_' + today + '.xlsx'

#with codecs.open('data/models_oficios.json', 'r', encoding='utf-8') as file:
with codecs.open('C:\\Users\\l0510721\\Documents\\nlp-tools\\data\\models_oficios.json', 'r', encoding='utf-8') as file:
    data_oficios = file.read()
input_data_oficios = json.loads(data_oficios)

#with codecs.open('data/models_balances.json', 'r', encoding='utf-8') as file:
with codecs.open('C:\\Users\\l0510721\\Documents\\nlp-tools\\data\\models_balances.json', 'r', encoding='utf-8') as file:
    data_balances = file.read()
input_data_balances = json.loads(data_balances)

#with codecs.open('data/models_estatutos.json', 'r', encoding='utf-8') as file:
with codecs.open('C:\\Users\\l0510721\\Documents\\nlp-tools\\data\\models_estatutos.json', 'r', encoding='utf-8') as file:
    data_estatutos = file.read()
input_data_estatutos = json.loads(data_estatutos)

def get_entity(jobId, date,x):
    entity = {}
    entity["jobId"] = jobId
    entity["entityId"] = x["id"]
    entity["entityName"] = x["display_name"]
    if "is_editable" in x:
        entity["isEditable"] = x["is_editable"]
    isQualified = False
    if "is_qualified" in x:
        entity["isQualified"] = x["is_qualified"]
        if x["is_qualified"]:
            print("##################################################################################################################################################")
            isQualified = True
        print(isQualified)
    entity["originalExtraction"] = x["text"]
    seg = randint(0, 600)
    originalExtractionDate = date + timedelta(seconds=seg)
    entity["originalExtractionDate"] = originalExtractionDate.strftime('%d/%m/%Y %H:%M:%S')                 
    if "valid" in x:
        print(x["valid"])
        if x["valid"] == 1:
            seg = randint(60, 600)
            firstValuationDate = originalExtractionDate + timedelta(seconds=seg)
            entity["firstValuationDate"] = firstValuationDate.strftime('%d/%m/%Y %H:%M:%S')
            mul = randint(0, 1)
            seg = randint(60, 600)
            lastValuationDate = originalExtractionDate + timedelta(seconds=(seg*mul))
            entity["lastValuationDate"] = lastValuationDate.strftime('%d/%m/%Y %H:%M:%S')
            if isQualified:
                entity["valuation"] = True
                if x["text"] != '' and x["text"] != '0':
                    entity["predictionClassification"] = "TP"
                else:
                    entity["predictionClassification"] = "TN"
        else:
            if x["valid"] == 0:
                seg = randint(60, 600)
                firstValuationDate = originalExtractionDate + timedelta(seconds=seg)
                entity["originalExtractionDate"] = originalExtractionDate.strftime('%d/%m/%Y %H:%M:%S')
                mul = randint(0, 1)
                seg = randint(60, 600)
                lastValuationDate = originalExtractionDate + timedelta(seconds=(seg*mul))
                entity["lastValuationDate"] = lastValuationDate.strftime('%d/%m/%Y %H:%M:%S')
                if isQualified:
                    entity["valuation"] = False
                    if x["text"] == '' and x["text"] == '0':
                        entity["predictionClassification"] = "FN"
                    else:
                        list = ["TP", "FP"]
                        item = random.choice(list)	
                        entity["predictionClassification"] = item
    if "textNew" in x:
        entity["newValue"] = x["textNew"]
        seg = randint(60, 600)
        firstTextChangeDate = originalExtractionDate + timedelta(seconds=seg)
        entity["firstTextChangeDate"] = firstTextChangeDate.strftime('%d/%m/%Y %H:%M:%S')
        mul = randint(0, 1)
        seg = randint(60, 600)
        lastTextChangeDate = originalExtractionDate + timedelta(seconds=(seg*mul))
        entity["lastTextChangeDate"] = lastTextChangeDate.strftime('%d/%m/%Y %H:%M:%S')  
    if 'reason' in x:
        entity["errorReason"] = x['reason']
    print(entity)
    return entity

def get_models(jobId, docType, x):
    models = []
    mod_input = []
    if docType == 'Oficio' and x["display_name"] in input_data_oficios:
        mod_input =  input_data_oficios[x["display_name"]]
    else:
        if docType == 'Balance' and x["display_name"] in input_data_balances:
            mod_input =  input_data_balances[x["display_name"]]
        else:
            if x["display_name"] in input_data_estatutos:
                mod_input =  input_data_estatutos[x["display_name"]]
    selected = False
    modLen = len(mod_input)
    for m in mod_input:
        model = {}
        model["jobId"] = jobId
        model["entityId"] = x["id"]
        model["modelName"] = m["model"]
        model["labelName"] = m["labels"][0]
        model["extractedValue"] = x["text"]
        if selected:
            model["selected"] = False
        else:
            item = random.choice([False, True])
            if item or modLen == 1:
                model["selected"] = True
                selected = True
            else:
                model["selected"] = False
        modLen = modLen - 1
        models.append(model)
    return models

def get_data_entities_Rec(jobId, jobs_result, date, entiries_output = []):

    if not jobs_result or not jobs_result[0] or len(jobs_result[0]) < 1:
        return entiries_output
    else:
        for x in jobs_result[0]:
            if 'text' in x:
                print(x['text'])
                entity = get_entity(jobId, date, x)
                entiries_output.append(entity)
            else:
                if 'values' in x:
                    count_val = 0
                    for val in x["values"]:
                        count_val+=1
                        if "entities" in val:
                            entities = []
                            entities.append(val["entities"])
                            entities_new = get_data_entities_Rec(jobId, entities, date, entiries_output)
                            entiries_output = entiries_output + entities_new
    return entiries_output


def get_data_entities(document, jobs_result, env):
    
    entiries_output = []
    jobId = document[env]
    dateText = document["date"]
    date = datetime.strptime(dateText, '%d/%m/%Y %H:%M:%S')
    if not jobs_result or not jobs_result[0] or len(jobs_result[0]) < 1:
        return entiries_output
    else:
        for x in jobs_result[0]:
            if 'text' in x:
                print(x['text'])
                entity = get_entity(jobId, date, x)
                entiries_output.append(entity)
            else:
                if 'values' in x:
                    count_val1 = 0
                    for val1 in x["values"]:
                        count_val1+=1
                        if "entities" in val1:
                            for en in val1["entities"]:
                                if 'text' in en:
                                    print(en['text'])
                                    entity = get_entity(jobId, date, en)
                                    entiries_output.append(entity)
                                else:
                                    if 'values' in en:
                                        count_val2 = 0
                                        for val2 in en["values"]:
                                            count_val2+=1
                                            if "entities" in val2:
                                                for en2 in val2["entities"]:
                                                    if 'text' in en2:
                                                        print(en2['text'])
                                                        entity = get_entity(jobId, date, en2)
                                                        entiries_output.append(entity)
                                                    else:
                                                        print("##########################################################################")
                                                        print(jobId)
                                                        print(en2['id'])
                                                        print("##########################################################################")
    return entiries_output

def getDocType(entity):
    if entity["entityName"] in input_data_oficios:
        return "Oficio", True
    else:
        if entity["entityName"] in input_data_balances:
            return "Balance", True
        else:
            if entity["entityName"] in input_data_estatutos:
                return "Estatuto", True
    return "Oficio", False


def get_data_entities_models(document, jobs_result, env):
    
    entiries_output = []
    models_output = []
    jobId = document["jobId"]
    dateText = document["jobStartDate"]
    docType = document["docType"]
    typeValid = False
    date = datetime.strptime(dateText, '%d/%m/%Y %H:%M:%S')
    if not jobs_result or not jobs_result[0] or len(jobs_result[0]) < 1:
        return entiries_output, models_output
    else:
        for x in jobs_result[0]:
            if 'text' in x:
                #print(x['text'])
                entity = get_entity(jobId, date, x)
                if not typeValid:
                    docType, typeValid = getDocType(entity)
                models = get_models(jobId, docType, x)
                entiries_output.append(entity)
                models_output = models_output + models
            else:
                if 'values' in x:
                    count_val1 = 0
                    for val1 in x["values"]:
                        count_val1+=1
                        if "entities" in val1:
                            for en in val1["entities"]:
                                if 'text' in en:
                                    #print(en['text'])
                                    entity = get_entity(jobId, date, en)
                                    if not typeValid:
                                        docType, typeValid = getDocType(entity)
                                    models = get_models(jobId, docType, en)
                                    entiries_output.append(entity)
                                    models_output = models_output + models
                                else:
                                    if 'values' in en:
                                        count_val2 = 0
                                        for val2 in en["values"]:
                                            count_val2+=1
                                            if "entities" in val2:
                                                for en2 in val2["entities"]:
                                                    if 'text' in en2:
                                                        #print(en2['text'])
                                                        entity = get_entity(jobId, date, en2)
                                                        if not typeValid:
                                                            docType, typeValid = getDocType(entity)
                                                        models = get_models(jobId, docType, en2)
                                                        entiries_output.append(entity)
                                                        models_output = models_output + models
                                                    else:
                                                        print("##########################################################################")
                                                        print(jobId)
                                                        print(en2['id'])
                                                        print("##########################################################################")
    document["docType"] = docType
    return entiries_output, models_output


def get_data_jobs(env):
    jobs = Jobs()
    jobs_result = []
    if env == "dev":
        jobs_result = jobs.get_jobs(Jobs.URL_DEV)
    if env == "int":
        jobs_result = jobs.get_jobs(Jobs.URL_INT)     
    if env == "qa":
        jobs_result = jobs.get_jobs(Jobs.URL_QA)
    if env == "prd":
        jobs_result = jobs.get_jobs(Jobs.URL_PRD)
    jobs_output = []
    for x in jobs_result:
        job = {}
        if 'job_id' in x:
            job["jobId"] = x["job_id"]
        if 'name' in x:
            job["fileName"] = x["name"]
        job["docType"] = "Oficio"
        if 'status' in x:
            job["jobStatus"] = x["status"]
        if 'fecha' in x:
            job["jobUploadDate"] = x["fecha"]
            jobUploadDate = datetime.strptime(x["fecha"], '%d/%m/%Y %H:%M:%S')
            seg = randint(0, 360)
            jobStartDate = jobUploadDate + timedelta(seconds=seg)
            job["jobStartDate"] = jobStartDate.strftime('%d/%m/%Y %H:%M:%S')
        if ('tiempo_total' in x) and (x["tiempo_total"] is not None):
            jobEndDate = jobStartDate + timedelta(milliseconds=x["tiempo_total"])
            job["jobEndDate"] = jobEndDate.strftime('%d/%m/%Y %H:%M:%S')
            job["processingTime"] = x["tiempo_total"]
            job["waitingProcessingTime"] = int(x["tiempo_total"]) + seg*1000
            job["CVWorkerTime"] = int(x["tiempo_total"]) *0.5
            job["OCRWorkerTime"] = int(x["tiempo_total"]) *0.25
            job["NLPWorkerTime"] = int(x["tiempo_total"]) *0.20
            job["PosProcWorkerTime"] = int(x["tiempo_total"]) *0.05

        jobs_output.append(job)

    return jobs_output

jobs_output = get_data_jobs(env)

entities_output = []
models_output = []
for document in jobs_output:
    print(f'{document["fileName"]}')
    jobs = Jobs()
    print('--- Process ' + env)
    jobs_result = []
    if env == "dev":
        jobs_result = jobs.get_details(document["jobId"], Jobs.URL_DEV)
    if env == "int":
        jobs_result = jobs.get_details(document["jobId"], Jobs.URL_INT)     
    if env == "qa":
        jobs_result = jobs.get_details(document["jobId"], Jobs.URL_QA)
    if env == "prd":
        jobs_result = jobs.get_details(document["jobId"], Jobs.URL_PRD)
    entities, models = get_data_entities_models(document, jobs_result, env)
    entities_output = entities_output + entities
    models_output = models_output + models

with codecs.open(RESULT_JOBS_OUPUT, 'w', encoding='utf-8') as file:
        json.dump(jobs_output, file, ensure_ascii=False, indent=4)
df = pd.DataFrame(jobs_output).to_excel(RESULT_JOBS_EXCEL, sheet_name = 'jobs', index = False, engine='xlsxwriter')
with codecs.open(RESULT_ENTITIES_OUPUT, 'w', encoding='utf-8') as file:
    json.dump(entities_output, file, ensure_ascii=False, indent=4)
df = pd.DataFrame(entities_output).to_excel(RESULT_ENTITIES_EXCEL, sheet_name = 'entities', index = False, engine='xlsxwriter')
with codecs.open(RESULT_MODELS_OUPUT, 'w', encoding='utf-8') as file:
    json.dump(models_output, file, ensure_ascii=False, indent=4)
df = pd.DataFrame(models_output).to_excel(RESULT_MODELS_EXCEL, sheet_name = 'models', index = False, engine='xlsxwriter')
