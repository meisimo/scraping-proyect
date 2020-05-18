# -*- coding: utf-8 -*-
import scrapy
import json
import datetime
import urllib
import random
import time
import re
import sys
'''Pymongo dependencies'''
from pymongo import MongoClient
from pymongo.errors import PyMongoError, DuplicateKeyError
from bson.objectid import ObjectId

ONE_DAY = datetime.timedelta(1)
DATE_FORMAT = '%d/%m/%Y'

def parse_dates(content_date):

    month = int(content_date['mes'])
    year = int(content_date['anyo'])
    next_month = month + 1
    if 12 < next_month :
        next_month %= 12
        next_year = year + 1
    else :
        next_year = year

    date_ini = datetime.datetime(year, month, 1)
    date_end = datetime.datetime(next_year, next_month, 1) - ONE_DAY

    return "%d/%d/%d"%(date_ini.year, date_ini.month, date_ini.day), "%d/%d/%d"%(date_end.year, date_end.month, date_end.day)

def complete_number_with_leading_zeros(size_len, number):
    number = str(number)
    for i in range(size_len-len(number)):
        number = '0' + number
    return number

class GazzetteScrapingException(Exception):
    '''Represents all the Excepctions fo theprocess'''

class BadResponseException(GazzetteScrapingException):
    '''The request to the server get an http response status different than 2xx'''
    def __init__(self, request, response):
        self.request = str(request)
        self.request_body = str(request.body)
        self.response = str(response)
        self.response_status = str(response.status)
        self.response_body = response.body
    
    def __str__(self):
        return '''[BAD RESPONSE ERROR]'''

    def detail(self):
        return '''[BAD RESPONSE ERROR]
            The request %s receive a bad response: %s
            Request:
                %s
                %s
            Response:
                %s
                %s
                %s
        ''' % ( self.request, self.response, self.request, self.request_body, self.response, self.response_status, self.response_body )

class EmptyResponseException(GazzetteScrapingException):
    '''The request to the server get an empty response'''
    def __init__(self, request, response):
        self.request = str(request)
        self.request_body = str(request.body)
        self.response_status = str(response.status)
    
    def __str__(self):
        return '''[EMPTY RESPONSE ERROR]'''

    def detail(self):
        return '''[EMPTY RESPONSE ERROR]
            The request %s receive an empty response.
            Request:
                %s
                %s
            Response:
                %s
        ''' % ( self.request, self.request, self.request_body, self.response_status)

class GazzetteWithEmptyFieldException(GazzetteScrapingException):
    '''The Gazzette doesn't has a field that is required'''
    def __init__(self, field, content):
        self.field = field
        self.content = content
    
    def __str__(self):
        return '''[Gazzette with required field empty] field: %s''' % self.field

    def detail(self):
        return '''[Gazzette with required field empty]
            The content to create a gazzette doesn't has the field %s, that is required.
            Content received:
                %s
        ''' % ( self.field , self.content )
    
class GazzetteWithWrongFormatFieldException(GazzetteScrapingException):
    '''The Gazzette has a field that with a wrong format'''
    def __init__(self, field, required_format, field_content, content):
        self.field = field
        self.required_format = required_format
        self.field_content = field_content
        self.content = content
    
    def __str__(self):
        return '''[Gazzette with invalid format field] field:%s format:%s''' % (self.field, self.required_format)

    def detail(self):
        return '''[Gazzette with invalid format field]
            The field %s doesn't has a valid format: %s
            Field content:
                %s
            Complete content:
                %s
        ''' % ( self.field , self.required_format, self.field_content, self.content )

class GazzetteNotInsertedException(GazzetteScrapingException):
    '''A Gazzette couldn't be inserted'''
    def __init__(self, error, document):
        self.error = error
        self.document = document

    def __str__(self):
        return '''[Error inserting a Gazzette]'''

    def detail(self):
        return '''[Error inserting a Gazzette]
            There was an error inserting a gazzette.
            Detail: 
                %s
            Document:
                %s
        ''' % (str(self.error), str(self.document))
    
class DuplicateGazzetteException(GazzetteScrapingException):
    '''Some gazzette already exists in the database'''
    def __init__(self, error, n_not_inserted, documents):
        self.error = error
        self.n_not_inserted = n_not_inserted
        self.documents = documents
    
    def __str__(self):
        return '''[Error inserting some Gazzettes] Not inserted: %d'''%self.n_not_inserted

    def detail(self):
        return '''[Error inserting some Gazzettes]
            There are %d gazzettes in an insertion that already exists in the database.
            Detail: 
                %s
            Documnts of the insertion bulk:
                %s
        ''' % (self.n_not_inserted, str(self.error), str(self.documents))

MONGO_URI = "mongodb://127.0.0.1"
client = MongoClient(MONGO_URI)

class GazzeteCollection():
    def __init__(self):
        self.db = client['digesto']
        self.collection = self.db['gazzette']
    
    def __generate_object_id(self, number, date):
        year = complete_number_with_leading_zeros(4, date.year)
        month = complete_number_with_leading_zeros(2, date.month)
        day = complete_number_with_leading_zeros(2, date.day)
        number = complete_number_with_leading_zeros(4, number)

        object_id = ObjectId( year + month + day + number )
        return object_id

    def add(self, documents):
        if not isinstance(documents, list):
            documents = [documents]
        
        for one_document in documents:
            one_document['_id'] = self.__generate_object_id(one_document['numero'], one_document['fecha'])
        
        gazzette_initial_amount = self.collection.count()
        try:
            insertion_result =  self.collection.insert(documents, continue_on_error=True)
        except DuplicateKeyError as e:
            gazzette_final_amount = self.collection.count()
            inserted_gazzettes = gazzette_initial_amount - gazzette_final_amount
            not_inserted_gazzettes = len(documents) - inserted_gazzettes
            raise DuplicateGazzetteException(e, not_inserted_gazzettes, documents)
        except PyMongoError as e:
            raise GazzetteNotInsertedException(e, documents)

class DataBase():

    @classmethod
    def __generate_object_id(cls, number, date):
        number = str(number)
        if re.match('^\d+[a-z]{1}', number) is None or 4 < len(number):
            number = str(random.randint(1,9999))
        
        year = complete_number_with_leading_zeros(4, date.year)
        month = complete_number_with_leading_zeros(2, date.month)
        day = complete_number_with_leading_zeros(2, date.day)
        number = complete_number_with_leading_zeros(4, number)
        
        object_id = ObjectId( year + month + day + number )
        return object_id
    
    @classmethod
    def add(cls, collection_name, documents):
        db = client['digesto']
        collection = db[collection_name]
        if not isinstance(documents, list):
            documents = [documents]
        
        for one_document in documents:
            one_document['_id'] = DataBase.__generate_object_id(one_document['numero'], one_document['fecha'])

        try:        
            gazzette_initial_amount = collection.count()
        except Exception:
            gazzette_initial_amount = 0
        
        try:
            insertion_result =  collection.insert(documents, continue_on_error=True)
        except DuplicateKeyError as e:
            gazzette_final_amount = collection.count()
            inserted_gazzettes = gazzette_initial_amount - gazzette_final_amount
            not_inserted_gazzettes = len(documents) - inserted_gazzettes
            raise DuplicateGazzetteException(e, not_inserted_gazzettes, documents)
        except PyMongoError as e:
            raise GazzetteNotInsertedException(e, documents)

class GazzeteSpider(scrapy.Spider):
    '''Self defined constants'''
    ORIGIN_URI = 'http://digesto.asamblea.gob.ni'
    REQUEST_URL = ORIGIN_URI + '/consultas/util/ws/proxy.php'
    FIELDS_FORMAT = {
        'numPublica': {
            'required': False,
            'format': re.compile('^\d+'),
            'format_description': 'only numeric field',
        },
        'fecPublica': {
            'required': True,
            'format': re.compile('^\d{2}[/]\d{2}[/]\d{4}$'),
            'format_description': 'only date with the format dd/mm/yyy',
        },
        'titulo': {
            'required': True,
            'format': None,
            'format_description': None,
        },
    }
    '''Spider properties'''
    name = "gazzete"
    start_urls = [
        ORIGIN_URI + '/consultas/coleccion/',
    ]
    '''DB properties'''
    # gazzete_collection = GazzeteCollection()
    
    def parse(self, response):
        folder_ids = response.css('#contentarbol ul li::attr("id")').extract()
        self.folder_name = {folder_id.replace("cole",""): response.css('#contentarbol #%s::text'%folder_id).extract_first().replace("\n","").replace("\t","").strip() for folder_id in folder_ids}
    
        req_body = { 'hddQueryType': 'initgetRdds', 'cole': '' }
        for folder_id in folder_ids:
            cole = folder_id[4:]
            collection_name = self.folder_name[cole].replace(" ", "_").lower()
            req_body['cole'] = cole
            print("Request folder %s" % self.folder_name[cole])
            try:
                yield scrapy.FormRequest(
                    url = self.REQUEST_URL,
                    callback = self.parse_folder( cole, collection_name),
                    formdata = req_body,
                    dont_filter = True
                )
            except GazzetteScrapingException as e:
                print(e)

    def parse_folder(self, folder_id, collection):
        def _( response ):
            if str(response.status)[:1] != '2':
                raise BadResponseException(response.request, response)
            if not response.body:
                raise EmptyResponseException(response.request, response)

            content_dates = json.loads(response.text).get('rdds')
            data = {
                    'hddQueryType': 'getRdds',
                    'cole': folder_id,
                    'slcCollection': folder_id,
                    'slcMedio': '',
                    'txtNumPublish': '',
                    'txtTitlePublish': '',
                    'txtDatePublishFrom': '',
                    'txtDatePublishTo': '',
                    'hddPageSize': '',
                    'hddCurrentPage': '',
                    'txtfilter': ''
                }
            for content_date in content_dates:
                data['txtDatePublishFrom'], data['txtDatePublishTo'] = parse_dates(content_date)
                try :
                    yield scrapy.FormRequest(
                        url = self.REQUEST_URL,
                        callback = self.process_content_list(collection, folder_id, data['txtDatePublishFrom'], data['txtDatePublishTo']),
                        formdata = data,
                        dont_filter = True
                    )
                except GazzetteScrapingException as e:
                    print(e)
        return _

    def process_content_list(self, collection, cole, date_ini, date_fin):
        def _(response):
            sys.stdout.write("Response of request folder %s %s\t" % (self.folder_name[cole], date_ini))

            if str(response.status)[:1] != '2':
                raise BadResponseException(response.request, response)
            if not response.body:
                raise EmptyResponseException(response.request, response)

            content_list = json.loads(response.text).get('rdds')
            gazzete_list = []

            for content in content_list:
                try:
                    gazzete = self.get_gazzete_content(content)
                    gazzete_list.append(gazzete)
                except GazzetteScrapingException as e:
                    # print(e.detail())
                    pass
                except Exception as e:
                    print("%s\t %s" % (e.detail(), content['titulo'] if content['titulo'] else '_NO_TITLE_'))
                
            try:
                inserted_gazzettes = len(gazzete_list)
                if 0<inserted_gazzettes:
                    DataBase.add(collection, gazzete_list)
                    # for gazzette in gazzete_list:
                    #     print(gazzette['titulo'])
            except DuplicateGazzetteException  as e:
                print(e)
                inserted_gazzettes -= e.n_not_inserted
            except GazzetteScrapingException as e:
                print("Error!")
                print( e.detail())
                inserted_gazzettes = -1
            sys.stdout.write("Inserted: %d\n" % inserted_gazzettes)
            # return {'gazzettes_added': inserted_gazzettes}
        return _

    def get_gazzete_content(self, content):
        for field, format in self.FIELDS_FORMAT.items():
            if format['required']:
                if field not in content or not content[field]:
                    raise GazzetteWithEmptyFieldException(field, content)
            if format['format'] and content[field]:
                if format['format'].search(content[field]) is None:
                    raise GazzetteWithWrongFormatFieldException(field, format['format_description'], content[field], content)
        return {
            'titulo': content['titulo'],
            'fecha': datetime.datetime.strptime(content['fecPublica'], DATE_FORMAT),
            'numero': content['numPublica'] if content['numPublica'] else random.randint(1,9999)
        }