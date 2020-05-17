import scrapy
import json
import datetime
import urllib
import random
import time
import Exception
'''Pymongo dependencies'''
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId

ORIGIN_URI = 'http://digesto.asamblea.gob.ni'
REQUEST_URL = ORIGIN_URI + '/consultas/util/ws/proxy.php'
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

class GazzetteScrapingException(Exception):
    '''Represents all the Excepctions fo theprocess'''
    pass

class BadResponseException(GazzetteScrapingException):
    '''The request to the server get an http response status different than 2xx'''
    def __init__(self, request, response):
        self.request = str(request)
        self.request_body = str(request.body)
        self.response = str(response)
        self.response_status = response.status
        self.response_body = response.body
    
    def __str__(self):
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
        self.response_status = response.status
    
    def __str__(self):
        return '''[EMPTY RESPONSE ERROR]
            The request %s receive an empty response.
            Request:
                %s
                %s
            Response:
                %s
        ''' % ( self.request, self.request, self.request_body, self.response_status)

class GazzetteWithEmptyFieldException(GazzetteScrapingException):
    pass

class GazzetteWithWrongFormatFieldException(GazzetteScrapingException):
    pass

class GazzetteNotInsertedException(GazzetteScrapingException):
    pass

class DuplicateGazzetteException(GazzetteScrapingException):
    pass

MONGO_URI = "mongodb://127.0.0.1"
client = MongoClient(MONGO_URI)

class GazzeteCollection():
    def __init__(self):
        self.db = client['digesto']
        self.collection = self.db['gazzette']
    
    def __generate_object_id(self, number, date):

        number = str(number)
        for i in range(4-len(number))
            number = '0' + number
        date = "%d%d%d" % ( date.year, date.month, date.day)
        return ObjectId( date + number )

    def add(self, document):
        if isinstace(document, list):
            for one_document in document:
                one_document['_id'] = self.__generate_object_id(one_document)
        else:
            document['_id'] = self.__generate_object_id(one_document)
        
        self.collection.insert(document, continue_on_error=True)

    

class GazzeteSpider(scrapy.Spider):
    name = "gazzete"

    start_urls = [
        ORIGIN_URI + '/consultas/coleccion/',
    ]

    gazzete_collection = GazzeteCollection()

    def parse(self, response):
        folder_ids = response.css('#contentarbol ul li::attr("id")').extract()
        req_body = { 'hddQueryType': 'initgetRdds', 'cole': '' }
        for folder_id in folder_ids:
            cole = folder_id[4:]
            req_body['cole'] = cole
            print("Request Folder: %s"%cole)
            yield scrapy.FormRequest(
                url = REQUEST_URL,
                callback = self.parse_folder(cole),
                formdata = req_body,
                dont_filter = True
            )

    def parse_folder(self, folder_id):
        def _( response ):
            print("Response %s"%folder_id)

            if ( response.status[:1] != '2' )
                raise BadResponseException(response.request, response)

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
            i = 0
            for content_date in content_dates:
                if 3 < i:
                    break
                if random.randint(0,100) < 10:
                    i += 1
                    data['txtDatePublishFrom'], data['txtDatePublishTo'] = parse_dates(content_date)
                    yield scrapy.FormRequest(
                        url = REQUEST_URL,
                        callback = self.process_content_list,
                        formdata = data,
                        dont_filter = True
                    )
        return _

    def process_content_list(self, response):
        try:
            content_list = json.loads(response.text).get('rdds')
        except ValueError as e:
            return {}
        
        gazzete_list = []
        for content in content_list:
            gazzete = self.get_gazzete_content(content)
            if gazzete is not None:
                gazzete_list.append(gazzete)
        
        if 0 < len(gazzete_list):
            self.gazzete_collection.add( gazzete_list )
            return {'gazztes_added': len(gazzete_list)}
        
        return {}

    def get_gazzete_content(self, content):
        if 'numPublica' not in content or not content['numPublica']:
            return None
        if 'fecPublica' not in content or not content['fecPublica']:
            return None
        if 'titulo' not in content or not content['titulo']:
            return None
        return {
            'titulo': content['titulo'],
            'fecha': datetime.datetime.strptime(content['fecPublica'], DATE_FORMAT),
            'numero': int(content['numPublica'])
        }
