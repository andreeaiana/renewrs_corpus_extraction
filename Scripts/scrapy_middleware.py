import urllib
class MyCustomDownloaderMiddleware(object):
    def process_request(self, request, spider):
        request._url = request.url.replace("%C3%83%C2%BC", urllib.parse.quote("ü"))
