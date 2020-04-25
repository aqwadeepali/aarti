import gzip, cStringIO

class CompressionManager:
    def __init__(self, app, compresslevel=9):
        def compress_response(response):
            response.direct_passthrough = False            
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Headers'] = 'Origin, X-Requested-With, Content-Type, Accept, X-Access-Token,  X-Key'
            response.headers['Access-Control-Allow-Methods'] = '*'
            # Compress the response
            if response.status_code != 200 or len(response.data) < 500 or 'Content-Encoding' in response.headers:
                return response

            gzip_buffer = cStringIO.StringIO()
            gzip_file = gzip.GzipFile(mode='wb', compresslevel=compresslevel, fileobj=gzip_buffer)
            gzip_file.write(response.data)
            gzip_file.close()
            response.data = gzip_buffer.getvalue()
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Content-Length'] = str(len(response.data))

            return response

        app.after_request(compress_response)