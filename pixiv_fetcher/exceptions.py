

class HttpResponseException(Exception):

    _default_template = '''<h1>%(content)s</h1>'''

    def __init__(self, code, phrase, content=None):
        self.code = code
        self.phrase = phrase
        self._content = content
        self.message = content
        self._template = lambda d: self._default_template % d

    def send_response(self, request):
        request.setResponseCode(self.code, self.phrase)
        res = {'content': self._content, 'code': self.code, 'phrase': self.phrase}
        request.write(self.template(res))
        request.finish()

    @property
    def template(self):
        return self._template

    @template.setter
    def template(self, v):
        self._template = v

    def __str__(self):
        return '<%s[%s]@0x%x>' % (self.phrase, self.code, id(self))
