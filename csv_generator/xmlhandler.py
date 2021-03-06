'''
Модуль содержит класс для обработки xml-файла.
'''


import xml.sax


class RequiredAttrError(Exception):
    pass


class XmlHandler(xml.sax.ContentHandler):
    '''
    Выполняет парсинг xml-файла для сохранения в указанный
    файл необходимых значений в csv-формате.
    '''

    def __init__(self, levels_file, objects_file):
        '''
        :param levels_file: файловый объект для сохранения значений levels
        :param objects_file: файловый объект для сохранения значений objects
        '''
        xml.sax.ContentHandler.__init__(self)
        self.levels_file = levels_file
        self.objects_file = objects_file
        self.var_names = ["id", "level"]
        self.var_values = {}
        self.object_names = []

    def startDocument(self):
        self.var_values = {}
        self.object_names = []

    def endDocument(self):
        self.levels_file.write('"{i}",{level}\n'.format(
                                    i=self.var_values["id"],
                                    level=self.var_values["level"]))
        for name in self.object_names:
            self.objects_file.write(
                '"{i}","{name}"\n'.format(i=self.var_values["id"],
                                          name=name))

    def startElement(self, name, attrs):
        if name == "var":
            values = {}
            for (k, v) in attrs.items():
                values[k] = v
            req_attrs = ["name", "value"]
            for attr in req_attrs:
                if attr not in values:
                    msg = "the '{}' attribute absent.".format(attr)
                    raise RequiredAttrError(msg)
            if values["name"] in self.var_names:
                self.var_values[values["name"]] = values["value"]
        elif name == "object":
            for (k, v) in attrs.items():
                if k == "name":
                    self.object_names.append(v)
