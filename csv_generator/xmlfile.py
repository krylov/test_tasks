'''
Модуль содержит класс для генерации xml-файла.
'''

import os
from random import randrange
from jinja2 import Environment, FileSystemLoader


class XmlFile(object):
    '''
    Класс для генерации xml-файла требуемого формата. Формат описывается с
    помощью шаблона xmlfile.templ. В конструкторе генерируются случайные
    значения и затем xml-файл с помощью Jinja2 генерируется путём заполнения
    этими данными вышеуказанного шаблона.
    '''

    def __init__(self, xml_id):
        '''
        :param xml_id: номер генериуемого xml-файла, значение соответствующего
                       атрибута id в xml-файле.
        '''
        self.data = {}
        self.data["value_id"] = xml_id
        self.data["rand"] = randrange(1, 101)
        self.data["rand_strings"] = ["value-" + str(randrange(1, 1001))
                                     for i in range(randrange(1, 11))]
        self.cwd = os.path.dirname(os.path.abspath(__file__))
        env = Environment(loader=FileSystemLoader(self.cwd))
        template = env.get_template("xmlfile.templ")
        self.content = template.render(self.data)

    def xml_content(self):
        '''
        Возвращает сгенерированный xml-текст.
        '''
        return self.content

    def save(self, filepath):
        '''
        Сохраняет сгенерированный xml-текст в заданный файл.

        :param filepath: путь к файлу для сохранения сгенерированного xml-файла.
        '''
        with open(filepath, "w") as f:
            f.write(self.content)
