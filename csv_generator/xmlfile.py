import os
from random import randrange
from jinja2 import Environment, FileSystemLoader


class XmlFile(object):

    def __init__(self, xml_id):
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
        return self.content

    def save(self, filepath):
        with open(filepath, "w") as f:
            f.write(self.content)
