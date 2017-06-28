'''
Модуль содержит классы для генерации xml-файлов с последующим архивированием и
генерации csv-файлов.
'''


import os
import zipfile
import multiprocessing
from xmlhandler import XmlHandler, RequiredAttrError
import xml.sax
from glob import glob
from xmlfile import XmlFile


def wait_for_proc_end(proc):
    while proc:
        p = proc.pop()
        while p.is_alive():
            pass


class ArchGen(object):
    '''
    Класс для создания zip-архивов сгенерированных xml-файлов.
    '''

    arch_templ_name = "a_{n}.zip"
    xml_templ_name = "t_{n}.xml"

    def __init__(self, path, arch_count, xml_count):
        '''
        :param path: путь к каталогу, где генерируются xml-файлы и
                     создаются архивы
        :param arch_count: количество создаваемых архивов
        :param xml_count: количество сгенерированных xml-файлов
                          в архиве
        '''
        self.arch_path = path
        self.arch_count = arch_count
        self.xml_count = xml_count

    def make_zip(self, zip_id):
        '''
        Создаёт zip-архив. Выполняется в отдельном процессе.

        :param zip_id: номер архива, помещаемый в имя архива
        '''
        zip_fpath = os.path.join(self.arch_path,
                                 ArchGen.arch_templ_name.format(n=zip_id))
        base_id = (zip_id - 1) * self.xml_count + 1
        with zipfile.ZipFile(zip_fpath, "w") as zf:
            for i in range(base_id, base_id + self.xml_count):
                xml_file = XmlFile(i)
                xml_fname = ArchGen.xml_templ_name.format(n=i)
                xml_fpath = os.path.join(self.arch_path, xml_fname)
                xml_file.save(xml_fpath)
                zf.write(xml_fpath, os.path.basename(xml_fpath))
                os.unlink(xml_fpath)
        print("The {} file was created.".format(zip_fpath))

    def zip_all(self):
        '''
        Создаёт заданное количество архивов с заданным числом
        сгенерированнх xml-файлов.
        '''
        proc = []
        for j in range(1, self.arch_count + 1):
            p = multiprocessing.Process(target=self.make_zip, args=(j,))
            p.start()
            proc.append(p)
        wait_for_proc_end(proc)


class CsvGen(object):
    '''
    Класс для генерации csv-файлов.
    '''

    def __init__(self, path):
        '''
        Путь к каталогу, в котором хранятся zip-архивы с xml-файлами.
        '''
        self.arch_path = path
        self.handle_archives()

    def handle_archives(self):
        '''
        Извлекает xml-файлы из всех архивов.
        '''
        archives = os.listdir(self.arch_path)
        proc = []
        for fname in archives:
            arch_file_path = os.path.join(self.arch_path, fname)
            p = multiprocessing.Process(target=self.extract_all,
                                        args=(arch_file_path,))
            p.start()
            proc.append(p)
        wait_for_proc_end(proc)

    def extract_all(self, arch_file_path):
        '''
        Извлекает все xml-файлы из заданного архива. Выполняется в отдельном
        процессе.

        :param arch_file_path: путь к архиву с xml-файлами.
        '''
        extract_dir_path = "{fname}.d".format(fname=arch_file_path)
        os.mkdir(extract_dir_path)
        with zipfile.ZipFile(arch_file_path, "r") as zf:
            zf.extractall(extract_dir_path)

    def parse_xml_files(self, dirname):
        '''
        Парсит xml-файлы из заданного каталога, чтобы сгенерировать требуемые
        csv-файлы. Выполняется в отдельном процессе. После обработки xml-файл
        удаляется. После обработки всех xml-файлов в каталоге удаляется сам
        каталог.

        :param dirname: имя каталога, содержащего xml-файлы заданного формата.
        '''
        dirpath = os.path.join(self.arch_path, dirname)
        for root, dirnames, filenames in os.walk(dirpath):
            break
        levels_fname = "levels.{dirname}.csv".format(dirname=dirname)
        objects_fname = "objects.{dirname}.csv".format(dirname=dirname)
        levels_fpath = os.path.join(self.arch_path, levels_fname)
        objects_fpath = os.path.join(self.arch_path, objects_fname)
        parser = xml.sax.make_parser()
        with open(levels_fpath, "w") as levels_file:
            with open(objects_fpath, "w") as objects_file:
                parser.setContentHandler(XmlHandler(levels_file, objects_file))
                for xml_fname in filenames:
                    xml_fpath = os.path.join(dirpath, xml_fname)
                    try:
                        parser.parse(open(xml_fpath, "r"))
                    except RequiredAttrError as exc:
                        print("File {f}: {msg}".format(
                                f=xml_fpath, msg=exc))
                    os.unlink(xml_fpath)
        try:
            os.rmdir(dirpath)
        except OSError as exc:
            print("Directory {d} couldn't be removed: {exc}".format(d=dirpath,
                                                                    exc=exc))

    def join_csv_files(self, name):
        '''
        Объединяет csv-файлы, которые после удаляются, в один общий.
        Выполняется в отдельном процессе.

        :param name: часть имени csv-файла, по которому определяется требуемый
                     набор данных.
        '''
        csv_templ_name = os.path.join(self.arch_path,
                                      "{}.*.csv".format(name))
        csv_fpath = os.path.join(self.arch_path,
                                 "{}.csv".format(name))
        with open(csv_fpath, "w") as csv_file:
            for fpath in glob(csv_templ_name):
                with open(fpath, "r") as f:
                    csv_file.write(f.read())
                os.unlink(fpath)
        print("The {} file was created.".format(csv_fpath))

    def gen_csv_files(self):
        '''
        Запускает обработку xml-файлов для каждого каталога с xml-файлами, а
        затем объединяет созданные csv-файлы в два общих по виду требуемого
        набора данных.
        '''
        for root, dirnames, filenames in os.walk(self.arch_path):
            break
        proc = []
        for dirname in dirnames:
            p = multiprocessing.Process(target=self.parse_xml_files,
                                        args=(dirname,))
            p.start()
            proc.append(p)
        wait_for_proc_end(proc)

        proc.append(multiprocessing.Process(target=self.join_csv_files,
                                            args=("levels",)))
        proc.append(multiprocessing.Process(target=self.join_csv_files,
                                            args=("objects",)))
        for p in proc:
            p.start()
        wait_for_proc_end(proc)
