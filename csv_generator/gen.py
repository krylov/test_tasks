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

    arch_templ_name = "a_{n}.zip"
    xml_templ_name = "t_{n}.xml"

    def __init__(self, path, arch_count, xml_count):
        self.arch_path = path
        self.arch_count = arch_count
        self.xml_count = xml_count

    def make_zip(self, zip_id):
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
        proc = []
        for j in range(1, self.arch_count + 1):
            p = multiprocessing.Process(target=self.make_zip, args=(j,))
            p.start()
            proc.append(p)
        wait_for_proc_end(proc)


class CsvGen(object):

    def __init__(self, path):
        self.arch_path = path
        self.handle_archives()

    def handle_archives(self):
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
        extract_dir_path = "{fname}.d".format(fname=arch_file_path)
        os.mkdir(extract_dir_path)
        with zipfile.ZipFile(arch_file_path, "r") as zf:
            zf.extractall(extract_dir_path)

    def parse_xml_files(self, dirname):
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
        csv_templ_name = os.path.join(self.arch_path,
                                      "{}.*.csv".format(name))
        csv_fpath = os.path.join(self.arch_path,
                                 "{}.csv".format(name))
        with open(csv_fpath, "w") as csv_file:
            for fpath in glob(csv_templ_name):
                with open(fpath, "r") as f:
                    csv_file.write(f.read())
                os.unlink(fpath)

    def gen_csv_files(self):
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
