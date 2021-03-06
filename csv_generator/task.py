#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import argparse
from shutil import rmtree
from gen import ArchGen, CsvGen


def main():
    '''
    Сначала должно выполняться архивирование:
    >>> ./task.py -p /tmp/somedir -c zip

    Затем генерация csv-файлов:
    >>> ./task.py -p /tmp/somedir -c csv

    '''

    parser = argparse.ArgumentParser(
                description="Архиватор и csv генератор.")
    parser.add_argument("-c", "--command", default="zip", type=str,
                        choices=["zip", "csv"],
                        help="Выполняемое действие: zip - архивирование "
                             "сгенерированнх xml-файлов; csv - генерация "
                             "csv-файлов из zip-архивов.")
    parser.add_argument("-p", "--path", type=str,
                        help="Путь к каталогу, в котором будут генерироваться "
                             "и архивироваться xml-файлы.")
    parser.add_argument("-a", "--archcount", type=int, default=50,
                        help="Количество zip-архивов. По умолчанию: 50.")
    parser.add_argument("-x", "--xmlcount", type=int, default=100,
                        help="Количество xml-файлов в архиве. "
                             "По умолчанию: 100.")
    args = parser.parse_args()

    if args.command == "zip":
        if os.path.exists(args.path):
            rmtree(args.path)
        os.mkdir(args.path)
        ArchGen(args.path, args.archcount, args.xmlcount).zip_all()
    elif args.command == "csv":
        if not os.path.exists(args.path):
            print("Directory {} doesn't exist. Run the script "
                  "with --command=zip".format(args.path))
            exit(1)
        csv = CsvGen(args.path)
        csv.gen_csv_files()


if __name__ == "__main__":
    main()
