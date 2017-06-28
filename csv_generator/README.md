# Установка

Реализация задания состоит из нескольких модулей:
* task.py: запускающий модуль
* gen.py: модуль с классами, реализующими решения двух заданных задач.
* xmlhandler.py: модуль с обработчиком xml-файла.
* xmlfile.py: модуль с классом для генерации xml-файла заданного формата.

Получить файлы можно из *github*:
```
git clone https://github.com/krylov/test_tasks.git
```

Файлы лежат в каталоге *test_tasks/csv_generator*


# Подготовка

Перед запуском необходимо убедиться, что в текущем окружении установлен
пакет *Jinja2* (у меня установлена версия *2.9.6*). Устновить его можно
при помощи *pip*:
```
pip install Jinja2
```

# Запуск

Список поддерживаемых приложением параметров с описанием можно получить
стандартным образом:
```
./task.py --help
```
Первая часть задачи, связанная с архивированием и генерацией xml-файлов
выполняется следующим образом:
```
./task.py -p /tmp/testdir -c zip
```
По умолчанию программа создаёт 50 zip-архивов, в каждом из которых содержится
100 сгенерированных xml-файлов. С помощью соответсвующих параметров при запуске
эти числа можно изменить. Например, чтобы создать 10 архивов с 20-ю файлами,
нужно выполнить следующую команду:
```
./task.py -a 10 -x 20 -p /tmp/testdir -c zip
```
Вторая часть задачи с обработкой полученных zip-архивов выполняется так:
```
./task.py -p /tmp/testdir -c csv
```
Путь при запуске программы должен быть указан обязательно. Генерация csv-файлов
не может быть выполнена если каталог, указываемый в параметре *p*,
не существует. Перед выполнением генерации xml-файлов и архивирования каталог,
указываемый в параметре *p*, удаляется и создаётся заново. Xml-файл генерируется
на основе шаблона *xmlfile.templ* при помощи пакета *Jinja2*.