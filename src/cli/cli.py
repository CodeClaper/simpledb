# cli.py
import os
import sys
import json
import socket
from sourcer import Source
import dbclient
import getpass
import readline

client = dbclient.DbClient('127.0.0.1', 4083)

## System keywords.
keywords = [
    'SELECT', 'UPDATE', 'DELETE', 'INSERT', 'DROP', 'CREATE', 'ALTER', 'DESC', 'DESCRIBE', 'SHOW', 'SET', ## operation.
    'TABLE', 'FROM', 'WHERE',  ## clause
    'AND', 'OR' ## logic
    'MAX', 'MIN', 'SUM', 'COUNT', 'AVG', ## aggregate functions
    'SOURCE'
]

def get_path_completions():
    line = readline.get_line_buffer()
    if (line.startswith('source ')):
        line = line.replace("source", "", 1).lstrip()

    # 扩展用户目录（如 ~）
    expanded_text = os.path.expanduser(line)

    # 分离目录和前缀
    if os.path.isdir(expanded_text):
        dir_path = expanded_text
        prefix = ""
    else:
        dir_path, prefix = os.path.split(expanded_text)
    
    # 检查目录是否存在
    if not os.path.isdir(dir_path):
        return []
    
    # 获取目录下的文件和子目录
    try:
        files = os.listdir(dir_path)
    except PermissionError:
        return []
    
    # 过滤匹配项，并为目录添加斜杠
    matches = [
        f + "/" if os.path.isdir(os.path.join(dir_path, f)) else f
        for f in files
        if f.startswith(prefix)
    ]
    matches.sort()
    return matches

## Load keywords.
def completer(text, state):
    keyword_options = [i for i in keywords if i.startswith(text) or i.startswith(text.upper())]
    path_options = get_path_completions()
    options = keyword_options + path_options
    try:
        return options[state]
    except:
        pass

readline.set_completer(completer)
readline.parse_and_bind('tab: complete')
readline.parse_and_bind('Control-v: paste')

def clearCmd(cmd: str):
    return cmd.strip(';').strip()

## Hanle sql.
def handleSql(cmd: str):
    if not cmd.endswith(';'):
        cmd +=';'
    return cmd

## Execute exit.
def exit():
    client.close()
    print('\nBye...\n')
    sys.exit(0)

## Execute clear.
def clear():
    os.system("clear")

## Execute sql statement.
def simpledb(cmd):
    sql = handleSql(cmd)
    resp = client.execute(sql)    
    print(resp)

## Read command.
def readCmd():
    try:
        return input('simpledb > ')
    except KeyboardInterrupt:
        exit()

## Source sql file.
def exec_source(file):
    source = Source(client, file)
    source.exec()

## Output result to file.
def output(cmd: str):
    list = cmd.split(">>")
    if (len(list) != 2):
        print("Error input for >>")
        return
    sql = handleSql(list[0].strip())
    filePath = list[1].rstrip(';').strip()
    ret = client.directExecute(sql)
    with open(filePath, 'w', encoding='utf-8') as file:
        file.write(ret)
    print(f"Result write into {filePath} successfully.")


def handle(cmd: str):
    cmd = clearCmd(cmd)
    list = cmd.split("|")
    if (len(list) != 2):
        print("Error input for |")
        return
    sql = handleSql(list[0].strip())
    hanler = list[1].strip().upper()
    ret = client.execute(sql)
    match hanler:
        case 'JQ':
            handleJsonpp(ret)
        case 'JSON_PP':
            handleJsonpp(ret)
        case _:
            print(f"Not support handler:{hanler}")
    

## JSON_PP
def handleJsonpp(raw: dict):
    jsonpp = json.dumps(
        raw, sort_keys=False, ensure_ascii=False, 
        indent=4, separators=(',', ':')
    )
    print(jsonpp)


## Execute command.
def exec_cmd(cmd):
    cmd = cmd.strip()
    if cmd.upper().startswith("SOURCE "):
        exec_source(cmd.strip("SOURCE ").strip("source").strip())
    elif ">>" in cmd:
        output(cmd)
    elif '|' in cmd:
        handle(cmd)   
    else:
        match cmd.upper():
            case 'EXIT':
                exit()
            case 'CLEAR':
                clear()
            case 'CLS':
                clear()
            case _:
                simpledb(cmd)

## Login 
def login() -> bool:
    account = input("Your account: ")
    password = getpass.getpass("Your password: ")
    clear()
    ret = client.execute(f"{account}/{password}")
    return ret["success"]

def printSlogan():
    print("  ____                            ____    ____          ")
    print(" /\\  _`\\   __                    /\\  _`\\ /\\  _`\\        ")
    print(" \\ \\,\\L\\_\\/\\_\\    ___ ___   _____\\ \\ \\/\\ \\ \\ \\L\\ \\      ")
    print("  \\/_\\__ \\\\/\\ \\ /' __` __`\\/\\ '__`\\ \\ \\ \\ \\ \\  _ <'     ")
    print("    /\\ \\L\\ \\ \\ \\/\\ \\/\\ \\/\\ \\ \\ \\L\\ \\ \\ \\_\\ \\ \\ \\L\\ \\    ")
    print("    \\ `\\____\\ \\_\\ \\_\\ \\_\\ \\_\\ \\ ,__/\\ \\____/\\ \\____/   ")
    print("     \\/_____/\\/_/\\/_/\\/_/\\/_/\\ \\ \\/  \\/___/  \\/___/     ")
    print("                              \\ \\_\\                     ")
    print("                               \\/_/                     ")
    print(" Welcome to simpledb. Your simpledb version is 0.0.1.   ")
    print(" Copyright (c) 2024, Inspur.com     ")

## Fetch Tables.
def fetchTables():
    ret = client.execute("show tables;")
    assert ret["success"] == True
    for item in ret["data"]:
        keywords.append(item["relname"])


## Main.
if __name__ == "__main__":
    try:
        if login():
            printSlogan()
            fetchTables()
            while True:
                cmd = readCmd()
                exec_cmd(cmd)
    except KeyboardInterrupt:
        exit()
    except socket.timeout:
        print("timout.")
        exit()
    except Exception as e:
        print(f"Error:{e}")
        exit()
