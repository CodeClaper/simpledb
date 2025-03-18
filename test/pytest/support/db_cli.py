#db_cli.py
import io
import socket
import json

class DbClient:
    
    def __init__(self, ip, port):
        # create a socket object.
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect((ip, port))
        self.client.settimeout(3000)

    def login(self, account, password) -> bool:
        self.client.send(f"{account}/{password}".encode("utf-8"))
        response = self.client.recv(65535)
        response = response.decode("utf-8").rstrip("\x00")
        return response != 'No access.'

    def execute(self, sql) -> dict:
        self.client.send(sql.encode("utf-8")[:65535])
        writer = io.StringIO()
        while True:
            resp_bytes = self.client.recv(65535)
            if not resp_bytes:
                raise Exception("not recive any data")
            response = resp_bytes.decode("utf-8").strip("\x00")
            if response.upper() == "OVER":
                break
            writer.write(response)
        ret = writer.getvalue()
        writer.close()
        return json.loads(ret)
               

    def close(self):
        self.client.close()
