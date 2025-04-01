#db_cli.py
import io
import socket
import json
import sys

class DbClient:
    
    def __init__(self, ip, port):
        # create a socket object.
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect((ip, port))
        self.client.settimeout(3000)

    def login(self, account, password) -> bool: 
        ret = self.execute(f"{account}/{password}")
        print(ret)
        return ret['success']

    def socket_recv(self, n):
        data = b''
        while len(data) < n:
            packet = self.client.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data

    def execute(self, sql) -> dict:
        resp = ''
        try:
            self.client.send(sql.encode("utf-8")[:65535])
            writer = io.StringIO()
            while True:
                len_resp_bytes = self.socket_recv(4)
                if not len_resp_bytes:
                    raise Exception("not recive any data")
                len = int.from_bytes(len_resp_bytes, byteorder=sys.byteorder)
                data_resp_bytes = self.socket_recv(len)
                if not data_resp_bytes:
                    raise Exception("not recive any data")
                response = data_resp_bytes.decode("utf-8").strip("\x00")
                if response.endswith("\r\n\r\n"):
                    writer.write(response[:-4])
                    break
                writer.write(response)
            resp = writer.getvalue()
            writer.close()
            return json.loads(resp)
        except ConnectionError:
            exit(1)
        except socket.timeout:
            print("timeout.")
            exit(1)
        except Exception as e:
            print(f"Error: {e}, and Raw is {resp}")
        return {}
               

    def close(self):
        self.client.close()
