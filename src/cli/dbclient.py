#connector.py
import io
import socket
import json
import sys

class DbClient:
    def __init__(self, ip, port):
        # create a socket object.
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket.setdefaulttimeout(300)
        try:
            self.client.connect((ip, port))
            self.client.settimeout(300)
        except socket.timeout:
            print(f"Connect to {ip}:{port} timeout.")
        except socket.error as e:
            print(f"Socket error: {e}")

    def show_bytes(self, byte_data):
        hex_values = ' '.join(hex(b)[2:].zfill(2) for b in byte_data)
        print(hex_values)

    def show_bytes2(self, string_data):
        hex_values = ' '.join(hex(ord(c))[2:].zfill(2) for c in string_data)
        print(hex_values)

    def socket_recv(self, n):
        data = b''
        while len(data) < n:
            packet = self.client.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data

    def directExecute(self, sql) -> str:
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
            return resp
        except ConnectionError:
            exit(1)
        except socket.timeout:
            print("timeout.")
            exit(1)
        except Exception as e:
            print(f"Error: {e}, and Raw is {resp}")
        return ''

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
