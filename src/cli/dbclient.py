#connector.py
import io
import socket
import json
import sys

class DbClient:
    def __init__(self, ip, port):
        # create a socket object.
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket.setdefaulttimeout(3000)
        try:
            self.client.connect((ip, port))
            self.client.settimeout(3000)
            self.hasConnected = True
        except socket.timeout:
            self.hasConnected = False
            print(f"Connect to {ip}:{port} timeout.")
        except socket.error as e:
            self.hasConnected = False
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

    def directExecute(self, sql: str):
        try:
            sql_bytes = sql.encode("utf-8")
            slen = len(sql_bytes)
            num_bytes = slen.to_bytes(4, byteorder=sys.byteorder)
            sdata = b''+ num_bytes + sql_bytes
            self.client.send(sdata)
            # writer = io.StringIO()
            while True:
                len_resp_bytes = self.socket_recv(4)
                if not len_resp_bytes:
                    raise Exception("not recive any data")
                rlen = int.from_bytes(len_resp_bytes, byteorder=sys.byteorder)
                data_resp_bytes = self.socket_recv(rlen)
                if not data_resp_bytes:
                    raise Exception("not recive any data")
                response = data_resp_bytes.decode("utf-8").strip("\x00")
                if response.endswith("\r\n\r\n"):
                    print(response[:-4])
                    break
                else:
                    print(response)
        except ConnectionError:
            exit(1)
        except socket.timeout:
            print("timeout.")
            exit(1)
        except Exception as e:
            print(f"Error: {e}")
        return ''

    def execute(self, sql: str) -> dict:
        resp = ''
        try:
            sql_bytes = sql.encode("utf-8")
            slen = len(sql_bytes)
            num_bytes = slen.to_bytes(4, byteorder=sys.byteorder)
            sdata = b''+ num_bytes + sql_bytes
            self.client.send(sdata)
            writer = io.StringIO()
            while True:
                len_resp_bytes = self.socket_recv(4)
                if not len_resp_bytes:
                    raise Exception("not recive any data")
                rlen = int.from_bytes(len_resp_bytes, byteorder=sys.byteorder)
                data_resp_bytes = self.socket_recv(rlen)
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
