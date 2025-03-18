import re

class Source:
    part = ""
    patch = []
    def __init__(self, client, file):
        self.client = client
        self.file = file

    def push(self, sql):
        self.patch.append(sql)
        if len(self.patch) >= 50:
            self.exec_inner()


    ## Execute the file.
    def exec(self):
        try:
            with open(self.file) as file:
                for line in file:
                    item = line.strip()
                    self.push(item)
                self.exec_inner()
        except FileNotFoundError:
            print(f"file {self.file} not found")
            pass
        except KeyboardInterrupt:
            exit()

    def exec_inner(self):
        if len(self.patch) == 0:
            pass
        if self.satisfy_batch_insert():
            self.batch_insert()
        else:
            self.batch_other()

        self.patch.clear()

    def satisfy_batch_insert(self) -> bool:
        for item in self.patch:
            if not item.upper().startswith("INSERT"):
                return False
            splits = re.split("VALUES", item, flags=re.IGNORECASE)
            if self.part == "":
                self.part = splits[0]
            if self.part != splits[0]:
                return False
        return True

    def batch_insert(self):
        sql = "{} values".format(self.part)
        for item in self.patch:
            splits = re.split("VALUES", item, flags=re.IGNORECASE)
            sql += "{}, ".format(splits[1].rstrip(";"))
        sql = sql.rstrip().rstrip(",")
        sql += ";"
        ret = self.client.execute(sql);
        print(ret)
        

    def batch_other(self):
        for item in self.patch:
            ret = self.client.execute(item);
            print(ret)





