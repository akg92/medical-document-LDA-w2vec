import psycopg2
import json


class Conf():
    conf = None
    file_name = None

    @staticmethod
    def read_properties():
        conf_str = ""
        with open(Conf.file_name) as f:
            Conf.conf = json.load(f)

    @staticmethod
    def init(file_name=None):
        if file_name:
            Conf.file_name = file_name
        Conf.read_properties()

    @staticmethod
    def get_connection_string():
        con_str = "dbname='{}' user='{}' host='{}' password='{}'".format(Conf.conf['dbname'], Conf.conf['user'],
                                                                         Conf.conf['host'], Conf.conf['password'])
        return con_str


# Connection class.
# Should elaborate with additional methods.
# Reusage of connection.
class Connection():

    def __init__(self, schema='mimiciii'):
        self.con = psycopg2.connect(Conf.get_connection_string())
        self.schema = schema
        self.log_file = 'con.log'

    def get_connector(self):
        return self.con

    def write_log(self, text):
        with open(self.log_file, 'w+') as f:
            f.write(text + '\n')

    # get cursor set to schema
    def get_cursor(self, schema_level=True):
        cursor = self.con.cursor()
        if schema_level:
            cursor.execute('SET search_path TO ' + self.schema)
        return cursor

    def execute_return_cursor(self, query):
        cursor = self.get_cursor()
        cursor.execute(query)
        return cursor
        # fetch all from cursor

    def cursor_to_list(self, cur, index=-1):
        result = []
        for ele in cur:
            if (index != -1):
                result.append(ele[index])
            else:
                result.append(ele)
        return result

    # execute_update
    def execute_update(self, query):
        cur = self.get_cursor()
        cur.execute(query)
        # cur.commit()
        cur.close

    # execute query
    def execute_fetch(self, query):
        cursor = self.get_cursor()
        cursor.execute(query)
        result = self.cursor_to_list(cursor)
        return result

    # get all columns
    def get_columns(self, table):
        query = "SELECT * FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}' ".format(
            self.schema, table)
        print(query)
        cursor = self.get_cursor(False)
        cursor.execute(query)
        result = self.cursor_to_list(cursor, 3)
        cursor.close()
        return result

    # get all distinct values of a column
    def get_distinct(self, table, column):
        query = "SELECT DISTINCT {} from {}".format(column, table)
        cursor = self.get_cursor()
        cursor.execute(query)
        return self.cursor_to_list(cursor, 0)

    # get total entries
    def get_total_count(self, table):
        query = "SELECT count(*) from {}".format(table)
        cur = self.get_cursor()
        cur.execute(query)
        return self.cursor_to_list(cur, 0)[0]

    def get_distinct_count(self, table, column):
        query = "SELECT  {},count(*) from {} group by {} order by {} ".format(column, table, column, column)
        cursor = self.get_cursor()
        cursor.execute(query)
        return self.cursor_to_list(cursor)

    # insert
    def insert_into(self, table, value):
        query = "INSERT INTO {} VALUES({})".format(table, value)
        cursor = self.get_cursor()
        # print(query)
        try:
            cursor.execute(query)
        except Exception:
            print("failed:" + query)
            self.write_log("failed:" + query)

    def close(self):
        self.con.close()

    def reset(self):
        self.close()
        self.con = psycopg2.connect(Conf.get_connection_string())

    def commit(self):
        self.con.commit()

    ## note events.

    def get_comments_by_patient(self,id):
        query = "select text from noteevents where subject_id={}".format(id)
        cursor = self.get_cursor()
        cursor.execute(query)
        result = self.cursor_to_list(cursor,index=0)
        return result

    def get_comments_by_time(self,from_admission,hadm_ids=set()):
        query = "select nt.hadm_id, nt.text FROM noteevents as nt inner join admissions as ad on ad.hadm_id=nt.hadm_id where extract(epoch from   nt.charttime-ad.admittime )/3600 < {} and  nt.category!='Discharge summary'".format(from_admission)
        cursor = self.get_cursor()
        cursor.execute(query)
        dictionary = {}

        for entry in cursor:
            ## first is hadm id
            hadm_id = entry[0]
            if hadm_id in hadm_ids:
                if hadm_id not in dictionary:
                    dictionary[hadm_id]= ""
                dictionary[hadm_id]= dictionary[hadm_id]+entry[1]
            ## create a single note.

        return dictionary







Conf.init('db.json')