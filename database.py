## Import libraries 
## test comment 

import csv, ast, psycopg2
import pandas as pd

## Class definition

class push_data:
    
    def dataType(self,val, current_type):
        try:
            # Evaluates numbers to an appropriate type, and strings an error
            t = ast.literal_eval(val)
        except ValueError:
            return 'varchar'
        except SyntaxError:
            return 'varchar'
        if type(t) in [int, float]:
            if (type(t) in [int]) and current_type not in ['float', 'varchar']:
                if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                    return 'smallint'
                elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                    return 'int'
                else:
                    return 'bigint'
            if type(t) is float and current_type not in ['varchar']:
                return 'decimal'
        else:
            return 'varchar'
    
    def to_sql(self,dbname,user,host,password,filepath,table_name,port,look_up=None,look_up_name=None,time_col=None,time_col_name=None):

        try:
            conn = psycopg2.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            dbname=dbname)

        except:
            print("I am unable to connect to the database")
            
        # create the table 
        cur = conn.cursor()
        cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")
        if (table_name,) not in cur.fetchall():

            ## Preparing the Table creation statement for train data
            ## following this tutorial: https://www.periscopedata.com/blog/python-create-table
            f = open(filepath, 'r')
            reader = csv.reader(f)

            longest, headers, type_list = [], [], []

            for row in reader:
                if len(headers) == 0:
                    headers = row
                    for col in row:
                        longest.append(0)
                        type_list.append('')
                else:
                    for i in range(len(row)):
                        # NA is the csv null value
                        if type_list[i] == 'varchar' or row[i] == 'NA':
                            pass
                        else:
                            var_type = self.dataType(row[i], type_list[i])
                            type_list[i] = var_type
                    if len(row[i]) > longest[i]:
                        longest[i] = len(row[i])
            f.close()     

            statement = 'create table ' + table_name + ' ('


            # I end up coercing the varchars
            for i in range(len(headers)):
                if headers[i] == time_col:
                    statement = (statement + '\n{} timestamp,').format(headers[i].lower())
                elif type_list[i] == 'varchar':
                    statement = (statement + '\n{} varchar(2048),').format(headers[i].lower())
                else:
                    statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])

            statement = statement[:-1] + ');'

            # SQL doesn't like periods in names
            statement = statement.replace(".","_")
        
            cur.execute(statement)
            conn.commit()    
            
            # push the data in
            with open(filepath, 'r') as f:
                next(f)
                cur.copy_from(f, table_name, sep=',')
            conn.commit()
        
        else:
            print('table name already exists')
    
        
        cur.execute(f"""SELECT indexname from pg_indexes where tablename = '{table_name}'""")

        if look_up is None and time_col is not None:
            # creating the index, this is critical for SB time series usage
            if (time_col_name,) not in cur.fetchall():
                cur.execute(f'''create index {time_col_name} on {table_name} ({time_col});''')
                conn.commit()
            else: print('time column is already indexed')
            
        if look_up is not None and time_col is not None:
            # creating the index, this is critical for SB time series usage
            if (time_col_name,) not in cur.fetchall():
                cur.execute(f'''create index {look_up_name}_{time_col_name} on {table_name} ({look_up},{time_col});''')
                conn.commit()
            else: print('look-up and time columns are already indexed')
            
        if look_up is not None and time_col is None:
            # creating the index, this is critical for SB time series usage
            if (time_col_name,) not in cur.fetchall():
                cur.execute(f'''create index {look_up_name} on {table_name} ({look_up});''')
                conn.commit()
            else: print('look-up column is already indexed')
                