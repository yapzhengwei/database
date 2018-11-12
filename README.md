## Simple class to push data into Postgresql (provides flexibility to extend to other databases)
Example:

    import push_data from database

    push_data().to_sql(dbname = 'apacds',
                        user = 'apacds', 
                        host = 'apacds.ckcivkxzsrfb.ap-southeast-1.rds.amazonaws.com', 
                        password ='Starwars_123',
                        port = 5432,
                        filepath ='/path/to/file.csv',
                        table_name = 'my_table_name', 
                        look_up = 'my_look_up_column', ## default None
                        look_up_name = 'my_look_up_column_name', ## default None
                        time_col = 'my_time_column', ## default None
                        time_col_name = 'my_time_column_name') ## default None
                        
                        
