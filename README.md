## Simple class to push data into Postgresql (provides flexibility to extend to other databases)
Example:

import push_data from database

push_data().to_sql(filepath = "/Users/yapzhengwei/SparkBeyond/Kaggle/Data/market_train_volume_droppednaoutliers_reset.csv"

                    table_name = 'market_train_volume_droppednaoutliers_reset'
                    
                    time_col='time'
                    
                    time_col_name = 'train_time')
