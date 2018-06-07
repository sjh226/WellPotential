import pandas as pd
import numpy as np
import sys
import pyodbc

def fetchdata():
	connection = pyodbc.connect(r'Driver={SQL Server Native Client 11.0};'
                                r'Server=SQLDW-L48.BP.Com;'
                                r'Database=EDW;'
                                r'trusted_connection=yes'
								)

	cursor = connection.cursor()

	SQLCommand = ("""
        SELECT
        DW.[API],
        DW.[WellName],
        DW.[Route],
        DW.[FirstProductionDate],
        DE.EVENT_ID as Event_ID,
        DD.[Daily_ID] as Daily_ID,
        DE.[Event_Objective_1] as [WorkOver_Type],
        DE.[Event_Code] as [WorkOver_Code],
        DE.[DATE_OPS_START] as [Start_Date],
        DE.[DATE_OPS_END] as [End_Date],
        DD.[Date_Report],
        DD.[Comment_Summary],
        DA.[Activity_Memo]
        From OperationsDataMart.Dimensions.Wells as DW
        inner join EDW.OpenWells.CD_Well as CW on Left(CW.API_NO,10)=DW.[API]
        inner join EDW.OpenWells.DM_EVENT as DE on CW.[Well_ID]=DE.[Well_ID]
        Inner join EDW.OpenWells.DM_Daily as DD on DE.[Well_ID]=DD.[Well_ID] and DE.[Event_ID]=DD.[Event_ID]
        Inner join EDW.OpenWells.DM_Activity as DA on DD.[Well_ID]=DA.[Well_ID] and DD.[Event_ID]=DA.[Event_ID] and DD.[Daily_ID]=DA.[Daily_ID]


        where

        DW.[BusinessUnit] like 'North' and
        (DE.[Event_Code] in ('WLI', 'SLK', 'WO', 'WS') or DE.[Event_Type] in ('Workover', 'Slickline', 'Well Integrity', 'Well Servicing'))
	""")

	cursor.execute(SQLCommand)
	results = cursor.fetchall()
	df = pd.DataFrame.from_records(results)

	try:
		df.columns = pd.DataFrame(np.matrix(cursor.description))[0]
	except:
		pass

	# Close the connection after pulling the data
	connection.close()

	return df

if __name__ == '__main__':
    df = fetchdata()
    df.to_csv('data/North_WO_Text.csv')
