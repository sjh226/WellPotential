import pandas as pd
import numpy as np
import sys
import pyodbc
import urllib
import sqlalchemy

def fetchdata():
	connection = pyodbc.connect(r'Driver={SQL Server Native Client 11.0};'
                                r'Server=SQLDW-L48.BP.Com;'
                                r'Database=EDW;'
                                r'trusted_connection=yes'
								)

	cursor = connection.cursor()

	SQLCommand = ("""
        SELECT	DW.API,
		        DW.WellName,
		        DW.Route,
		        DW.FirstProductionDate,
		        DE.EVENT_ID AS Event_ID,
		        DD.Daily_ID AS Daily_ID,
		        DE.Event_Objective_1 AS WorkOver_Type,
		        DE.Event_Code AS WorkOver_Code,
		        DE.DATE_OPS_START AS Start_Date,
		        DE.DATE_OPS_END AS End_Date,
		        DD.Date_Report,
		        DD.Comment_Summary,
		        DA.Activity_Memo
        FROM [OperationsDataMart].[Dimensions].[Wells] AS DW
        INNER JOIN [EDW].[OpenWells].[CD_Well] AS CW
			ON LEFT(CW.API_NO, 10) = DW.API
        INNER JOIN [EDW].[OpenWells].[DM_EVENT] AS DE
			ON CW.Well_ID = DE.Well_ID
        INNER JOIN [EDW].[OpenWells].[DM_Daily] AS DD
			ON DE.Well_ID = DD.Well_ID
			AND DE.Event_ID = DD.Event_ID
        INNER JOIN [EDW].[OpenWells].[DM_Activity] AS DA
			ON DD.Well_ID = DA.Well_ID
			AND DD.Event_ID = DA.Event_ID
			AND DD.Daily_ID = DA.Daily_ID
        WHERE	DW.BusinessUnit LIKE 'North'
          AND	(DE.Event_Code IN ('WLI', 'SLK', 'WO', 'WS')
		   		OR DE.Event_Type IN ('Workover', 'Slickline', 'Well Integrity', 'Well Servicing'))
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


def sql_push(df, table):
    params = urllib.parse.quote_plus('Driver={SQL Server Native Client 11.0};\
									 Server=SQLDW-L48.BP.Com;\
									 Database=TeamOperationsAnalytics;\
     								 trusted_connection=yes
                                     )
    engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)

    df.to_sql(table, engine, schema='dbo', if_exists='append', index=False)


if __name__ == '__main__':
    df = fetchdata()
    df.to_csv('data/North_WO_Text.csv')
