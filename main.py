import pyodbc 
import json
def connect(serverLocation,databaseName,username,password):
    server = 'tcp:{0}'.format(serverLocation) 
    database = databaseName 
    username = username 
    password = password
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    return cnxn
def generateScript(SchemaName,TableName,conn,f):
    f.write('--[{0}].[{1}]\n'.format(SchemaName,TableName))
    queryIdentity = 'select name from sys.identity_columns where OBJECT_NAME(object_id) = \'{0}\' and OBJECT_SCHEMA_NAME(object_id) = \'{1}\' '.format(TableName,SchemaName)
    cursor = conn.execute(queryIdentity)
    rows = cursor.fetchall()
    flagIndentity=False
    if len(rows)>0:   
        f.write('SET IDENTITY_INSERT [{0}].[{1}] ON\nGO\n'.format(SchemaName,TableName))
        flagIdentity = True
    else:
        print 'no identity for {0}.{1}'.format(SchemaName,TableName) 
    query ='select CONSTRAINT_NAME from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where TABLE_SCHEMA = \'{0}\' and TABLE_NAME = \'{1}\' and CONSTRAINT_TYPE = \'PRIMARY KEY\' '.format(SchemaName,TableName)
    #print query
    cursor = conn.execute(query)
    
    rows = cursor.fetchall()
    constraint_names=list()
    for row in rows:
        constraint_names.append('\''+row.CONSTRAINT_NAME+'\'')
    constraint_in = '( {0} )'.format((",".join(constraint_names)))
    query2 = 'select COLUMN_NAME from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE where TABLE_SCHEMA = \'{0}\' and TABLE_NAME = \'{1}\' and CONSTRAINT_NAME IN {2}'.format(SchemaName,TableName,constraint_in)
    #print query2
    try:
        cursor = conn.execute(query2)
    except:
        print query2
        print '{0}.{1} might not have primark key.'.format(SchemaName ,TableName)
        return
    rows=cursor.fetchall()
    pk_names = list()
    for row in rows:
        pk_names.append(row.COLUMN_NAME)
    query3 = '''
                select I_S.COLUMN_NAME,I_S.CHARACTER_MAXIMUM_LENGTH,I_S.NUMERIC_SCALE,I_S.NUMERIC_PRECISION,I_S.IS_NULLABLE,DATA_TYPE from INFORMATION_SCHEMA.COLUMNS as I_S
                inner join sys.tables T
                on I_S.TABLE_NAME = T.name
                inner join sys.schemas sch
                on sch.schema_id = T.schema_id 
                inner join sys.columns C
                on C.object_id = T.object_id and C.name = I_S.COLUMN_NAME
                where TABLE_SCHEMA = '{0}' and TABLE_NAME = '{1}' and sch.name = '{0}' and generated_always_type = 0
            '''
    query3 = query3.format(SchemaName,TableName)
    try:
        cursor = conn.execute(query3)
    except Exception as ex:
        print ex
        print query3
        return
    rows = cursor.fetchall()
    print 'rows we got : '+str(len(rows))
    tempTableName = '#'+SchemaName+TableName
    columnNames =list()
    realColumnNames=list()
    justCN = list()
    dateTypeIndex=list()
    i=0
    for r in rows:
        length = ''
        if r.CHARACTER_MAXIMUM_LENGTH:
            length = '('+str(r.CHARACTER_MAXIMUM_LENGTH)+')'
        elif r.NUMERIC_SCALE and r.NUMERIC_PRECISION:
            if int(r.NUMERIC_SCALE)>0:
                length = '({0},{1})'.format(str(r.NUMERIC_PRECISION),str(r.NUMERIC_SCALE))

        nullable=''
        if r.IS_NULLABLE == 'NO':
            nullable = 'not null'
        realColumnNames.append('['+r.COLUMN_NAME+']')
        justCN.append(r.COLUMN_NAME)
        #if r.contains('date'):
            #dateTypeIndex.append(i)
        x = '['+r.COLUMN_NAME+'] '+r.DATA_TYPE+''+length+' '+nullable
        columnNames.append(x)
        i+=1
    f.write('CREATE TABLE [{0}] ( {1} )\n'.format(tempTableName,(",".join(columnNames))))
    columnParamList = ",".join(realColumnNames)
    query4 = 'select {0} from [{1}].[{2}]'.format(columnParamList,SchemaName,TableName)
    #print query4
    try:
        cursor = conn.execute(query4)
    except:
        print query4
        return
    rows = cursor.fetchall()
    for r in rows:
        l=''
        flag=False
        i=0
        for k in r:
            #print type(k)
            if flag:
                l=l+','
            if k == None:
                l = l + 'NULL'
            else:
                try:
                    if 'date' in str(type(k)) and len(str(k))==26:
                        b=str(k)
                        b=b[0:26-3]
                        k=b
                    if 'unicode' in str(type(k)):
                        try:
                            k=str(k).encode('utf-8').replace("'","''")
                            l = l + '\''+str(k)+'\''
                        except:
                            l = l + '\''+k+'\''
                    else:
                        try:
                            l = l + '\''+k+'\''
                        except:
                            l = l + '\''+str(k).encode('utf-8').replace("'","''")+'\''
                except Exception as ex:
                    print ex
                    print TableName
                    print 'EXCEPTION RAISED'
                    print k
                    return
            flag=True
            i+=1
        f.write('INSERT INTO {0} VALUES( {1} )\n'.format(tempTableName,l.encode('utf-8')))
    f.write('MERGE [{0}].[{1}] Target\n'.format(SchemaName,TableName))
    f.write('USING (\n\tSELECT *\n\t FROM {0}\n\t) AS SOURCE\n'.format(tempTableName))
    flag=False
    l=''
    for pk in pk_names:
        if flag:
            l=l+' AND '
        l=l+('Source.{0} = Target.{0}'.format(pk))
        flag=True
    f.write('\tON {0}\n'.format(l))
    f.write('WHEN MATCHED\n\tTHEN\n')
    flag=False
    l=''
    for col in justCN:
        if col not in pk_names:
            if flag:
                l=l+' \n\t\t\t,'
            l = l+ ( 'Target.{0} = Source.{0}'.format(col))
            flag=True
    f.write('\t\tUPDATE\n\t\tSET {0}\n'.format(l))
    f.write('WHEN NOT MATCHED\n\tTHEN\n')
    f.write('\t\tINSERT (\n\t\t\t {0}\n\t\t\t)\n'.format(columnParamList.replace(',','\n\t\t\t,')))
    flag=False
    l=''
    for col in justCN:
        if flag:
            l = l+'\n\t\t\t,'
        l = l + ('source.[{0}]'.format(col))
        flag=True
    f.write('\t\tVALUES (\n\t\t\t {0}\n\t\t\t);\nGO\n'.format(l))
    f.write('DROP TABLE {0}\n'.format(tempTableName))
    if flagIdentity:
        f.write('SET IDENTITY_INSERT [{0}].[{1}] OFF\nGO\n'.format(SchemaName,TableName))
            
        

app=app2=None
connection = None
def start(tables):
    global connection
    f=open('output.sql','w')
    for table in tables:
        Schema,TableName = table.split('.',1)
        generateScript(Schema,TableName,connection,f)


def readConfig(filename):
    serverLocation = databaseName = username = password = None
    configText = None
    with open(filename,'r') as f:
        configText=f.read()
    configDict = json.loads(configText)
    serverLocation = configDict['AzureSqlServerProperties']['ServerLocation']
    databaseName = configDict['AzureSqlServerProperties']['Database']
    username = configDict['AzureSqlServerProperties']['Username']
    password = configDict['AzureSqlServerProperties']['Password']
    requiredTables = list(configDict['TableNames'])
    return serverLocation,databaseName,username,password,requiredTables


def main():
    global connection
    try:
        #print readConfig('config.json')
        serverLocation , databaseName,username,password,requiredTables = readConfig('config.json')
    except:
        print 'Some Error Reading Config File'
        return
    connection=connect(serverLocation,databaseName,username,password)
    hierarchyListQuery = open('hierarchyList.sql','r').read()
    tableListCursor = connection.execute(hierarchyListQuery)
    rows = tableListCursor.fetchall()
    tableNames = list()
    for r in rows:
        tableNames.append(r[0]+'.'+r[1])
    tableNames = tableNames[::-1]
    connection.close()
    connection=connect(serverLocation,databaseName,username,password)

    #requiredTables = [
    #'App.ProfileLevel'
    #
     #   ]

    finalTables = list()
    processed = 0
    #print tableNames
    for t in tableNames:
        if t in requiredTables:
            print t
            processed+=1
            finalTables.append(t)
    start(finalTables)
    print processed
    print 'Check Output.sql file for Merge Script.'
main()


