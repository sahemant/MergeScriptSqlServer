import pyodbc 
from appJar import gui
def connect():
    server = 'tcp:[azure sql server address ].database.windows.net' 
    database = 'database name' 
    username = 'username' 
    password = 'password'
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    return cnxn
def generateScript(SchemaName,TableName,conn,f):
    f.write('--[{0}].[{1}]\n\r'.format(SchemaName,TableName))
    queryIdentity = 'select name from sys.identity_columns where OBJECT_NAME(object_id) = \'{0}\' and OBJECT_SCHEMA_NAME(object_id) = \'{1}\' '.format(TableName,SchemaName)
    cursor = conn.execute(queryIdentity)
    rows = cursor.fetchall()
    flagIndentity=False
    if len(rows)>0:   
        f.write('SET IDENTITY_INSERT [{0}].[{1}] ON\n\rGO\n\r'.format(SchemaName,TableName))
        flagIdentity = True
    else:
        print 'no identity for {0}.{1}'.format(SchemaName,TableName) 
    query ='select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where TABLE_SCHEMA = \'{0}\' and TABLE_NAME = \'{1}\' and CONSTRAINT_TYPE = \'PRIMARY KEY\' '.format(SchemaName,TableName)
    #print query
    cursor = conn.execute(query)
    
    rows = cursor.fetchall()
    constraint_names=list()
    for row in rows:
        constraint_names.append('\''+row.CONSTRAINT_NAME+'\'')
    constraint_in = '( {0} )'.format((",".join(constraint_names)))
    query2 = 'select * from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE where TABLE_SCHEMA = \'{0}\' and TABLE_NAME = \'{1}\' and CONSTRAINT_NAME IN {2}'.format(SchemaName,TableName,constraint_in)
    #print query2
    try:
        cursor = conn.execute(query2)
    except:
        print query2
        return
    rows=cursor.fetchall()
    pk_names = list()
    for row in rows:
        pk_names.append(row.COLUMN_NAME)
    query3 = 'select * from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA =\'{0}\' and TABLE_NAME=\'{1}\''.format(SchemaName,TableName)
    try:
        cursor = conn.execute(query3)
    except:
        print query3
        return
    rows = cursor.fetchall()
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
    f.write('CREATE TABLE [{0}] ( {1} )\n\r'.format(tempTableName,(",".join(columnNames))))
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
        f.write('INSERT INTO {0} VALUES( {1} )\n\r'.format(tempTableName,l.encode('utf-8')))
    f.write('MERGE [{0}].[{1}] Target\n\r'.format(SchemaName,TableName))
    f.write('USING (SELECT * FROM {0}) AS SOURCE\n\r'.format(tempTableName))
    flag=False
    l=''
    for pk in pk_names:
        if flag:
            l=l+' AND '
        l=l+('Source.{0} = Target.{0}'.format(pk))
        flag=True
    f.write('ON {0}\n\r'.format(l))
    f.write('WHEN MATCHED THEN\n\r')
    flag=False
    l=''
    for col in justCN:
        if col not in pk_names:
            if flag:
                l=l+' , '
            l = l+ ( 'Target.{0} = Source.{0}'.format(col))
            flag=True
    f.write('UPDATE SET {0}\n\r'.format(l))
    f.write('WHEN NOT MATCHED THEN\n\r')
    f.write('INSERT ( {0} )\n\r'.format(columnParamList))
    flag=False
    l=''
    for col in justCN:
        if flag:
            l = l+','
        l = l + ('source.[{0}]'.format(col))
        flag=True
    f.write('VALUES ( {0} );\n\rGO\n\r'.format(l))
    if flagIdentity:
        f.write('SET IDENTITY_INSERT [{0}].[{1}] OFF\n\rGO\n\r'.format(SchemaName,TableName))
            
        

app=app2=None
connection = None
def main(tables):
    global connection
    f=open('output.sql','w')
    for table in tables:
        Schema,TableName = table.split('.',1)
        generateScript(Schema,TableName,connection,f)

server=None
database=None
username=''
password=''
def checkConnection(server,pwd,db,user):
    server = server#'tcp:{0}'.format(server) 
    database = db 
    username = user 
    password = pwd
    if len(username)!=0:
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=tcp:'+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    else:
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+'username=fareast\sahemant;DATABASE='+database+';Trusted_Connection=yes;')
    return cnxn


def checkBoxButtonsHandler(button):
    if button=="Cancel":
        app2.stop()
        exit()
    else:
        checkBoxes = app2.getAllCheckBoxes()
        keys=checkBoxes.keys()
        tableNames=list()
        for k in keys:
            if checkBoxes[k]:
                tableNames.append(k)
        app2.stop()
        print 'Generating Scripts'
        main(tableNames)
        

def selectTables(conn):
    global app2
    app2.startScrollPane("scroller")
    query = 'select TABLE_SCHEMA,TABLE_NAME from INFORMATION_SCHEMA.COLUMNS Group by TABLE_NAME,TABLE_SCHEMA order by TABLE_SCHEMA,TABLE_NAME '
    cursor = conn.execute(query)
    rows=cursor.fetchall()
    for r in rows:
        app2.addCheckBox(r[0]+'.'+r[1])
    app2.stopScrollPane()
    app2.addButtons(["Ok","Cancel"],checkBoxButtonsHandler)

def mainButtonHandler(button):
    global app
    global app2
    global connection
    if button == "Cancel":
        app.stop()
        exit()
    else:
        server = app.getEntry("Server")
        pwd = app.getEntry("Password")
        db = app.getEntry("Database")
        user = app.getEntry("username")
        try:
            connection = checkConnection(server,pwd,db,user)
            app.stop()
            try:
                app2 = gui("Script Generator","400x500")
                selectTables(connection)
                app2.go()
            except Exception as ex:
                print ex
        except:
            try:
                app.infoBox('error','Error Connecting Database')
                #app.addLabel("error","Error Connecting Server")
            except:
                pass
def gui2():
    global app
    app=gui("Script Generator","400x500")
    app.addLabel("Server: ")
    app.addLabelEntry("Server")
    app.addLabelEntry("username")
    app.addLabelSecretEntry("Password")
    app.addLabelEntry("Database")
    app.addButtons(["Submit","Cancel"],mainButtonHandler)
    app.go()
    
#gui2()
connection=connect()
hierarchyListQuery = open('hirarchyList.txt','r').read()
tableListCursor = connection.execute(hierarchyListQuery)
rows = tableListCursor.fetchall()
tableNames = list()
for r in rows:
    tableNames.append(r[0]+'.'+r[1])
tableNames = tableNames[::-1]
connection.close()
connection=connect()

requiredTables = [
'App.ProfileLevel'

    ]

finalTables = list()
processed = 0
print tableNames
for t in tableNames:
    if t in requiredTables:
        print t
        processed+=1
        finalTables.append(t)
main(finalTables)
print processed


